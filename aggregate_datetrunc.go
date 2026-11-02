package db

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
	_ "time/tzdata" // CGO-off and Alpine builds cannot rely on system zone files
)

type truncUnit uint8

const (
	truncHour truncUnit = iota
	truncDay
	truncWeek
	truncMonth
	truncYear
)

// dateTrunc is a compiled {$dateTrunc: {date, unit, timezone, startOfWeek,
// epochUnit}}. Its zone is loaded once, when the pipeline is parsed.
type dateTrunc struct {
	field     string
	unit      truncUnit
	loc       *time.Location
	weekStart int   // 0 = Sunday
	epochDiv  int64 // numbers are Unix milliseconds (1000) or seconds (1)
}

// projectExpr is a computed $project field.
type projectExpr struct {
	trunc *dateTrunc
}

var weekdays = map[string]int{"sunday": 0, "sun": 0, "monday": 1, "mon": 1, "tuesday": 2, "tue": 2,
	"wednesday": 3, "wed": 3, "thursday": 4, "thu": 4, "friday": 5, "fri": 5, "saturday": 6, "sat": 6}

// parseExpr compiles an expression object: today only $dateTrunc.
func parseExpr(m map[string]any) (*dateTrunc, error) {
	if len(m) != 1 {
		return nil, errors.New("an expression object needs exactly one operator")
	}
	body, ok := m["$dateTrunc"].(map[string]any)
	if !ok {
		for op := range m {
			return nil, fmt.Errorf("unsupported expression %s", op)
		}
	}
	t := &dateTrunc{loc: time.UTC, epochDiv: 1000}
	for k, v := range body {
		s, isString := v.(string)
		switch k {
		case "date":
			f, ok := fieldRef(v)
			if !ok {
				return nil, errors.New("$dateTrunc date needs a \"$field\" reference")
			}
			t.field = f
		case "unit":
			units := map[string]truncUnit{"hour": truncHour, "day": truncDay, "week": truncWeek, "month": truncMonth, "year": truncYear}
			u, ok := units[s]
			if !isString || !ok {
				return nil, errors.New("$dateTrunc unit must be hour, day, week, month or year")
			}
			t.unit = u
		case "timezone":
			// "Local" depends on the machine; names are IANA zones or UTC
			if !isString || s == "" || s == "Local" {
				return nil, errors.New("$dateTrunc timezone must be an IANA zone name")
			}
			loc, err := time.LoadLocation(s)
			if err != nil {
				return nil, fmt.Errorf("$dateTrunc timezone %q: %v", s, err)
			}
			t.loc = loc
		case "startOfWeek":
			d, ok := weekdays[strings.ToLower(s)]
			if !isString || !ok {
				return nil, errors.New("$dateTrunc startOfWeek must name a weekday")
			}
			t.weekStart = d
		case "epochUnit":
			switch s {
			case "ms":
				t.epochDiv = 1000
			case "s":
				t.epochDiv = 1
			default:
				return nil, errors.New("$dateTrunc epochUnit must be \"ms\" or \"s\"")
			}
		default:
			return nil, fmt.Errorf("$dateTrunc does not take %q", k)
		}
	}
	if t.field == "" {
		return nil, errors.New("$dateTrunc needs date")
	}
	if _, ok := body["unit"]; !ok {
		return nil, errors.New("$dateTrunc needs unit")
	}
	return t, nil
}

// truncEval evaluates one dateTrunc for one stream. It remembers the zone
// period and the bucket of the last instant: rows of the same bucket reuse
// the formatted output without consulting the zone or formatting again.
type truncEval struct {
	t        *dateTrunc
	zlo, zhi int64 // UTC seconds [zlo, zhi) share offset off
	off      int64
	blo, bhi int64 // local seconds [blo, bhi) of the cached bucket
	valid    bool
	out      [24]byte
	n        int
}

const (
	minTruncDay = -719528 // 0000-01-01
	maxTruncDay = 2932896 // 9999-12-31
	secsPerDay  = int64(86400)
)

// eval returns the bucket of v, or null when v is not a usable date. The
// returned string aliases the evaluator until its next call.
func (e *truncEval) eval(v value) value {
	var u int64
	switch v.k {
	case kString:
		var ok, local bool
		if u, local, ok = parseInstant(v.s); !ok {
			return value{k: kNull}
		}
		if local {
			// a bare calendar date is midnight in the pipeline's zone
			y, m, d := civil(floorDiv(u, secsPerDay))
			u = time.Date(int(y), time.Month(m), d, 0, 0, 0, 0, e.t.loc).Unix()
		}
	case kInt:
		u = floorDiv(v.i, e.t.epochDiv)
	case kFloat:
		f := math.Floor(v.f / float64(e.t.epochDiv))
		if !(f > -1e15 && f < 1e15) {
			return value{k: kNull}
		}
		u = int64(f)
	default:
		return value{k: kNull}
	}
	if u < int64(minTruncDay)*secsPerDay || u > int64(maxTruncDay+1)*secsPerDay {
		return value{k: kNull}
	}
	if !e.valid || u < e.zlo || u >= e.zhi {
		e.zone(u)
	}
	local := u + e.off
	if !e.valid || local < e.blo || local >= e.bhi {
		if !e.bucket(local) {
			return value{k: kNull}
		}
	}
	return value{k: kString, s: e.out[:e.n]}
}

func (e *truncEval) zone(u int64) {
	t := time.Unix(u, 0).In(e.t.loc)
	_, off := t.Zone()
	start, end := t.ZoneBounds()
	e.off, e.zlo, e.zhi = int64(off), math.MinInt64, math.MaxInt64
	if !start.IsZero() {
		e.zlo = start.Unix()
	}
	if !end.IsZero() {
		e.zhi = end.Unix()
	}
	e.valid = false // the bucket's UTC rendering depends on the offset
}

func (e *truncEval) bucket(local int64) bool {
	day := floorDiv(local, secsPerDay)
	if day < minTruncDay || day > maxTruncDay {
		return false
	}
	y, m, d := civil(day)
	b := e.out[:0]
	switch e.t.unit {
	case truncHour:
		e.blo = floorDiv(local, 3600) * 3600
		e.bhi = e.blo + 3600
		// the bucket as the UTC instant it starts at: unique and ordered
		// across a repeated fall-back hour
		start := e.blo - e.off
		uy, um, ud := civil(floorDiv(start, secsPerDay))
		sec := start - floorDiv(start, secsPerDay)*secsPerDay
		h, mi := sec/3600, sec/60%60 // offsets of :30 and :45 shift the minute
		b = appendDate(b, uy, um, ud)
		b = append(b, 'T', byte('0'+h/10), byte('0'+h%10), ':', byte('0'+mi/10), byte('0'+mi%10))
		b = append(b, ":00Z"...)
	case truncDay:
		e.blo, e.bhi = day*secsPerDay, (day+1)*secsPerDay
		b = appendDate(b, y, m, d)
	case truncWeek:
		wd := int((day%7 + 7 + 4) % 7) // 1970-01-01 was a Thursday
		first := day - int64((wd-e.t.weekStart+7)%7)
		e.blo, e.bhi = first*secsPerDay, (first+7)*secsPerDay
		fy, fm, fd := civil(first)
		b = appendDate(b, fy, fm, fd)
	case truncMonth:
		first := daysFromCivil(y, m, 1)
		ny, nm := y, m+1
		if nm > 12 {
			ny, nm = y+1, 1
		}
		e.blo, e.bhi = first*secsPerDay, daysFromCivil(ny, nm, 1)*secsPerDay
		b = appendDate(b, y, m, 1)
	case truncYear:
		e.blo, e.bhi = daysFromCivil(y, 1, 1)*secsPerDay, daysFromCivil(y+1, 1, 1)*secsPerDay
		b = appendDate(b, y, 1, 1)
	}
	e.n = len(b)
	e.valid = true
	return true
}

func appendDate(b []byte, y int64, m, d int) []byte {
	return append(b, byte('0'+y/1000%10), byte('0'+y/100%10), byte('0'+y/10%10), byte('0'+y%10), '-',
		byte('0'+m/10), byte('0'+m%10), '-', byte('0'+d/10), byte('0'+d%10))
}

func floorDiv(a, b int64) int64 {
	q := a / b
	if (a%b != 0) && ((a < 0) != (b < 0)) {
		q--
	}
	return q
}

// civil converts days since 1970-01-01 to a proleptic Gregorian date.
func civil(z int64) (y int64, m, d int) {
	z += 719468
	era := floorDiv(z, 146097)
	doe := z - era*146097
	yoe := (doe - doe/1460 + doe/36524 - doe/146096) / 365
	y = yoe + era*400
	doy := doe - (365*yoe + yoe/4 - yoe/100)
	mp := (5*doy + 2) / 153
	d = int(doy - (153*mp+2)/5 + 1)
	if mp < 10 {
		m = int(mp + 3)
	} else {
		m = int(mp - 9)
	}
	if m <= 2 {
		y++
	}
	return y, m, d
}

func daysFromCivil(y int64, m, d int) int64 {
	if m <= 2 {
		y--
	}
	era := floorDiv(y, 400)
	yoe := y - era*400
	mm := int64(m) + 9
	if m > 2 {
		mm = int64(m) - 3
	}
	doy := (153*mm+2)/5 + int64(d) - 1
	doe := yoe*365 + yoe/4 - yoe/100 + doy
	return era*146097 + doe - 719468
}

// parseInstant reads an RFC 3339 timestamp into Unix seconds, or a bare
// YYYY-MM-DD date (local reports that: the seconds are then the date's
// midnight as if in UTC, for the caller to place in its zone).
func parseInstant(s []byte) (u int64, local, ok bool) {
	num := func(b []byte) (int, bool) {
		n := 0
		for _, c := range b {
			if c < '0' || c > '9' {
				return 0, false
			}
			n = n*10 + int(c-'0')
		}
		return n, true
	}
	if len(s) < 10 || s[4] != '-' || s[7] != '-' {
		return 0, false, false
	}
	y, ok1 := num(s[0:4])
	mo, ok2 := num(s[5:7])
	d, ok3 := num(s[8:10])
	if !ok1 || !ok2 || !ok3 || mo < 1 || mo > 12 || d < 1 || d > daysIn(y, mo) {
		return 0, false, false
	}
	days := daysFromCivil(int64(y), mo, d)
	if len(s) == 10 {
		return days * secsPerDay, true, true
	}
	if len(s) < 20 || (s[10] != 'T' && s[10] != 't') || s[13] != ':' || s[16] != ':' {
		return 0, false, false
	}
	h, ok1 := num(s[11:13])
	mi, ok2 := num(s[14:16])
	sec, ok3 := num(s[17:19])
	if !ok1 || !ok2 || !ok3 || h > 23 || mi > 59 || sec > 59 {
		return 0, false, false
	}
	i := 19
	if s[i] == '.' {
		j := i + 1
		for j < len(s) && s[j] >= '0' && s[j] <= '9' {
			j++
		}
		if j == i+1 {
			return 0, false, false
		}
		i = j
	}
	u = days*secsPerDay + int64(h*3600+mi*60+sec)
	switch {
	case i == len(s)-1 && (s[i] == 'Z' || s[i] == 'z'):
		return u, false, true
	case i == len(s)-6 && (s[i] == '+' || s[i] == '-') && s[i+3] == ':':
		oh, ok1 := num(s[i+1 : i+3])
		om, ok2 := num(s[i+4 : i+6])
		if !ok1 || !ok2 || oh > 23 || om > 59 {
			return 0, false, false
		}
		off := int64(oh*3600 + om*60)
		if s[i] == '+' {
			return u - off, false, true
		}
		return u + off, false, true
	}
	return 0, false, false
}

func daysIn(y, m int) int {
	switch m {
	case 2:
		if y%4 == 0 && (y%100 != 0 || y%400 == 0) {
			return 29
		}
		return 28
	case 4, 6, 9, 11:
		return 30
	}
	return 31
}
