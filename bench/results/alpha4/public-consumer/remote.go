package main
import("context";"fmt";"math";"os";db "github.com/apteva/db";"github.com/apteva/db/client")
func remoteSmoke(){ctx:=context.Background();c,e:=client.New(os.Getenv("SMOKE_URL"),client.Options{Token:os.Getenv("APTEVADB_TOKEN")});must(e);defer c.Close();must(c.CreateCollection(ctx,"numbers"));numbers:=c.Collection("numbers")
for i,v:=range []string{`{"v":9223372036854775807}`,`{"v":9223372036854775807}`,`{"v":null}`,`{}`}{must(numbers.InsertJSON(ctx,[]byte{byte('a'+i)},[]byte(v)))}
r,e:=numbers.Aggregate(ctx,[]byte(`[{"$group":{"_id":null,"sum":{"$sum":"$v"},"avg":{"$avg":"$v"},"rows":{"$count":{}},"present":{"$countNonNull":"$v"}}}]`));must(e);if len(r)!=1||r[0]["sum"]!=float64(math.MaxInt64)*2||r[0]["avg"]!=float64(math.MaxInt64)||r[0]["rows"]!=int64(4)||r[0]["present"]!=int64(2){panic(r)};fmt.Println("downloaded daemon / public client",r)
must(c.CreateComputedCollection(ctx,"present","numbers",[]byte(`[{"$group":{"_id":null,"n":{"$countNonNull":"$v"}}}]`)));must(numbers.UpsertJSON(ctx,[]byte("d"),[]byte(`{"v":false}`)));key,e:=db.ComputedGroupKey(nil);must(e);var row map[string]any;ok,e:=c.Collection("present").FindDocument(ctx,key,&row);must(e);if !ok||row["n"]!=int64(3){panic(row)};fmt.Println("remote maintained counter",row)
}
