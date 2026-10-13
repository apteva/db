package main
import("context";"fmt";"math";"os"; db "github.com/apteva/db")
func must(e error){if e!=nil{panic(e)}}
func main(){if os.Args[1]=="remote"{remoteSmoke();return};ctx:=context.Background();d,e:=db.Open(os.Args[1],db.Options{Sync:true,SyncMode:db.SyncOS});must(e)
c,e:=d.Collection("numbers");must(e)
for i,v:=range []string{`{"v":9223372036854775807}`,`{"v":9223372036854775807}`,`{"v":null}`,`{}`}{must(c.InsertJSON([]byte{byte('a'+i)},[]byte(v)))}
p:=[]byte(`[{"$group":{"_id":null,"sum":{"$sum":"$v"},"avg":{"$avg":"$v"},"rows":{"$count":{}},"present":{"$countNonNull":"$v"}}}]`)
for _,shards:=range []int{1,10}{r,e:=c.AggregateParallel(ctx,p,shards);must(e);if len(r)!=1||r[0]["sum"]!=float64(math.MaxInt64)*2||r[0]["avg"]!=float64(math.MaxInt64)||r[0]["rows"]!=int64(4)||r[0]["present"]!=int64(2){panic(r)};fmt.Println("shards",shards,r)}
_,e=d.CreateComputedCollection(ctx,"present","numbers",[]byte(`[{"$group":{"_id":null,"n":{"$countNonNull":"$v"}}}]`));must(e)
must(c.UpsertJSON([]byte("d"),[]byte(`{"v":0}`)));must(d.Checkpoint());must(d.Close())
d,e=db.Open(os.Args[1],db.Options{});must(e);defer d.Close();view,e:=d.OpenCollection("present");must(e);key,e:=db.ComputedGroupKey(nil);must(e);var r map[string]any;ok,e:=view.FindDocument(key,&r);must(e);if !ok||r["n"]!=int64(3){panic(r)};must(d.Verify(ctx));fmt.Println("version",db.Version,"format",db.FormatVersion,"persisted non-null count",r["n"])
}
