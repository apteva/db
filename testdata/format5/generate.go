package main
import("context";"os";db "github.com/apteva/aptevadb")
func check(err error){if err!=nil{panic(err)}}
func main(){d,err:=db.Open(os.Args[1],db.Options{Sync:true,SyncMode:db.SyncOS});check(err);c,err:=d.Collection("orders");check(err);check(c.DefineComputedFields(context.Background(),[]byte(`{"total":{"$multiply":["$qty","$price"]}}`)));check(c.InsertJSON([]byte("a"),[]byte(`{"customer":1,"qty":2,"price":5}`)));check(c.CreateIndex(context.Background(),"customer","customer"));_,err=d.CreateComputedCollection(context.Background(),"totals","orders",[]byte(`[{"$group":{"_id":"$customer","total":{"$sum":"$total"}}}]`));check(err);check(d.Close())}
