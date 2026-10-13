package main
import("context";"os";"strings";db "github.com/apteva/db")
func check(err error){if err!=nil{panic(err)}}
func main(){d,err:=db.Open(os.Args[1],db.Options{SyncMode:db.SyncOS});check(err);c,err:=d.Collection("rows");check(err);check(c.InsertDocument([]byte("saved"),map[string]any{"tenant":"a","n":int64(9007199254740993),"blob":strings.Repeat("x",80000)}));check(c.CreateCompoundIndex(context.Background(),"tenant_n",[]db.IndexField{{Field:"tenant"},{Field:"n",Desc:true}}));check(c.CreateCompoundIndex(context.Background(),"nullable",[]db.IndexField{{Field:"email"}},db.IndexOptions{Unique:true,NullsDistinct:true}));check(d.Close())}
