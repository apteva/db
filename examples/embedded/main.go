package main

import (
	"context"
	"fmt"
	db "github.com/apteva/db"
	"os"
)

type User struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	dir, err := os.MkdirTemp("", "aptevadb-example-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	database, err := db.Open(dir, db.DurableOptions())
	if err != nil {
		return err
	}
	defer database.Close()
	users, err := database.Collection("users")
	if err != nil {
		return err
	}
	if err = users.InsertDocument([]byte("u1"), User{"Ada", 30}); err != nil {
		return err
	}
	if err = users.CreateUniqueIndex(context.Background(), "name", "name"); err != nil {
		return err
	}
	var user User
	found, err := users.FindOne(context.Background(), []byte(`{"age":{"$gte":18}}`), &user)
	if err != nil {
		return err
	}
	if !found {
		return db.ErrNotFound
	}
	fmt.Printf("%s is %d\n", user.Name, user.Age)
	return nil
}
