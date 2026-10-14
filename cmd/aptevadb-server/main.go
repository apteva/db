package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	db "github.com/apteva/db"
	"github.com/apteva/db/server"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "aptevadb-server:", err)
		os.Exit(1)
	}
}
func run() error {
	dir := flag.String("dir", "./data", "database directory")
	listen := flag.String("listen", "127.0.0.1:5282", "listen address")
	cert := flag.String("tls-cert", "", "TLS certificate file")
	key := flag.String("tls-key", "", "TLS private-key file")
	feed := flag.Bool("change-feed", false, "enable persistent native change capture")
	feedBytes := flag.Int64("change-history-bytes", 256<<20, "logical change history byte window")
	feedAge := flag.Duration("change-history-age", time.Hour, "change history age window")
	version := flag.Bool("version", false, "print version")
	flag.Parse()
	if *version {
		fmt.Printf("AptevaDB %s (format %d, HTTP v1)\n", db.Version, db.FormatVersion)
		return nil
	}
	host, _, err := net.SplitHostPort(*listen)
	if err != nil {
		return err
	}
	loopback := host == "localhost" || net.ParseIP(host).IsLoopback()
	if (*cert == "") != (*key == "") {
		return errors.New("tls-cert and tls-key must be supplied together")
	}
	if !loopback && *cert == "" {
		return errors.New("non-loopback listeners require TLS")
	}
	token := os.Getenv("APTEVADB_TOKEN")
	if len(token) < 16 {
		return errors.New("set APTEVADB_TOKEN to a random token of at least 16 bytes")
	}
	opts := db.DurableOptions()
	if *feed {
		opts.ChangeFeed = &db.ChangeFeedOptions{RetentionBytes: *feedBytes, RetentionAge: *feedAge}
	}
	database, err := db.Open(*dir, opts)
	if err != nil {
		return err
	}
	defer database.Close()
	handler, err := server.New(database, server.Options{Token: token})
	if err != nil {
		return err
	}
	defer handler.Close()
	transport := handler.NewHTTPServer(*listen)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	done := make(chan error, 1)
	go func() {
		if *cert != "" {
			done <- transport.ListenAndServeTLS(*cert, *key)
		} else {
			done <- transport.ListenAndServe()
		}
	}()
	select {
	case err = <-done:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 35*time.Second)
		defer cancel()
		err = transport.Shutdown(shutdown)
		if err != nil {
			_ = transport.Close()
		}
		return err
	}
}
