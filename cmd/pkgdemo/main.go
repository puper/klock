package main

import (
	"context"
	"log"
	"time"

	"github.com/puper/klock/pkg/hierlock"
)

func main() {
	hl, err := hierlock.New(1024)
	if err != nil {
		panic(err)
	}
	hl.LockL1(context.Background(), "p1")
	log.Println("p1 locked")
	lCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	unlocker, err := hl.LockL1(lCtx, "p1")
	if err != nil {
		log.Println("p1 locked again", err)
		return
	}
	defer unlocker()
	log.Println("p1 locked again")
}
