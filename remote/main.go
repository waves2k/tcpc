package main

import (
	"fmt"
	"log"

	"github.com/waves2k/tcpc/tcpcc"
)

func main() {
	channerl, err := tcpcc.NewT[string](tcpcc.DefaultTCPCConfig(":4000", ":3000"))

	if err != nil {
		log.Fatal(err)
	}

	for msg := range channerl.Recieve() {
		fmt.Println("got message over TCP:", msg)

	}
}
