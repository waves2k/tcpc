package main

import (
	"fmt"
	"log"
	"time"

	"github.com/waves2k/tcpc/tcpcc"
)

func main() {
	channel, err := tcpcc.NewT[string](tcpcc.DefaultTCPCConfig(":3000", ":4000"))

	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < 100; i++ {
		msg := fmt.Sprintf("TEXT_%d", i)

		channel.Send() <- msg

		log.Printf("message [%s] is sent", msg)

		time.Sleep(time.Millisecond * 600)
	}
}
