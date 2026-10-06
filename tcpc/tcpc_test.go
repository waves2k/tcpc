package tcpc

import (
	"fmt"
	"log"
	"testing"
	"time"
)

func TestNewReciever(t *testing.T) {
	reciever, err := NewReciever[int](":3000")
	if err != nil {
		t.Errorf("error to create a new reciever: %s", err.Error())
	}

	go func() {
		for msg := range reciever.Chan {
			log.Println("recieved:", msg)
		}

		log.Println("end of readng reciever's egdress")
	}()

	sender, err := NewSender[int](&SenderConfig{
		Retries:      3,
		DialInterval: time.Second * 3,
		RemoteAddr:   ":3000",
	})

	if err != nil {
		fmt.Println(err)
	}

	sender.Chan <- 100
	log.Println("end of the test")
}
