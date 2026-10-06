package tcpc

import (
	"encoding/gob"
	"log"
	"net"
	"time"
)

var (
	defaultMaxRetries   = 5
	defaultDialInterval = time.Second * 3
)

type TCPC[T any] struct {
	listenAddr string
	remoteAddr string

	sendCh chan T
	recvCh chan T
}

func New[T any](listenAddr, remoteAddr string) (*TCPC[T], error) {
	tcpc := TCPC[T]{
		listenAddr: listenAddr,
		remoteAddr: remoteAddr,
		recvCh:     make(chan T),
		sendCh:     make(chan T),
	}

	return &tcpc, nil
}

type SenderConfig struct {
	Retries      int
	DialInterval time.Duration
	RemoteAddr   string
}

func DefaultSenderConfig(remoteAddr string) *SenderConfig {
	return &SenderConfig{
		RemoteAddr:   remoteAddr,
		Retries:      defaultMaxRetries,
		DialInterval: defaultDialInterval,
	}
}

type Sender[T any] struct {
	*SenderConfig

	Chan           chan T
	currentRetries int
	outboundConn   net.Conn
}

func NewSender[T any](cfg *SenderConfig) (*Sender[T], error) {
	sender := &Sender[T]{
		Chan:         make(chan T),
		SenderConfig: cfg,
	}

	go sender.dialRemote()

	return sender, nil
}

func (s *Sender[T]) dialRemote() {
	if s.currentRetries >= s.Retries {
		log.Printf("Max retries.")
		return
	}

	conn, err := net.Dial("tcp", s.RemoteAddr)
	if err != nil {
		log.Printf("Dial error (%s).\nRetrying in (%d)..", err, s.DialInterval)
		time.Sleep(s.DialInterval)

		s.currentRetries++
		s.dialRemote()
	}

	s.outboundConn = conn

	go s.loop()
}

func (s *Sender[T]) loop() {
	for {
		msg := <-s.Chan
		if err := gob.NewEncoder(s.outboundConn).Encode(msg); err != nil {
			log.Println(err)
		}
	}
}

type Reciever[T any] struct {
	Chan chan T

	listenAddr string
	listener   net.Listener
}

func NewReciever[T any](listenAddr string) (*Reciever[T], error) {
	recv := &Reciever[T]{
		listenAddr: listenAddr,
		Chan:       make(chan T),
	}

	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, err
	}
	recv.listener = ln

	go recv.acceptLoop()
	return recv, nil
}

func (r *Reciever[T]) acceptLoop() {
	defer func() {
		r.listener.Close()
	}()

	for {
		conn, err := r.listener.Accept()
		if err != nil {
			log.Println("error to accept connection:", err)
			return
		}

		go r.handleConn(conn)
	}
}

func (r *Reciever[T]) handleConn(conn net.Conn) {
	for {
		var message T
		if err := gob.NewDecoder(conn).Decode(&message); err != nil {
			log.Println("error to decode message via gob:", err)
			continue
		}

		r.Chan <- message
	}
}
