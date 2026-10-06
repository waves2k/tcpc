package tcpcc

import (
	"encoding/gob"
	"errors"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

// Errors
var (
	ErrMaxConnectRetries = errors.New("max retries to connect to remote addr")
	ErrConfigureing      = errors.New("error to configure new channel reader/writer")
)

// Global variables
var (
	defaultMaxConnectRetries    = 5
	defaultMaxReconnectRetries  = 3
	defaultDialInterval         = time.Second * 3
	defaultTriesToDecodeMessage = 3
)

type TCPCConfig struct {
	ListenAddr           string
	RemoteAddr           string
	ConnectRetries       int
	ReconnectRetries     int
	DialInterval         time.Duration
	TriesToDecodeMessage int
}

func DefaultTCPCConfig(listenAddr, remoteAddr string) *TCPCConfig {
	return &TCPCConfig{
		ListenAddr:           listenAddr,
		RemoteAddr:           remoteAddr,
		ConnectRetries:       defaultMaxConnectRetries,
		ReconnectRetries:     defaultMaxReconnectRetries,
		DialInterval:         defaultDialInterval,
		TriesToDecodeMessage: defaultTriesToDecodeMessage,
	}
}

type TCPC[T any] struct {
	*TCPCConfig

	listener net.Listener

	done chan struct{}

	connBroken   chan struct{}
	connMu       sync.Mutex
	outboundConn net.Conn

	senderChan   chan T
	recieverChan chan T
}

func NewT[T any](cfg *TCPCConfig) (*TCPC[T], error) {
	if cfg == nil {
		return nil, ErrConfigureing
	}

	tcpc := &TCPC[T]{
		TCPCConfig:   cfg,
		senderChan:   make(chan T),
		recieverChan: make(chan T),
		connBroken:   make(chan struct{}, 1),
		done:         make(chan struct{}),
	}

	go tcpc.startListening()

	listener, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		return nil, err
	}

	tcpc.listener = listener
	go tcpc.acceptLoop()

	return tcpc, nil
}

func (s *TCPC[T]) dialRemote() net.Conn {
	var (
		conn net.Conn
		err  error
	)

	ticker := time.NewTicker(s.DialInterval)
	defer ticker.Stop()

	for attempt := 1; attempt <= s.ConnectRetries; attempt++ {
		conn, err = net.Dial("tcp", s.RemoteAddr)
		if err == nil {
			return conn
		}

		log.Printf("dial error: %v, retry %d/%d\n", err, attempt, s.ConnectRetries)

		if attempt < s.ConnectRetries {
			<-ticker.C
		}
	}

	return nil
}

func (s *TCPC[T]) Send() chan<- T {
	return s.senderChan
}

func (s *TCPC[T]) Recieve() <-chan T {
	return s.recieverChan
}

func (s *TCPC[T]) sendingLoop(conn net.Conn) {
	encoder := gob.NewEncoder(conn)
	for msg := range s.senderChan {
		if err := encoder.Encode(msg); err != nil {
			log.Printf("encode error: %v, reconnecting..\n", err)

			select {
			case s.connBroken <- struct{}{}:
			default:
			}

			return
		}
	}

	select {
	case s.connBroken <- struct{}{}:
	default:
	}
}

func (s *TCPC[T]) acceptLoop() {
	defer func() {
		s.listener.Close()
	}()

	log.Println("started accepting TCP connections")

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			log.Println("error to accept connection:", err)
			return
		}

		go s.handleConn(conn)
	}
}

func (s *TCPC[T]) startListening() {
	for {
		conn := s.dialRemote()
		if conn == nil {
			log.Println("cannot establish connection, giving up")
			close(s.senderChan)
			return
		}

		go s.sendingLoop(conn)

		<-s.connBroken
		conn.Close()
	}
}

func (s *TCPC[T]) handleConn(conn net.Conn) {
	defer conn.Close()

	decoder := gob.NewDecoder(conn)
	log.Println("started handling remote connection: ", conn.RemoteAddr())

	conn.SetDeadline(time.Now().Add(time.Second * 10))

	for {
		var message T

		if err := decoder.Decode(&message); err != nil {

			switch {
			case errors.Is(err, io.EOF):
				log.Println("remote closed connection:", conn.RemoteAddr())

			case errors.Is(err, net.ErrClosed):
				log.Println("connection closed locally:", conn.RemoteAddr())

			default:
				log.Printf("decode error from %s: %v\n", conn.RemoteAddr(), err)
			}
			return
		}

		select {
		case s.recieverChan <- message:
		case <-s.done:
			return
		}

		conn.SetDeadline(time.Now().Add(time.Second * 10))
	}
}
