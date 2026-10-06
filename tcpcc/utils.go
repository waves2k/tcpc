package tcpcc

type ReconnectionManager[T any] struct {
	tcpc *TCPC[T]

	connBroken chan struct{}
}

func NewReconnectManager[T any](tcpc *TCPC[T]) *ReconnectionManager[T] {
	manager := &ReconnectionManager[T]{
		tcpc: tcpc,

		connBroken: make(chan struct{}),
	}

	return manager
}

func (m *ReconnectionManager[T]) StartListen() {
	for {
		<-m.connBroken

		m.tcpc.dialRemote()
	}
}
