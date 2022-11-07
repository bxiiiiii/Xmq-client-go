package xmqclientgo

// RouteMode
const (
	RoundRobinPartition = iota
	SinglePartition
	CustomPartition
)


type PartPublisher struct {
	num int
	clients []*Client
	brokers map[string]*Broker
	client  map[string]*Client
}

func NewPartPulisher(topic string) (*PartPublisher, error) {
	p := &PartPublisher{}
	p.brokers = make(map[string]*Broker)
	p.client = make(map[string]*Client)

	urls := GetBrokers(topic)
	for _, url := range urls {
		client, err := Connect(url)
		if err != nil {

		}
		p.clients = append(p.clients, client)
	}

	return p, nil
}

func (p *PartPublisher) publish(m Msg) (err error) {
	// p.client, err = Connect(url)
	switch m.mode {
	case RoundRobinPartition:
	case CustomPartition:
		
	default:

	}
	return nil
}

func (p *Publisher) getBroker() string {

}
