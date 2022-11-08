package xmqclientgo

import (
	"errors"
	"time"
)

// RouteMode
const (
	RoundRobinPartition = iota
	SinglePartition
	CustomPartition
)

type PartPublisher struct {
	num     int
	clients []*Client
	// brokers map[string]*Broker
	client map[string]*Client
}

func (p *PartPublisher) msg2part(key int64) int {
	part := 0
	part %= p.num
	return part
}

func NewPartPulisher(topic string) (*PartPublisher, error) {
	p := &PartPublisher{}
	// p.brokers = make(map[string]*Broker)
	p.client = make(map[string]*Client)

	brokers := GetBrokers(topic)
	for _, b := range brokers {
		client, err := Connect(b.url)
		if err != nil {
			// todo
		}
		p.clients = append(p.clients, client)
	}

	return p, nil
}

func (p *PartPublisher) publish(m *Msg) error {
	if m.redo > MaxRedo {
		return errors.New("match max redo")
	}
	// p.client, err = Connect(url)
	switch m.mode {
	case RoundRobinPartition:
	case CustomPartition:
		for _, v := range p.clients {
			if v.bi.id == m.partition {
				err := v.Publish(m.topic, m.data)
				if err != nil {
					return err
				}
				select {
				case <-m.ch:
					return nil
				case <-time.After(3 * time.Second):
					m.redo++
					p.publish(m)
				}
				break
			}

		}
	default:
		partition := p.msg2part(m.id)
		for _, v := range p.clients {
			if v.bi.id == partition {
				err := v.Publish(m.topic, m.data)
				if err != nil {
					return err
				}
				select {
				case <-m.ch:
					return nil
				case <-time.After(3 * time.Second):
					m.redo++
					p.publish(m)
				}
				break
			}
		}
	}
	return nil
}
