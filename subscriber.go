package xmqclientgo

import (
	"fmt"
)

type Subscriber struct {
	sl map[string]*subcription
}

func NewSubscriber() (*Subscriber, error) {
	s := &Subscriber{}

	// urls := GetBrokers(topic)
	// for _, url := range urls {
	// 	client, err := Connect(url)
	// 	if err != nil {

	// 	}
	// 	s.clients = append(s.clients, client)
	// }

	return s, nil
}

func (s *Subscriber) subscribe(sub *subcription) error {
	switch sub.partition {
	case -1:
		brokers := GetBrokers(sub.topic)
		for _, b := range brokers {
			c, err := Connect(b.url)
			if err != nil {

			}
			sub.clients[b.name] = c
		}
		for _, c := range sub.clients {
			c.subscribe(sub)
		}
	default:
		b := GetBroker(sub.topic, sub.partition)
		c, err := Connect(b.url)
		if err != nil {
			return err
		}
		sub.clients[b.name] = c
		c.subscribe(sub)
	}
	s.sl[sub.name] = sub

	return nil
}

func (s *Subscriber) unsubscribe(sub *subcription) {
	for _, c := range sub.clients {
		c.bw.WriteString(fmt.Sprintf(unsubProto, sub))
	}
}
