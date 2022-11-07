package xmqclientgo

import "errors"

type Subscriber struct {
	clients map[int]*Client
}

func NewSubscriber(topic string) (*Subscriber, error) {
	s := &Subscriber{}

	urls := GetBrokers(topic)
	for _, url := range urls {
		client, err := Connect(url)
		if err != nil {

		}
		s.clients = append(s.clients, client)
	}

	return s, nil
}

func (s *Subscriber) subscribe(sub *subcription) error {
	switch sub.partition {
	case -1:
		for c, ok := range s.clients {
			if !ok {
				//todo
			}
			c.subscribe(sub)
		}
	default:
		c, ok := s.clients[sub.partition]
		if !ok {
			return errors.New("partition not exist.")
		}
		c.subsribe(sub)
	}

	return nil
}
