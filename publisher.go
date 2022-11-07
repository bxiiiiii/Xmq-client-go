package xmqclientgo

import (
	"errors"
	"time"
)

type Publisher struct {
	client  *Client
}

 const MaxRedo = 3

 func NewPublisher(topic string) (*Publisher,error){
	p := &Publisher{}
	url := GetBrokers(topic)
	client, err := Connect(url[0])
	if err != nil {
		return nil , err
	}
	p.client = client
	return p, nil	
 }

func (p *Publisher) publish(m *Msg) error{
	if m.redo > MaxRedo {
		return errors.New("match max redo")
	}
	err := p.client.Publish(m.topic, m.data)
	if err != nil {
		return err
	}
	select {
	case  <- m.ch:
		return	nil
	case <-time.After(3 * time.Second):
		m.redo++
		p.publish(m)
	}
	return nil
}