package xmqclientgo

import (
	pb "Xmq-client-go/proto"
	"Xmq-client-go/queue"
	"errors"
	"fmt"
)

type Publisher struct {
	fullName  string
	Opt       PublisherOpt
	client    *Client
	asyncSend *AsyncSend
}

func NewPublisher(srvUrl string, host string, port int, name string, topic string, opt ...PubOption) (*Publisher, error) {
	Option := default_publisher
	Option.srvUrl = srvUrl
	Option.host = host
	Option.port = port
	Option.name = name
	Option.topic = topic

	for _, o := range opt {
		o.set(&Option)
	}
	if Option.partitionNum != 1 {
		return nil, errors.New("partitionNum out of range")
	}

	as := &AsyncSend{
		AsyncSendQueue: queue.New(),
		asyncSendCh:    make(chan bool, Option.AsyncMaxSendBufSize),
	}
	p := &Publisher{
		Opt:       Option,
		asyncSend: as,
	}
	p.client.OperationMaxRedoNum = int32(Option.OperationMaxRedoNum)
	return p, nil
}

func (p *Publisher) Connect() error {
	cliUrl := fmt.Sprintf("%v:%v", p.Opt.host, p.Opt.port)
	name, err := p.client.Connect(p.Opt.srvUrl, cliUrl, p.Opt.name, p.Opt.topic, int32(p.Opt.partitionNum), p.Opt.ConnectTimeout)
	if err != nil {
		return err
	}
	p.fullName = name

	go p.asyncPush()

	return nil
}

func (p *Publisher) Publish(m *Msg) error {
	args := &pb.PublishArgs{
		Name:      p.fullName,
		Topic:     m.topic,
		Partition: int32(m.partition),
		Mid:       nrand(),
		Payload:   string(m.data),
		Redo:      0,
	}
	_, err := p.client.Push2serverWithRedo(args, p.Opt.OperationTimeout)
	if err != nil {
		return err
	}
	return nil
}

// callback ?
func (p *Publisher) AsyncPublish(m *Msg) error {
	if p.asyncSend.AsyncSendQueue.Size() >= p.Opt.AsyncMaxSendBufSize {
		return errors.New("AsyncMaxSendBufSize is full")
	}
	p.asyncSend.AsyncSendQueue.Push(m)
	p.asyncSend.asyncSendCh <- true
	return nil
}

func (p *Publisher) asyncPush() {
	for {
		<-p.asyncSend.asyncSendCh
		for !p.asyncSend.AsyncSendQueue.Empty() {
			m := p.asyncSend.AsyncSendQueue.Front()
			if err := p.Publish(m.(*Msg)); err != nil {
				// todo
			}
			p.asyncSend.AsyncSendQueue.Pop()
		}
	}
}
