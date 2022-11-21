package xmqclientgo

import (
	rc "Xmq-client-go/RegistraionCenter"
	pb "Xmq-client-go/proto"
	"fmt"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	Exclusive = iota
	Shared
	Key_Shared
)

type MsgHandler func(msg *Msg)

type Subscriber struct {
	name string
	sl   map[string]*subcription
}

type subcription struct {
	name       string
	topic      string
	subType    int
	mcb        MsgHandler
	mch        chan *Msg
	partition  int
	clients    map[string]*Client
	magbuf     []Msg
	pushOffset uint64
}

func NewSubscriber(name string) (*Subscriber, error) {
	s := &Subscriber{
		name: name,
		sl:   make(map[string]*subcription),
	}

	return s, nil
}

func (s *Subscriber) subscribe(sub *subcription) error {
	lis, err := net.Listen("tcp", cliUrl)
	if err != nil {
		return err
	}
	gs := grpc.NewServer()
	pb.RegisterClientServer(gs, &Client{})
	if err := gs.Serve(lis); err != nil {
		return err
	}

	s.sl[sub.name] = sub
	switch sub.partition {
	case -1:
		// TODO: use another way to get server url ?
		brokers, err := rc.ZkCli.GetBrokers(sub.topic)
		if err != nil {
			return err
		}
		for _, b := range brokers {
			if err := s.connectAndSendSub2server(sub, b); err != nil {
				return err
			}
		}
	default:
		// TODO: use another way to get server url ?
		b, err := rc.ZkCli.GetBroker(sub.topic, sub.partition)
		if err != nil {
			return err
		}
		if err := s.connectAndSendSub2server(sub, b); err != nil {
			return err
		}

	}

	return nil
}

func (s *Subscriber) connectAndSendSub2server(sub *subcription, b *rc.PartitionNode) error {
	pName := fmt.Sprintf(partitionKey, b.TopicName, b.Partition)
	conn, err := grpc.Dial(b.Url, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	s.sl[sub.name].clients[pName] = &Client{conn: conn}

	cArgs := &pb.ConnectArgs{
		Name:      s.name,
		Url:       cliUrl,
		Redo:      0,
		Topic:     b.TopicName,
		Partition: int32(b.Partition),
	}
	timeout := 1
	_, err = s.sl[sub.name].clients[pName].Connect2serverWithRedo(cArgs, timeout)
	if err != nil {
		return err
	}
	//TODO: consider confict name

	sArgs := &pb.SubscribeArgs{
		Topic:        b.TopicName,
		Partition:    int32(b.Partition),
		Subscription: sub.name,
		SubType:      pb.SubscribeArgs_SubType(sub.subType),
		PushOffset:   sub.pushOffset,
		Redo:         0,
	}
	_, err = s.sl[sub.name].clients[pName].SubscribeWithRedo(sArgs, timeout)

	return nil
}

func (s *Subscriber) unsubscribe(sub *subcription) error {
	for _, c := range sub.clients {
		args := &pb.UnSubscribeArgs{
			Name:         s.name,
			Topic:        sub.topic,
			Partition:    int32(sub.partition),
			Subscription: sub.name,
			Redo:         0,
		}
		timeout := 1
		_, err := c.UnSubscribeWithRedo(args, timeout)
		if err != nil {
			return err
		}
	}
	return nil
}
