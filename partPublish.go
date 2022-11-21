package xmqclientgo

import (
	rc "Xmq-client-go/RegistraionCenter"
	pb "Xmq-client-go/proto"

	"fmt"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// RouteMode
const (
	RoundRobinPartition = iota
	SinglePartition
	CustomPartition
)

type PartPublisher struct {
	name         string
	clients      map[string]*Client
	partitionNum int
}

func (p *PartPublisher) msg2part(key int64) int {
	part := 0
	part %= p.partitionNum
	return part
}

func NewPartPulisher(topic string, name string) (*PartPublisher, error) {
	lis, err := net.Listen("tcp", cliUrl)
	if err != nil {
		return nil, err
	}
	s := grpc.NewServer()
	pb.RegisterClientServer(s, &Client{})
	if err := s.Serve(lis); err != nil {
		return nil, err
	}

	p := &PartPublisher{}
	p.clients = make(map[string]*Client)

	// TODO: use another way to get server url ?
	brokers, err := rc.ZkCli.GetBrokers(topic)
	if err != nil {
		return nil, err
	}

	for _, b := range brokers {
		pName := fmt.Sprintf(partitionKey, b.TopicName, b.Partition)
		conn, err := grpc.Dial(b.Url, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		p.clients[pName] = &Client{conn: conn}

		args := &pb.ConnectArgs{
			Name:      name,
			Url:       cliUrl,
			Redo:      0,
			Topic:     b.TopicName,
			Partition: int32(b.Partition),
		}
		timeout := 1
		_, err = p.clients[pName].Connect2serverWithRedo(args, timeout)
		if err != nil {
			return nil, err
		}
		//TODO: consider confict name
	}

	return p, nil
}

func (p *PartPublisher) publish(m *Msg) error {
	timeout := 1
	args := &pb.PublishArgs{
		Name:    p.name,
		Topic:   m.topic,
		Mid:     nrand(),
		Payload: string(m.data),
		Redo:    0,
	}

	switch m.mode {
	case RoundRobinPartition:
		//todo
	case CustomPartition:
		args.Partition = int32(m.partition)
		pName := fmt.Sprintf(partitionKey, m.topic, m.partition)
		_, err := p.clients[pName].Push2serverWithRedo(args, timeout)
		if err != nil {
			return err
		}
	default:
		args.Partition = int32(p.msg2part(m.id))
		pName := fmt.Sprintf(partitionKey, m.topic, args.Partition)
		_, err := p.clients[pName].Push2serverWithRedo(args, timeout)
		if err != nil {
			return err
		}
	}
	return nil
}
