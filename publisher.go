package xmqclientgo

import (
	rc "Xmq-client-go/RegistraionCenter"
	pb "Xmq-client-go/proto"

	"flag"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Publisher struct {
	name   string
	client *Client
	zkCli  rc.ZkClient
}

const (
	defaultName = "world"
)

var (
	addr = flag.String("addr", "localhost:50051", "the address to connect to")
	name = flag.String("name", defaultName, "Name to greet")
)

const MaxRedo = 3

func NewPublisher(name string, topic string) (*Publisher, error) {
	lis, err := net.Listen("tcp", cliUrl)
	if err != nil {
		return nil, err
	}
	s := grpc.NewServer()
	pb.RegisterClientServer(s, &Client{})
	if err := s.Serve(lis); err != nil {
		return nil, err
	}
	// should let server do ?
	// start
	p := &Publisher{}
	isExists, err := rc.ZkCli.IsTopicExists(topic)
	if err != nil {
		return nil, err
	}

	if !isExists {
		tnode := rc.TopicNode{
			Name: topic,
			Pnum: 1,
		}
		if err := rc.ZkCli.RegisterTnode(tnode); err != nil {
			return nil, err
		}
	}
	// end
	// TODO: use another way to get server url ?
	brokers, err := p.zkCli.GetBrokers(topic)
	if err != nil {
		return nil, err
	}

	conn, err := grpc.Dial(brokers[0].Url, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	p.client = &Client{conn: conn}
	args := &pb.ConnectArgs{
		Name: name,
		Url:  cliUrl,
		Redo: 0,
	}
	reply, err := p.client.Connect2serverWithRedo(args, 1)
	if err != nil {
		return nil, err
	}
	p.name = reply.Name
	// log
	return p, nil
}

func (p *Publisher) publish(m *Msg) error {
	args := &pb.PublishArgs{
		Name:      p.name,
		Topic:     m.topic,
		Partition: int32(m.partition),
		Mid:       nrand(),
		Payload:   string(m.data),
		Redo:      0,
	}
	_, err := p.client.Push2serverWithRedo(args, 0)
	if err != nil {
		return err
	}
	return nil
}
