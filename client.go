package xmqclientgo

import (
	pb "Xmq-client-go/proto"
	"context"
	"crypto/rand"
	"errors"
	"math/big"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Client struct {
	topic     string
	partition int
	conn      *grpc.ClientConn

	pb.UnimplementedClientServer
}

type Msg struct {
	id        int64
	topic     string
	data      []byte
	mode      int
	ch        chan bool
	redo      int
	partition int
}

var (
	cliUrl = "localhost:8888"
)

const (
	subcriptionKey = "%s/p%d/%s" // topic/partition/subcriptionName
	msgKey         = "%s/p%d/%d" // topic/partition/msid
	partitionKey   = "%s/p%d"    //topic/partition
)

func (c *Client) Connect2serverWithRedo(args *pb.ConnectArgs, timeout int) (*pb.ConnectReply, error) {
	if args.Redo >= MaxRedo {
		return nil, errors.New("match max redo")
	}

	reply, err := c.Connet2server(args, timeout)
	if err != nil {
		if ok := c.CheckTimeout(err); ok {
			args.Redo += 1
			return c.Connect2serverWithRedo(args, timeout)
		}
		return nil, err
	}
	return reply, nil
}

func (c *Client) Connet2server(args *pb.ConnectArgs, timeout int) (*pb.ConnectReply, error) {
	cli := pb.NewServerClient(c.conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(timeout))
	defer cancel()
	return cli.Connect(ctx, args)
}

func (c *Client) Push2serverWithRedo(args *pb.PublishArgs, timeout int) (*pb.PublishReply, error) {
	if args.Redo >= MaxRedo {
		return nil, errors.New("match max redo")
	}

	reply, err := c.Push2server(args, timeout)
	if err != nil {
		if ok := c.CheckTimeout(err); ok {
			args.Redo += 1
			return c.Push2serverWithRedo(args, timeout)
		}
	}
	return reply, nil
}

func (c *Client) Push2server(args *pb.PublishArgs, timeout int) (*pb.PublishReply, error) {
	cli := pb.NewServerClient(c.conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(timeout))
	defer cancel()
	return cli.ProcessPub(ctx, args)
}

func (c *Client) SubscribeWithRedo(args *pb.SubscribeArgs, timeout int) (*pb.SubscribeReply, error) {
	if args.Redo >= MaxRedo {
		return nil, errors.New("match max redo")
	}

	reply, err := c.Subscribe(args, timeout)
	if err != nil {
		if ok := c.CheckTimeout(err); ok {
			args.Redo += 1
			return c.SubscribeWithRedo(args, timeout)
		}
	}
	return reply, nil
}

func (c *Client) Subscribe(args *pb.SubscribeArgs, timeout int) (*pb.SubscribeReply, error) {
	cli := pb.NewServerClient(c.conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(timeout))
	defer cancel()
	return cli.ProcessSub(ctx, args)
}

func (c *Client) UnSubscribeWithRedo(args *pb.UnSubscribeArgs, timeout int) (*pb.UnSubscribeReply, error) {
	if args.Redo >= MaxRedo {
		return nil, errors.New("match max redo")
	}

	reply, err := c.UnSubscribe(args, timeout)
	if err != nil {
		if ok := c.CheckTimeout(err); ok {
			args.Redo += 1
			return c.UnSubscribeWithRedo(args, timeout)
		}
	}
	return reply, nil
}

func (c *Client) UnSubscribe(args *pb.UnSubscribeArgs, timeout int) (*pb.UnSubscribeReply, error) {
	cli := pb.NewServerClient(c.conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(timeout))
	defer cancel()
	return cli.ProcessUnSub(ctx, args)
}

func (c *Client) CheckTimeout(err error) bool {
	statusErr, ok := status.FromError(err)
	if ok && statusErr.Code() == codes.DeadlineExceeded {
		return true
	}
	return false
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}
