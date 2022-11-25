package xmqclientgo

type PublisherOpt struct {
	name                string
	host                string
	port                int
	srvUrl              string
	topic               string
	partitionNum        int32
	mode                PublishMode
	ConnectTimeout      int
	OperationTimeout    int
	OperationMaxRedoNum int
	AsyncMaxSendBufSize int
}

var default_publisher = PublisherOpt{
	partitionNum:        1,
	mode:                PMode_Shared,
	ConnectTimeout:      30,
	OperationTimeout:    30,
	OperationMaxRedoNum: 3,
	AsyncMaxSendBufSize: 1000,
}

type PubOption interface {
	set(opt *PublisherOpt)
}

type funcPubOption struct {
	f func(opt *PublisherOpt)
}

func newfuncPubOption(f func(opt *PublisherOpt)) *funcPubOption {
	return &funcPubOption{
		f: f,
	}
}

func (fpo *funcPubOption) set(po *PublisherOpt) {
	fpo.f(po)
}

func WithMode(mode PublishMode) PubOption {
	return newfuncPubOption(func(opt *PublisherOpt) {
		opt.mode = mode
	})
}

func WithConnectTimeout(timeout int) PubOption {
	return  newfuncPubOption(func(opt *PublisherOpt) {
		opt.ConnectTimeout = timeout
	})
}

func WithOperationTimeout(timeout int) PubOption {
	return newfuncPubOption(func(opt *PublisherOpt) {
		opt.OperationTimeout = timeout
	})
}

func WithAsyncMaxSendBufSize(size int) PubOption {
	return newfuncPubOption(func(opt *PublisherOpt) {
		opt.AsyncMaxSendBufSize = size
	})
}

func WithOperationMaxRedoNum(num int) PubOption {
	return newfuncPubOption(func(opt *PublisherOpt) {
		opt.OperationMaxRedoNum = num
	})
}

func WithPartitionNum(num int) PubOption {
	return newfuncPubOption(func(opt *PublisherOpt) {
		opt.partitionNum = int32(num)
	})
}
