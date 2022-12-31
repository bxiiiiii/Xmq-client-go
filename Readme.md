# Xmq-client-go

A Go client library for of Xmq.

# 用法

## 发布者

### 创建配置

- 发布方式
- 超时时间
- 异步队列大小
  
...

### 样例
``` go
opts := []xmq.PubOption{
	xmq.WithpMode(xmq.PMode_Shared),
}
puber, err := xmq.NewPublisher("localhost:8888", "localhost", 7777, "TestTopic", opt...)
if err != nil {
	panic(err)
}

if err := puber.Connect(); err != nil {
	panic(err)
}

msg := &xmq.Msg{
	Data: []byte("hello Xmq")
}
if err := puber.Publish(msg); err != nil {
	panic(err)
}
```

## 订阅者

### 创建配置

订阅者：
- 超时时间
- 重传次数

...

订阅：
- 订阅方式
- 订阅分区
- 订阅位点
- 接收队列大小

...
### 样例
``` go
suber := cli.NewSubscriber("localhost:8888", "localhost", 8877, "s2")

opts := []cli.SubscipOption{
	cli.WithspMode(cli.SMode_Failover),
}
subscription, err := suber.Subscribe("testSubscrption1" "testTopic", opt...)
if err != nil {
  	panic(err)
}

msg, err := subscription.Receive()
if err != nil {
	fmt.Println(msg)
	subscription.MsgAck(msg)
}
```
