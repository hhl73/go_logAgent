package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.etcd.io/etcd/clientv3"
)

var (
	client *clientv3.Client
)

type LogEntry struct {
	Path  string `json:"path"`  //日志存放的路径
	Topic string `json:"topic"` //日志要发往kafka的topic
}

func Init(add string, interval int) (err error) {
	client, err = clientv3.New(clientv3.Config{
		Endpoints:   []string{add},
		DialTimeout: time.Duration(interval) * time.Second,
	})
	if err != nil {
		fmt.Println("connect to etcd failed,err:", err)
		return
	}
	return
}

func GetConf(key string) (logEntryConf []*LogEntry, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	resp, err := client.Get(ctx, key)
	cancel()
	if err != nil {
		fmt.Printf("put to etcd failed, err", err)
		return
	}
	for _, ev := range resp.Kvs {
		fmt.Printf("%s:%s\n", ev.Value, logEntryConf)
		err = json.Unmarshal(ev.Value, &logEntryConf)
		if err != nil {
			fmt.Println("unmarshal etcd value failed,", err)
			return
		}
	}
	return
}

func WatchConf(key string, newConfChan chan<- []*LogEntry) {
	ch := client.Watch(context.Background(), key)
	for wresp := range ch {
		for _, evt := range wresp.Events {
			fmt.Printf("Type:%v key:%v value:%v\n", evt.Type, string(evt.Kv.Key), string(evt.Kv.Value))
			var newConf []*LogEntry
			//如果是删除操作，json.Unmarshal会报错，需手动添加一个空的newConf
			if evt.Type != clientv3.EventTypeDelect {
				err := json.Unmarshal(evt.Kv.Value, &newConf)
				if err != nil {
					fmt.Printf("unmarshal new conf failed, err:%v\n", err)
					continue
				}
			}
			newConfChan <- newConf
		}
	}
}

// func PutConf(key string) {
// 	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
// 	_, err := cliv3.Put(ctx, "baodelu", "dsb")
// 	cancel()
// 	if err != nil {
// 		fmt.Printf("put to etcd failed, err", err)
// 		return
// 	}
// }
