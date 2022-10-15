package main

import (
	"fmt"
	"logAgent/etcd"
	"sync"
	"time"

	"github.com/hhl73/conf"

	"github.com/hhl73/etcd"
	"github.com/hhl73/kafka"
	"github.com/hhl73/tailog"
	"gopkg.in/ini.v1"
)

var (
	cfg = new(conf.AppConf)
	wg  sync.WaitGroup
)

// logAgent程序入口
func run() {
	//读取日志
	for {
		select {
		case line := <-tailog.ReadChan():
			//发送到kafka
			kafka.SendToKafka(cfg.KafkaConf.Topic, line.Text)
		default: //没有日志
			time.Sleep(time.Second)
		}
	}
}

func main() {
	// 加载配置文件
	err := ini.MapTo(cfg, "./conf/logAgent.ini")
	if err != nil {
		fmt.Printf("load ini failed, err:%v\n", err)
		return
	}

	// 初始化kafka连接
	err = kafka.Init([]string{cfg.KafkaConf.Address}, cfg.KafkaConf.Size)
	if err != nil {
		return
	}
	fmt.Println("init kafka success")

	// 初始化etcd
	err = etcd.Init(cfg.EtcdConf.Address, cfg.EtcdConf.Timeout)
	if err != nil {
		return
	}
	fmt.Println("init etcd success")

	// 从etcd中获取日志收集项的配置信息
	logEntryConf, err := etcd.GetConf(cfg.EtcdConf.Key)
	if err != nil {
		return
	}
	fmt.Printf("get conf from etcd success, conf:%v\n", logEntryConf)

	// 收集日志发往kafka
	// 循环每一个日志收集项，创建tailObj，并发往kafka
	taillog.Init(logEntryConf)

	// 监视etcd中配置的变动，如有变动，给新的配置信息给taillog
	wg.Add(1)
	go etcd.WatchConf(cfg.EtcdConf.Key, taillog.NewConfChan())
	wg.Wait()
}
