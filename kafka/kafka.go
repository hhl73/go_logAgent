/**
 * @Author: Mr.Cheng
 * @Description:往kafka写入日志
 * @File: kafka
 * @Version: 1.0.0
 * @Date: 2021/12/9 下午2:19
 */

package kafka

import (
	"fmt"
	"time"

	"github.com/Shopify/sarama"
)

type logData struct {
	Topic string
	Data  string
}

var (
	client      sarama.SyncProducer // 全局连接kafka的生产者
	logDataChan chan *logData
)

// 初始化连接
func Init(address []string, size int) (err error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll          // 发送模式（需leader和follow都确认）
	config.Producer.Partitioner = sarama.NewRandomPartitioner // 选择分区的方式（轮询）
	config.Producer.Return.Successes = true                   // 成功交付的消息将在success channel中返回

	// 连接kafka
	client, err = sarama.NewSyncProducer(address, config)
	if err != nil {
		fmt.Printf("client kafka failed, err:%v\n", err)
		return err
	}

	// 初始化logDataChan
	logDataChan = make(chan *logData, size)

	// 从logDataChan中取数据发往kafaka
	go sendToKafka()
	return nil
}

func SendToChan(Topic, Data string) {
	data := &logData{
		Topic: Topic,
		Data:  Data,
	}
	select {
	case logDataChan <- data:
	default:
		time.Sleep(time.Millisecond * 100)
	}
}

func sendToKafka() {
	// 循环从通道logDataChan取值并发送给kafka
	for {
		select {
		case data := <-logDataChan:
			msg := &sarama.ProducerMessage{}
			msg.Topic = data.Topic
			msg.Value = sarama.StringEncoder(data.Data)
			pid, offset, err := client.SendMessage(msg)
			if err != nil {
				fmt.Printf("send msg failed, err:%v\n", err)
			}
			fmt.Printf("send msg success, pid:%v offect:%v\n", pid, offset)
		default:
			time.Sleep(time.Millisecond * 50)
		}
	}
}
