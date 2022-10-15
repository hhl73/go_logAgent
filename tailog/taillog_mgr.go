/**
 * @Author: Mr.Cheng
 * @Description:
 * @File: taillogMgr
 * @Version: 1.0.0
 * @Date: 2021/12/14 下午3:47
 */

package taillog

import (
	"fmt"
	"time"

	"github.com/hhl73/etcd"
)

type TailMgr struct {
	logEntry    []*etcd.LogEntry
	tskMap      map[string]*TailTask
	newConfChan chan []*etcd.LogEntry
}

var tskMgr *TailMgr

// 循环每一个日志收集项，创建tailObj，并发往kafka
func Init(logEntryConf []*etcd.LogEntry) {
	tskMgr = &TailMgr{
		logEntry:    logEntryConf,
		tskMap:      make(map[string]*TailTask, 16),
		newConfChan: make(chan []*etcd.LogEntry),
	}

	for _, LogEntry := range logEntryConf {
		// fmt.Printf("Path:%v Topic:%v\n", LogEntry.Path, LogEntry.Topic)
		tailtask, err := NewTailTask(LogEntry.Path, LogEntry.Topic)
		if err != nil {
			continue
		}
		// 在tskMap中存储一下，以便发生配置变更时做增删改操作
		key := fmt.Sprintf("%s_%s", tailtask.Path, tailtask.Topic)
		tskMgr.tskMap[key] = tailtask
	}

	go tskMgr.run()
}

// 监听newConfChan是否有数据，有数据则表示etcd配置有变化，需做相应的处理
func (t *TailMgr) run() {
	for {
		select {
		case newConf := <-t.newConfChan:
			fmt.Printf("配置发生变更，Conf:%v\n", newConf)
			// 找出新增项
			for _, logEntry := range newConf {
				key := fmt.Sprintf("%s_%s", logEntry.Path, logEntry.Topic)
				_, ok := t.tskMap[key]
				if ok {
					// 表示该配置项原先存在
					continue
				} else {
					// 属于新增配置
					fmt.Printf("新增项，path:%s topic:%s\n", logEntry.Path, logEntry.Topic)
					tailtask, err := NewTailTask(logEntry.Path, logEntry.Topic)
					if err != nil {
						continue
					}
					// TailMgr的logEntry和tskMap增加对应项
					t.logEntry = append(t.logEntry, logEntry)
					t.tskMap[key] = tailtask
					go tailtask.ReadFromTail()
				}
			}
			// 找出删除项
			for index, c1 := range t.logEntry {
				isDelete := true
				for _, c2 := range newConf {
					if c1.Path == c2.Path && c1.Topic == c2.Topic {
						isDelete = false
						break
					}
				}
				if isDelete {
					// 表示属于删除项，从tskMap拿出tailtask对象，执行对象的cancel函数，并将该对象从tskMap中删除
					fmt.Printf("删除项，path:%s topic:%s\n", c1.Path, c1.Topic)
					key := fmt.Sprintf("%s_%s", c1.Path, c1.Topic)
					t.tskMap[key].cancel()
					// TailMgr的logEntry和tskMap删除对应项
					delete(t.tskMap, key)
					t.logEntry = append(t.logEntry[:index], t.logEntry[index+1:]...)
				}
			}
		default:
			time.Sleep(time.Second)
		}
	}
}

// 向外暴露newConfChan
func NewConfChan() chan<- []*etcd.LogEntry {
	return tskMgr.newConfChan
}
