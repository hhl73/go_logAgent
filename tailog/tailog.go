/**
 * @Author: Mr.Cheng
 * @Description:收集日志模块
 * @File: taillog
 * @Version: 1.0.0
 * @Date: 2021/12/8 下午9:54
 */

package taillog

import (
	"context"
	"fmt"
	"github/hhl73/kafka"
	"time"

	"github.com/hpcloud/tail"
)

type TailTask struct {
	Path     string
	Topic    string
	Instance *tail.Tail
	// 为了停止任务，存下context
	ctx    context.Context
	cancel context.CancelFunc
}

func NewTailTask(Path, Topic string) (tailtask *TailTask, err error) {
	config := tail.Config{
		Location:  &tail.SeekInfo{Offset: 0, Whence: 2}, // 从文件那个地方开始读
		ReOpen:    true,                                 // 重新打开
		MustExist: false,                                // 文件不存在不报错
		Poll:      true,
		Follow:    true, // 是否跟随
	}
	ctx, cancel := context.WithCancel(context.Background())
	tailObj, err := tail.TailFile(Path, config)
	if err != nil {
		fmt.Printf("tail file failed, err:%v\n", err)
		return nil, err
	}
	tailtask = &TailTask{Path: Path, Topic: Topic, Instance: tailObj, ctx: ctx, cancel: cancel}
	// 开启读取日志并发送给kafka
	go tailtask.ReadFromTail()
	return tailtask, nil
}

func (tailtask *TailTask) ReadFromTail() {
	for {
		select {
		case <-tailtask.ctx.Done():
			return
		case line, ok := <-tailtask.Instance.Lines:
			if !ok {
				fmt.Printf("tail fail close reopen, filename:%s\n", tailtask.Path)
				time.Sleep(time.Second)
				continue
			}
			kafka.SendToChan(tailtask.Topic, line.Text)
		default:
			time.Sleep(time.Second)
		}
	}
}
