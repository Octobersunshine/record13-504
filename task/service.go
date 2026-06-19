package task

import (
	"context"
	"log"
	"time"

	"distributed-lock-demo/lock"
)

type TaskService struct {
	lockSvc    *lock.DistributedLock
	lockTTL    time.Duration
	refreshInt time.Duration
}

type TaskResult struct {
	Success   bool   `json:"success"`
	Message   string `json:"message"`
	TaskID    string `json:"task_id,omitempty"`
	Executed  bool   `json:"executed"`
	Instance  string `json:"instance_id"`
}

func NewTaskService(lockSvc *lock.DistributedLock) *TaskService {
	return &TaskService{
		lockSvc:    lockSvc,
		lockTTL:    30 * time.Second,
		refreshInt: 10 * time.Second,
	}
}

func (s *TaskService) ExecuteTask(ctx context.Context, taskID string, instanceID string, workFn func() error) *TaskResult {
	lockKey := "task:lock:" + taskID

	acquiredLock, ok, err := s.lockSvc.Acquire(ctx, lockKey, s.lockTTL)
	if err != nil {
		log.Printf("[%s] 获取锁失败: %v", instanceID, err)
		return &TaskResult{
			Success:  false,
			Message:  "获取分布式锁异常",
			Executed: false,
			Instance: instanceID,
		}
	}
	if !ok {
		log.Printf("[%s] 任务 %s 已被其他实例占用", instanceID, taskID)
		return &TaskResult{
			Success:  true,
			Message:  "任务正在被其他实例执行",
			TaskID:   taskID,
			Executed: false,
			Instance: instanceID,
		}
	}

	log.Printf("[%s] 成功获取任务锁: %s", instanceID, taskID)

	refreshCtx, cancelRefresh := context.WithCancel(ctx)
	defer cancelRefresh()

	go s.startLockRefresher(refreshCtx, acquiredLock, s.refreshInt, s.lockTTL, instanceID)

	defer func() {
		cancelRefresh()
		if err := acquiredLock.Release(); err != nil {
			log.Printf("[%s] 释放锁失败: %v", instanceID, err)
		} else {
			log.Printf("[%s] 已释放任务锁: %s", instanceID, taskID)
		}
	}()

	log.Printf("[%s] 开始执行任务: %s", instanceID, taskID)
	if err := workFn(); err != nil {
		log.Printf("[%s] 任务执行失败: %s, err: %v", instanceID, taskID, err)
		return &TaskResult{
			Success:  false,
			Message:  "任务执行失败: " + err.Error(),
			TaskID:   taskID,
			Executed: true,
			Instance: instanceID,
		}
	}

	log.Printf("[%s] 任务执行成功: %s", instanceID, taskID)
	return &TaskResult{
		Success:  true,
		Message:  "任务执行成功",
		TaskID:   taskID,
		Executed: true,
		Instance: instanceID,
	}
}

func (s *TaskService) startLockRefresher(ctx context.Context, l *lock.Lock, interval time.Duration, ttl time.Duration, instanceID string) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ok, err := l.Refresh(ttl)
			if err != nil {
				log.Printf("[%s] 刷新锁异常: %v", instanceID, err)
				return
			}
			if !ok {
				log.Printf("[%s] 锁已失效，停止刷新", instanceID)
				return
			}
			log.Printf("[%s] 锁已续约", instanceID)
		}
	}
}
