package task

import (
	"context"
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"distributed-lock-demo/lock"
)

var ErrLockLost = errors.New("分布式锁已丢失，任务被中断")
var ErrTaskNotActive = errors.New("当前实例未持有该任务的锁")

type lockHolder struct {
	lock          *lock.Lock
	cancelTask    context.CancelFunc
	cancelRefresh context.CancelFunc
	acquiredAt    time.Time
	released      atomic.Bool
}

type TaskService struct {
	lockSvc    *lock.DistributedLock
	lockTTL    time.Duration
	refreshInt time.Duration
	holders    sync.Map
}

type TaskResult struct {
	Success   bool   `json:"success"`
	Message   string `json:"message"`
	TaskID    string `json:"task_id,omitempty"`
	Executed  bool   `json:"executed"`
	Instance  string `json:"instance_id"`
}

type ActiveTaskInfo struct {
	TaskID     string `json:"task_id"`
	LockKey    string `json:"lock_key"`
	AcquiredAt string `json:"acquired_at"`
}

func NewTaskService(lockSvc *lock.DistributedLock) *TaskService {
	return &TaskService{
		lockSvc:    lockSvc,
		lockTTL:    30 * time.Second,
		refreshInt: 10 * time.Second,
	}
}

type WorkFunc func(ctx context.Context) error

func (s *TaskService) ExecuteTask(ctx context.Context, taskID string, instanceID string, workFn WorkFunc) *TaskResult {
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

	taskCtx, taskCancel := context.WithCancel(ctx)
	defer taskCancel()

	refreshCtx, refreshCancel := context.WithCancel(context.Background())
	defer refreshCancel()

	holder := &lockHolder{
		lock:          acquiredLock,
		cancelTask:    taskCancel,
		cancelRefresh: refreshCancel,
		acquiredAt:    time.Now(),
	}
	s.holders.Store(taskID, holder)

	go s.startLockRefresher(refreshCtx, taskCancel, acquiredLock, s.refreshInt, s.lockTTL, instanceID)

	defer func() {
		s.holders.Delete(taskID)
		refreshCancel()
		taskCancel()

		if holder.released.Load() {
			log.Printf("[%s] 锁已被手动释放，跳过重复释放: %s", instanceID, taskID)
			return
		}

		if errors.Is(taskCtx.Err(), context.Canceled) {
			log.Printf("[%s] 锁已丢失，跳过主动释放: %s", instanceID, taskID)
			return
		}

		releaseCtx, releaseCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer releaseCancel()
		if err := acquiredLock.Release(releaseCtx); err != nil {
			log.Printf("[%s] 释放锁失败: %v", instanceID, err)
		} else {
			log.Printf("[%s] 已释放任务锁: %s", instanceID, taskID)
		}
	}()

	log.Printf("[%s] 开始执行任务: %s", instanceID, taskID)
	if err := workFn(taskCtx); err != nil {
		if errors.Is(taskCtx.Err(), context.Canceled) && !holder.released.Load() {
			log.Printf("[%s] 任务因锁丢失被中断: %s", instanceID, taskID)
			return &TaskResult{
				Success:  false,
				Message:  ErrLockLost.Error(),
				TaskID:   taskID,
				Executed: false,
				Instance: instanceID,
			}
		}
		log.Printf("[%s] 任务执行失败: %s, err: %v", instanceID, taskID, err)
		return &TaskResult{
			Success:  false,
			Message:  "任务执行失败: " + err.Error(),
			TaskID:   taskID,
			Executed: true,
			Instance: instanceID,
		}
	}

	if errors.Is(taskCtx.Err(), context.Canceled) && !holder.released.Load() {
		log.Printf("[%s] 任务因锁丢失被中断: %s", instanceID, taskID)
		return &TaskResult{
			Success:  false,
			Message:  ErrLockLost.Error(),
			TaskID:   taskID,
			Executed: false,
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

func (s *TaskService) ReleaseTask(ctx context.Context, taskID string) error {
	val, ok := s.holders.Load(taskID)
	if !ok {
		return ErrTaskNotActive
	}

	holder := val.(*lockHolder)
	if !holder.released.CompareAndSwap(false, true) {
		return ErrTaskNotActive
	}

	log.Printf("手动释放任务锁: %s", taskID)

	holder.cancelRefresh()
	holder.cancelTask()

	releaseCtx, releaseCancel := context.WithTimeout(ctx, 5*time.Second)
	defer releaseCancel()

	if err := holder.lock.Release(releaseCtx); err != nil {
		log.Printf("手动释放锁失败: %v", err)
		return err
	}

	s.holders.Delete(taskID)
	log.Printf("手动释放任务锁成功: %s", taskID)
	return nil
}

func (s *TaskService) ForceReleaseLock(ctx context.Context, taskID string) error {
	lockKey := "task:lock:" + taskID

	val, ok := s.holders.Load(taskID)
	if ok {
		holder := val.(*lockHolder)
		holder.released.Store(true)
		holder.cancelRefresh()
		holder.cancelTask()
		s.holders.Delete(taskID)
	}

	log.Printf("强制释放锁: %s (key: %s)", taskID, lockKey)
	return s.lockSvc.ForceRelease(ctx, lockKey)
}

func (s *TaskService) ListActiveTasks() []ActiveTaskInfo {
	var tasks []ActiveTaskInfo
	s.holders.Range(func(key, value any) bool {
		holder := value.(*lockHolder)
		tasks = append(tasks, ActiveTaskInfo{
			TaskID:     key.(string),
			LockKey:    holder.lock.Key(),
			AcquiredAt: holder.acquiredAt.Format(time.RFC3339),
		})
		return true
	})
	return tasks
}

func (s *TaskService) startLockRefresher(ctx context.Context, onLockLost context.CancelFunc, l *lock.Lock, interval time.Duration, ttl time.Duration, instanceID string) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			refreshCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			ok, err := l.Refresh(refreshCtx, ttl)
			cancel()

			if err != nil {
				log.Printf("[%s] 刷新锁异常，尝试重新续约: %v", instanceID, err)
				s.retryRefresh(ctx, l, ttl, instanceID, onLockLost)
				return
			}
			if !ok {
				log.Printf("[%s] 锁已失效，取消任务执行", instanceID)
				onLockLost()
				return
			}
			log.Printf("[%s] 锁已续约", instanceID)
		}
	}
}

func (s *TaskService) retryRefresh(ctx context.Context, l *lock.Lock, ttl time.Duration, instanceID string, onLockLost context.CancelFunc) {
	maxRetries := 3
	for i := 0; i < maxRetries; i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}

		time.Sleep(time.Duration(i+1) * time.Second)

		refreshCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		ok, err := l.Refresh(refreshCtx, ttl)
		cancel()

		if err != nil {
			log.Printf("[%s] 重试续约失败 (%d/%d): %v", instanceID, i+1, maxRetries, err)
			continue
		}
		if !ok {
			log.Printf("[%s] 重试续约发现锁已失效，取消任务执行", instanceID)
			onLockLost()
			return
		}

		log.Printf("[%s] 重试续约成功", instanceID)
		return
	}

	log.Printf("[%s] 重试续约全部失败，取消任务执行", instanceID)
	onLockLost()
}
