package handler

import (
	"context"
	"errors"
	"net/http"
	"time"

	"distributed-lock-demo/task"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type TaskHandler struct {
	taskSvc    *task.TaskService
	instanceID string
}

type RunTaskRequest struct {
	TaskID   string `json:"task_id" binding:"required"`
	Duration int    `json:"duration_seconds"`
}

type ReleaseTaskRequest struct {
	TaskID string `json:"task_id" binding:"required"`
}

func NewTaskHandler(taskSvc *task.TaskService) *TaskHandler {
	return &TaskHandler{
		taskSvc:    taskSvc,
		instanceID: uuid.New().String()[:8],
	}
}

func (h *TaskHandler) RunTask(c *gin.Context) {
	var req RunTaskRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"message": "参数错误: " + err.Error(),
		})
		return
	}

	if req.Duration <= 0 {
		req.Duration = 5
	}

	result := h.taskSvc.ExecuteTask(c.Request.Context(), req.TaskID, h.instanceID, func(ctx context.Context) error {
		timer := time.NewTimer(time.Duration(req.Duration) * time.Second)
		defer timer.Stop()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}

		if req.TaskID == "fail-task" {
			return errors.New("模拟任务失败")
		}
		return nil
	})

	c.JSON(http.StatusOK, result)
}

func (h *TaskHandler) ReleaseTask(c *gin.Context) {
	var req ReleaseTaskRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"message": "参数错误: " + err.Error(),
		})
		return
	}

	if err := h.taskSvc.ReleaseTask(c.Request.Context(), req.TaskID); err != nil {
		if errors.Is(err, task.ErrTaskNotActive) {
			c.JSON(http.StatusNotFound, gin.H{
				"success": false,
				"message": "当前实例未持有该任务的锁，无法手动释放",
				"task_id": req.TaskID,
			})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"message": "手动释放锁失败: " + err.Error(),
			"task_id": req.TaskID,
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"message": "任务锁已手动释放",
		"task_id": req.TaskID,
	})
}

func (h *TaskHandler) ForceReleaseTask(c *gin.Context) {
	var req ReleaseTaskRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"message": "参数错误: " + err.Error(),
		})
		return
	}

	if err := h.taskSvc.ForceReleaseLock(c.Request.Context(), req.TaskID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"message": "强制释放锁失败: " + err.Error(),
			"task_id": req.TaskID,
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"message": "任务锁已强制释放",
		"task_id": req.TaskID,
	})
}

func (h *TaskHandler) ListActiveTasks(c *gin.Context) {
	tasks := h.taskSvc.ListActiveTasks()
	c.JSON(http.StatusOK, gin.H{
		"success":     true,
		"instance_id": h.instanceID,
		"active_tasks": tasks,
		"count":       len(tasks),
	})
}

func (h *TaskHandler) Health(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status":      "ok",
		"instance_id": h.instanceID,
	})
}
