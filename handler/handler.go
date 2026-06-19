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

func (h *TaskHandler) Health(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status":      "ok",
		"instance_id": h.instanceID,
	})
}
