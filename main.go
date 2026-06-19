package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"distributed-lock-demo/handler"
	"distributed-lock-demo/lock"
	"distributed-lock-demo/task"

	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
)

func main() {
	redisAddr := getEnv("REDIS_ADDR", "localhost:6379")
	redisPassword := getEnv("REDIS_PASSWORD", "")
	redisDB := 0
	httpPort := getEnv("HTTP_PORT", ":8080")

	rdb := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: redisPassword,
		DB:       redisDB,
	})

	ctx := context.Background()
	if _, err := rdb.Ping(ctx).Result(); err != nil {
		log.Fatalf("连接Redis失败: %v", err)
	}
	log.Println("Redis连接成功:", redisAddr)

	lockSvc := lock.NewDistributedLock(rdb)
	taskSvc := task.NewTaskService(lockSvc)
	taskHandler := handler.NewTaskHandler(taskSvc)

	r := gin.Default()
	r.GET("/health", taskHandler.Health)
	r.POST("/task/run", taskHandler.RunTask)
	r.POST("/task/release", taskHandler.ReleaseTask)
	r.POST("/task/force-release", taskHandler.ForceReleaseTask)
	r.GET("/task/active", taskHandler.ListActiveTasks)

	go func() {
		log.Println("HTTP服务启动,监听端口:", httpPort)
		if err := r.Run(httpPort); err != nil {
			log.Fatalf("HTTP服务启动失败: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("服务正在关闭...")
	rdb.Close()
	log.Println("服务已退出")
}

func getEnv(key, defaultValue string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultValue
}
