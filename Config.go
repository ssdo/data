package data

import (
	"github.com/ssdo/utility"
	"github.com/ssgo/db"
	"github.com/ssgo/redis"
	"github.com/ssgo/u"
	"time"
)

var inited = false

var QueryLimiter *utility.Limiter  // 查询频次限制
var UpdateLimiter *utility.Limiter // 更新频次限制

var Config = struct {
	Redis               *redis.Redis                                                         // Redis连接池
	DB                  *db.DB                                                               // 数据库连接池
	QueryLimitDuration  time.Duration                                                        // 发送对象限制器时间间隔（用户ID）
	QueryLimitTimes     int                                                                  // 发送对象限制器时间单位内允许的次数（用户ID）
	UpdateLimitDuration time.Duration                                                        // 重复发送限制器时间间隔（用户ID+模版编号+参数）
	UpdateLimitTimes    int                                                                  // 重复发送限制器时间单位内允许的次数（用户ID+模版编号+参数）
	IdMaker             func() string                                                        // 用户编号生成器
}{
	Redis:               nil,
	DB:                  nil,
	QueryLimitDuration:  1 * time.Minute,
	QueryLimitTimes:     10000,
	UpdateLimitDuration: 1 * time.Minute,
	UpdateLimitTimes:    10000,
	IdMaker:             u.Id8,
}

func Init() {
	if inited {
		return
	}
	inited = true

	if Config.Redis == nil {
		Config.Redis = redis.GetRedis("user", nil)
	}

	if Config.DB == nil {
		Config.DB = db.GetDB("user", nil)
	}

	QueryLimiter = utility.NewLocalLimiter("Target", Config.QueryLimitDuration, Config.QueryLimitTimes)
	UpdateLimiter = utility.NewLocalLimiter("Update", Config.UpdateLimitDuration, Config.UpdateLimitTimes)
}
