package data

import (
	"fmt"
	ssdb "github.com/ssgo/db"
	"github.com/ssgo/log"
	"github.com/ssgo/redis"
	"github.com/ssgo/u"
	"reflect"
	"strings"
)

// 业务表
type Data struct {
	Table         string // 表名
	Id            string // ID字段名
	Deleted       string // 是否已删除字段名
	Version       string // 版本号字段名
	DefaultFields string // 查询时获取的字段
}

func NewData(tableField, defaultFields string) *Data {
	data := &Data{
		Table:         tableField,
		Id:            "id",
		Deleted:       "deleted",
		Version:       "",
		DefaultFields: parseFields(defaultFields),
	}
	NewDataBy(data)
	return data
}

func NewDataWithVersion(tableField, defaultFields string) *Data {
	data := NewData(tableField, defaultFields)
	data.Version = "version"
	NewDataBy(data)
	return data
}

func NewDataBy(data *Data) {
	data.DefaultFields = parseFields(data.DefaultFields)
	// 如果基于版本操作，字段中追加 deleted
	if data.Version != "" && data.Deleted != "" && data.DefaultFields != "" && strings.Contains(data.DefaultFields, "`"+data.Deleted+"`") {
		data.DefaultFields += ",`" + data.Deleted + "`"
	}
}

func parseFields(fields string) string {
	if strings.ContainsRune(fields, '(') {
		return fields
	}

	fieldArr := strings.Split(fields, ",")
	for i, field := range fieldArr {
		field = strings.TrimSpace(field)
		if fields[0] != '`' {
			fieldArr[i] = fmt.Sprint("`", field, "`")
		}
	}
	return strings.Join(fieldArr, ",")
}

func (data *Data) Get(obj interface{}, id string, logger *log.Logger) bool {
	db := Config.DB.CopyByLogger(logger)
	r := db.Query(fmt.Sprint("SELECT ", data.DefaultFields, " FROM `", data.Table, "` WHERE `", data.Deleted, "`=0 AND `", data.Id, "`=?"), id)
	return r.Error == nil && r.To(obj) == nil
}

func (data *Data) Insert(in interface{}, logger *log.Logger) string {
	db := Config.DB.CopyByLogger(logger)

	// 分配一个id并存储到数据库
	id := ""
	for i := 0; i < 10000; i++ {
		id = Config.IdMaker()
		// 找到一个不重复的Id
		if db.Query(fmt.Sprint("SELECT `", data.Id, "` FROM `", data.Table, "` WHERE `", data.Id, "`=?"), id).StringOnR1C1() == "" {
			break
		}
	}
	if id == "" {
		logger.Error("failed to create in id")
		return ""
	}

	// 将id字段存入data
	insertData := make(map[string]interface{})
	u.Convert(in, insertData)
	insertData[data.Id] = id
	insertData[data.Deleted] = 0

	if data.Version != "" {
		// 更新版本号
		rd := Config.Redis.CopyByLogger(logger)
		insertData[data.Version] = data.getVersion(rd, db)
	}

	if db.Insert(data.Table, insertData).Error == nil {
		return id
	}

	return ""
}

func (data *Data) Update(in interface{}, logger *log.Logger) bool {
	db := Config.DB.CopyByLogger(logger)

	updateData := make(map[string]interface{})
	u.Convert(in, updateData)
	id := u.String(updateData[data.Id])
	if id == "" {
		return false
	}

	delete(updateData, data.Id)

	// 更新版本号
	if data.Version != "" {
		rd := Config.Redis.CopyByLogger(logger)
		updateData[data.Version] = data.getVersion(rd, db)
	}

	return db.Update(data.Table, updateData, fmt.Sprint("`", data.Deleted, "`=0 AND `"+data.Id+"`=?"), id).Error == nil
}

func (data *Data) UpdateBy(logger *log.Logger, in interface{}, where string, args ...interface{}) bool {
	db := Config.DB.CopyByLogger(logger)

	if data.Version != "" {
		// 更新版本号
		rd := Config.Redis.CopyByLogger(logger)
		updateData := make(map[string]interface{})
		u.Convert(in, updateData)
		updateData[data.Version] = data.getVersion(rd, db)
		in = updateData
	}

	return db.Update(data.Table, in, fmt.Sprint("`", data.Deleted, "`=0 AND `"+where), args...).Error == nil
}

func (data *Data) Delete(id string, logger *log.Logger) bool {
	return data.Update(map[string]interface{}{data.Deleted: 1, data.Id: id}, logger)
	//return db.Exec(fmt.Sprint("DELETE FROM `", data.Table, "` WHERE ", "`", data.Id, "`=?"), id).Error == nil
}

func (data *Data) getVersion(redis *redis.Redis, db *ssdb.DB) int64 {
	version := redis.INCR("_DATA_VERSION_" + data.Table)
	if version > 1 {
		return version
	}

	// 第一次或者Redis数据重置后，从数据库查询最大版本号并存储
	r := db.Query(fmt.Sprint("SELECT MAX(`", data.Version, "`) FROM `", data.Table, "`"))
	maxVersion := r.IntOnR1C1()
	redis.SET("_DATA_VERSION_"+data.Table, maxVersion+1)
	return maxVersion + 1
}

// ------------- List -------------

type List struct {
	data         *Data
	db           *ssdb.DB
	logger       *log.Logger
	sql          string
	fields       string
	where        string
	orderBy      string
	args         []interface{}
	leftJoins    []string
	leftJoinArgs []interface{}
}

func (data *Data) List(logger *log.Logger) *List {
	return &List{
		data:         data,
		db:           Config.DB.CopyByLogger(logger),
		logger:       logger,
		sql:          "",
		fields:       data.DefaultFields,
		where:        "1",
		orderBy:      "",
		args:         []interface{}{},
		leftJoins:    []string{},
		leftJoinArgs: []interface{}{},
	}
}

func (list *List) parse(tag string) (string, []interface{}) {
	if list.sql != "" {
		return list.sql, list.args
	}

	leftJoinsStr := ""
	if len(list.leftJoins) > 0 {
		leftJoinsStr = " " + strings.Join(list.leftJoins, " ")
		list.args = append(list.leftJoinArgs, list.args...)
	}

	fields := list.fields
	if tag == "COUNT" {
		fields = "COUNT(*)"
	}

	deleted := ""
	if tag == "VERSION" {
		fields += ", `" + list.data.Deleted + "`"
	} else {
		deleted = "`" + list.data.Deleted + "`=0 AND "
	}

	return fmt.Sprint("SELECT ", fields, " FROM `", list.data.Table, "`", leftJoinsStr, " WHERE ", deleted, list.where, list.orderBy), list.args
}

func (list *List) Sql(sql string, args ...interface{}) *List {
	list.sql = sql
	list.args = args
	return list
}

func (list *List) Fields(fields string) *List {
	list.fields = parseFields(fields)
	return list
}

func (list *List) Where(where string, args ...interface{}) *List {
	list.where = where
	list.args = args
	return list
}

func (list *List) OrderBy(orderBy string) *List {
	list.orderBy = " ORDER BY " + orderBy
	return list
}

func (list *List) LeftJoin(join, on string, args ...interface{}) *List {
	list.leftJoins = append(list.leftJoins, fmt.Sprint("LEFT JOIN ", join, " ON ", on))
	list.leftJoinArgs = append(list.leftJoinArgs, args...)
	return list
}

func (list *List) QueryAll(out interface{}) bool {
	sql, args := list.parse("")
	r := list.db.Query(sql, args...)
	return r.Error == nil && r.To(out) == nil
}

func (list *List) Count() int {
	sql, args := list.parse("COUNT")
	return int(list.db.Query(sql, args...).IntOnR1C1())
}

func (list *List) QueryByPage(out interface{}, start, num int) bool {
	sql, args := list.parse("")
	args = append(args, start, num)
	r := list.db.Query(fmt.Sprint(sql, " LIMIT ?,?"), args...)
	return r.Error == nil && r.To(out) == nil
}

func (list *List) QueryByVersion(out interface{}, minVersion, maxVersion uint64) (newVersion uint64) {
	outValue := reflect.ValueOf(out)
	if outValue.Kind() != reflect.Ptr || (outValue.Elem().Kind() != reflect.Slice && outValue.Elem().Kind() != reflect.Map) {
		return minVersion
	}

	if maxVersion == 0 {
		rd := Config.Redis.CopyByLogger(list.logger)
		maxVersion = rd.GET("_DATA_VERSION_" + list.data.Table).Uint64()
		if maxVersion == 0 {
			db := Config.DB.CopyByLogger(list.logger)
			maxVersion = uint64(db.Query(fmt.Sprint("SELECT MAX(`", list.data.Version, "`) FROM `", list.data.Table, "`")).IntOnR1C1())
		}
	}
	if minVersion >= maxVersion {
		// 没有新数据
		return maxVersion
	}

	sql, args := list.parse("VERSION")
	args = append(args, minVersion+1, maxVersion)
	sql = strings.Replace(sql, "`"+list.data.Deleted+"`=0 AND ", "", 1)
	r := list.db.Query(fmt.Sprint(sql, " AND `", list.data.Version, "` BETWEEN ? AND ?"), args...)
	ok := false

	if r.Error == nil {
		if outValue.Elem().Kind() == reflect.Slice {
			ok = r.To(out) == nil
		} else {
			ok = r.ToKV(out) == nil
		}
	}

	if ok {
		return maxVersion
	}
	return minVersion
}
