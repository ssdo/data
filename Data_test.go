package data_test

import (
	_ "github.com/go-sql-driver/mysql"
	"github.com/ssdo/data"
	ssdb "github.com/ssgo/db"
	"github.com/ssgo/log"
	"github.com/ssgo/redis"
	"testing"
)

type Info struct {
	Id   string
	Name string
}

type Info2 struct {
	Id      string
	Type    string
	Name    string
	Deleted bool
}

func TestData(t *testing.T) {
	logger := log.DefaultLogger
	db := ssdb.GetDB("test", nil)
	db.Exec("DROP TABLE IF EXISTS `d1`")
	db.Exec("CREATE TABLE `d1` (`id` CHAR(8) NOT NULL, `name` varchar(45) NOT NULL, `deleted` tinyint(1) NOT NULL, PRIMARY KEY (`id`));")

	data.Config.DB = db
	data.Init()
	d := data.NewData("d1", "id,name")

	id := d.Insert(Info{Name: "AAA"}, logger)
	if id == "" {
		t.Fatal("failed to insert")
	}

	info := Info{}
	d.Get(&info, id, logger)
	if info.Name != "AAA" {
		t.Fatal("failed to get")
	}

	id2 := d.Insert(Info{Name: "BBB"}, logger)
	if id2 == "" {
		t.Fatal("failed to insert2")
	}

	d.Get(&info, id2, logger)
	if info.Name != "BBB" {
		t.Fatal("failed to get2")
	}

	info.Name = "AAA"
	d.Update(info, logger)

	list := make([]Info, 0)
	d.List(logger).Where("`name`='AAA'").QueryAll(&list)
	if len(list) != 2 || (list[0].Id != id && list[0].Id != id2) || (list[1].Id != id && list[1].Id != id2) {
		t.Fatal("failed to list", list, id, id2)
	}

	d.Delete(id, logger)

	list = make([]Info, 0)
	d.List(logger).Where("`name`='AAA'").QueryAll(&list)
	if len(list) != 1 || list[0].Id != id2 {
		t.Fatal("failed to list2", list, id, id2)
	}
}

func TestDataByVersion(t *testing.T) {
	logger := log.DefaultLogger
	db := ssdb.GetDB("test", nil)
	db.Exec("DROP TABLE IF EXISTS `d2`")
	db.Exec("CREATE TABLE `d2` (`id` CHAR(8) NOT NULL, `type` varchar(45) NOT NULL, `name` varchar(45) NOT NULL, `deleted` tinyint(1) NOT NULL, `version` bigint unsigned NOT NULL, PRIMARY KEY (`id`));")

	rd := redis.GetRedis("test", nil)
	rd.DEL("_DATA_VERSION_d2")

	data.Config.DB = db
	data.Init()
	d := data.NewDataWithVersion("d2", "id,name")

	d.Insert(Info2{Type: "A", Name: "1"}, logger)
	d.Insert(Info2{Type: "B", Name: "2"}, logger)
	d.Insert(Info2{Type: "A", Name: "11"}, logger)
	deletedId := d.Insert(Info2{Type: "A", Name: "111"}, logger)

	list := make([]Info2, 0)
	version := d.List(logger).Where("`type`='A'").QueryByVersion(&list, 0, 0)
	if len(list) != 3 || version != 4 || list[0].Deleted == true {
		t.Fatal("failed to list1", version)
	}

	// 删除后获取到的数据中 deleted = 0
	d.Delete(deletedId, logger)
	list = make([]Info2, 0)
	version = d.List(logger).Where("`type`='A'").QueryByVersion(&list, version, 0)
	if len(list) != 1 || version != 5 || list[0].Id != deletedId || list[0].Deleted == false {
		t.Fatal("failed to list2", len(list), version, list[0].Id, deletedId, list[0].Deleted)
	}

	d.Insert(Info2{Type: "A", Name: "1111"}, logger)
	d.Insert(Info2{Type: "B", Name: "11111"}, logger)
	list = make([]Info2, 0)
	version = d.List(logger).Where("`type`='A'").QueryByVersion(&list, version, 0)
	if len(list) != 1 || version != 7 {
		t.Fatal("failed to list3", version)
	}

	listById := make(map[string]Info2)
	version = d.List(logger).Where("`type`='A'").QueryByVersion(&listById, 0, 0)
	if len(listById) != 4 || version != 7 || listById[deletedId].Deleted == false {
		t.Fatal("failed to list3", len(listById), version)
	}
}

func TestDataByPage(t *testing.T) {
	logger := log.DefaultLogger
	db := ssdb.GetDB("test", nil)
	db.Exec("DROP TABLE IF EXISTS `d1`")
	db.Exec("CREATE TABLE `d1` (`id` CHAR(8) NOT NULL, `name` varchar(45) NOT NULL, `deleted` tinyint(1) NOT NULL, PRIMARY KEY (`id`));")

	data.Config.DB = db
	data.Init()
	d := data.NewData("d1", "id,name")

	d.Insert(Info{Name: "1"}, logger)
	d.Insert(Info{Name: "2"}, logger)
	d.Insert(Info{Name: "3"}, logger)
	d.Insert(Info{Name: "4"}, logger)
	d.Insert(Info{Name: "5"}, logger)
	d.Insert(Info{Name: "6"}, logger)
	d.Insert(Info{Name: "7"}, logger)
	d.Insert(Info{Name: "8"}, logger)
	d.Insert(Info{Name: "9"}, logger)

	l := d.List(logger).OrderBy("Name")
	if l.Count() != 9 {
		t.Fatal("failed to count")
	}

	list := make([]Info, 0)
	if l.QueryAll(&list); len(list) != 9 {
		t.Fatal("failed to list all")
	}

	list = make([]Info, 0)
	if l.QueryByPage(&list, 0, 3); len(list) != 3 || list[2].Name != "3" {
		t.Fatal("failed to list QueryByPage", len(list), list)
	}

	if l.QueryByPage(&list, 3, 3); len(list) != 6 || list[5].Name != "6" {
		t.Fatal("failed to list ByPage2", len(list), list)
	}

}
