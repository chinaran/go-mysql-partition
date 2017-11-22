package part

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"testing"
	"time"
)

func TestPartition(t *testing.T) {
	// step 1: 连接数据库
	db := initSqlConn("xx", "xx", "127.0.0.1", "test", 3306)

	// step 2: 创建测试表
	sql := `
		CREATE TABLE IF NOT EXISTS test.partition_test (
			timestamp BIGINT(20) NOT NULL COMMENT '时间',
			data INT(11) NOT NULL COMMENT '数据',
			INDEX timestamp (timestamp)
		)
		COLLATE='utf8_general_ci'
		/*!50100 PARTITION BY RANGE (timestamp)
		( PARTITION p0 VALUES LESS THAN (0) ENGINE = InnoDB)  */;
	`
	_, err := db.Exec(sql)
	if err != nil {
		t.Fatalf("CREATE TABLE test.partition_test err: %v", err)
	}

	// step 3: 分区管理测试
	p := Partition{"test", "partition_test", DAY_SEC, DAY_SEC * 365, 1, 5, 0}
	if p.IsNeedAddPartition(time.Now().Unix()) {
		p.HandlePartitionByDay(db, time.Now().Unix(), false)
	}
	t.Log(p.GetCurrentPartition(db))

	// step 4: 删除测试表
	sql = "drop table test.partition_test"
	_, err = db.Exec(sql)
	if err != nil {
		t.Fatalf("drop table test.partition_test err: %v", err)
	}
}

func initSqlConn(user, pass, host, dbName string, port int) (db *sql.DB) {
	sqlConn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8",
		user, pass, host, port, dbName)

	db, err := sql.Open("mysql", sqlConn)
	if err != nil {
		log.Fatal(err.Error())
	}

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	err = db.Ping()
	if err != nil {
		log.Fatal(err.Error())
	}

	return db
}
