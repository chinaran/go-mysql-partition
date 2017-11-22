package part

import (
	"database/sql"
	"fmt"
	"log"
)

const (
	DAY_SEC = (24 * 60 * 60)
	BJ_SEC  = (8 * 60 * 60) // 北京时间多8个小时（时区影响）
)

type Partition struct {
	DbName    string // 数据库名
	TableName string // 数据表名
	PartTv    int64  // 分区间隔
	ReserveTv int64  // 分区保留时间
	CondUnit  int64  // 时间戳单位(秒: 1，毫秒：1000，以此类推)
	NewNum    int64  // 要新创建分区的个数
	MaxTime   int64  // 当前分区的最大时间（不包括多创建的分区）
}

func init() {
	log.SetFlags(log.LstdFlags)
}

func (p *Partition) addPartition(db *sql.DB, no, cond int64) error {
	sql := fmt.Sprintf("alter table %s.%s add partition (partition p%d values less than(%d) ENGINE = InnoDB)",
		p.DbName, p.TableName, no, cond*p.CondUnit)

	_, err := db.Exec(sql)
	if err != nil {
		log.Printf("添加分区 [%s] fail: [%v]", sql, err)
		return err
	}
	log.Printf("添加分区 [%s] ok", sql)
	return nil
}

func (p *Partition) delPartition(db *sql.DB, no int64) error {
	sql := fmt.Sprintf("alter table %s.%s drop partition p%d",
		p.DbName, p.TableName, no)

	_, err := db.Exec(sql)
	if err != nil {
		log.Printf("删除分区 [%s] fail: [%v]", sql, err)
		return err
	}
	log.Printf("删除分区 [%s] ok", sql)
	return nil
}

func (p *Partition) IsNeedAddPartition(now int64) bool {
	return (now >= p.MaxTime)
}

func (p *Partition) GetCurrentPartition(db *sql.DB) (map[int64]int64, error) {
	sql := fmt.Sprintf("SELECT substring(PARTITION_NAME, 2), PARTITION_DESCRIPTION FROM INFORMATION_SCHEMA.PARTITIONS WHERE TABLE_SCHEMA = '%s' and TABLE_NAME = '%s'", p.DbName, p.TableName)
	rows, err := db.Query(sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var no, cond int64
	m := make(map[int64]int64) // TODO: 用 set 更好，but go 没有 set

	for rows.Next() {
		err = rows.Scan(&no, &cond)
		if err != nil {
			return nil, err
		}
		m[no] = cond
	}
	return m, nil
}

func (p *Partition) HandlePartitionByDay(db *sql.DB, now int64, addHistoryPartition bool) error {
	// step 1: 获取当前的分区
	m, err := p.GetCurrentPartition(db)
	if err != nil {
		return err
	}

	// step 2: 创建新分区
	var i, firstStart, start, end int64
	now += BJ_SEC
	firstStart = (now-p.ReserveTv)/p.PartTv - 1
	start = now/p.PartTv - p.NewNum
	end = now/p.PartTv + p.NewNum
	p.MaxTime = now/p.PartTv*p.PartTv + p.PartTv - BJ_SEC

	if addHistoryPartition {
		i = firstStart
	} else {
		i = start
	}

	for ; i <= end; i++ {
		if _, ok := m[i]; !ok {
			p.addPartition(db, i, i*p.PartTv+p.PartTv-BJ_SEC)
		}
	}

	// step 3: 删除老分区
	for k, _ := range m {
		if k < firstStart {
			p.delPartition(db, k)
		}
	}

	return nil
}
