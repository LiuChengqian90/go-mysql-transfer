/*
 * Copyright 2020-2021 the original author(https://github.com/wj596)
 *
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */
package endpoint

import (
	"fmt"
	"strings"
	"sync"

	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/client"
	"github.com/siddontang/go-mysql/mysql"
	"go.mongodb.org/mongo-driver/mongo/options"

	"go-mysql-transfer/global"
	"go-mysql-transfer/model"
	"go-mysql-transfer/util/logs"
)

const (
	PrimaryKeyName  = "_privateKeyName"
	PrimaryKeyValue = "_privateKeyValue"
)

type MysqlEndpoint struct {
	options     *options.ClientOptions
	collections map[cKey]*client.Conn
	collLock    sync.RWMutex
	collwg      map[cKey]*sync.RWMutex
}

func newMysqlEndpoint() *MysqlEndpoint {
	r := &MysqlEndpoint{}
	r.collections = make(map[cKey]*client.Conn)
	r.collwg = make(map[cKey]*sync.RWMutex)
	return r
}

func (s *MysqlEndpoint) Connect() error {
	s.collLock.Lock()
	for _, rule := range global.RuleInsList() {
		conn, err := client.Connect(global.Cfg().MysqlAddr, global.Cfg().MysqlUsername, global.Cfg().MysqlPassword, rule.MysqlDatabase)
		if err != nil {
			logs.Errorf("connect to mysql error: %s", err.Error())
			return err
		}
		ccKey := s.collectionKey(rule.MysqlDatabase, rule.MysqlCollection)
		s.collections[ccKey] = conn
		s.collwg[ccKey] = &sync.RWMutex{}
	}
	s.collLock.Unlock()

	return nil
}

func (s *MysqlEndpoint) Ping() error {

	for key, collention := range s.collections {
		err := collention.Ping()
		if err != nil {
			logs.Errorf("ping %s mysql error: %s", key.collection, err.Error())
			return err
		}
	}

	return nil
}

func (s *MysqlEndpoint) isDuplicateKeyError(stack string) bool {
	return strings.Contains(stack, "ERROR 1062 (23000): Duplicate entry")
}

func (s *MysqlEndpoint) collectionKey(database, collection string) cKey {
	return cKey{
		database:   database,
		collection: collection,
	}
}

func (s *MysqlEndpoint) collection(key cKey) *client.Conn {
	s.collLock.RLock()
	c, ok := s.collections[key]
	s.collLock.RUnlock()
	if ok {
		return c
	}

	s.collLock.Lock()
	conn, err := client.Connect(global.Cfg().MysqlAddr, global.Cfg().MysqlUsername, global.Cfg().MysqlPassword, key.database)
	if err != nil {
		logs.Errorf("connect to mysql error: %s", err.Error())
		return nil
	}
	s.collections[key] = conn
	s.collwg[key] = &sync.RWMutex{}
	s.collLock.Unlock()
	return conn
}

func (s *MysqlEndpoint) Consume(from mysql.Position, rows []*model.RowRequest) error {
	num := s.Stock(rows)
	logs.Infof("处理完成 %d 条数据", num)
	return nil
}

func (s *MysqlEndpoint) Stock(rows []*model.RowRequest) int64 {
	var sum int64
	for _, row := range rows {
		rule, _ := global.RuleIns(row.RuleKey)
		if rule.TableColumnSize != len(row.Row) {
			logs.Warnf("%s schema mismatching", row.RuleKey)
			continue
		}

		kvm := rowMap(row, rule, false)
		id := primaryKey(row, rule)
		kvm[PrimaryKeyName] = primaryKeyName(row, rule)
		kvm[PrimaryKeyValue] = id
		ccKey := s.collectionKey(rule.MysqlDatabase, rule.MysqlCollection)
		s.collwg[ccKey].Lock()
		switch row.Action {
		case canal.InsertAction:
			rr, _ := s.buildInsertSql(ccKey, row, kvm)
			sum += rr
		case canal.UpdateAction:
			rr, _ := s.buildUpdateSql(ccKey, row, kvm)
			sum += rr
		case canal.DeleteAction:
			rr, _ := s.buildDeleteSql(ccKey, row, kvm)
			sum += rr
		}
		s.collwg[ccKey].Unlock()
	}

	return sum
}

func (s *MysqlEndpoint) Close() {
	for c, conn := range s.collections {
		logs.Infof("disconnecting %s", c.database+"."+c.collection)
		if conn != nil {
			conn.Close()
		}
	}
}

func (s *MysqlEndpoint) buildDeleteSql(ccKey cKey, row *model.RowRequest, kvm map[string]interface{}) (int64, error) {
	sql := fmt.Sprintf("delete from %s where %s = ?", ccKey.collection, kvm[PrimaryKeyName])
	collection := s.collection(ccKey)
	stmt, err := collection.Prepare(sql)
	if err != nil {
		logs.Errorf("buildDeleteSql prepare sql error: %s", err.Error())
		return 0, err
	}
	defer stmt.Close()

	result, err := stmt.Execute(kvm[PrimaryKeyValue])
	if err != nil {
		logs.Errorf("buildDeleteSql execute sql error: %s", err.Error())
	}

	return int64(result.AffectedRows), err
}

func (s *MysqlEndpoint) buildUpdateSql(ccKey cKey, row *model.RowRequest, kvm map[string]interface{}) (int64, error) {
	keys := make([]string, 0)
	values := make([]interface{}, 0)

	for k, v := range kvm {
		if k == PrimaryKeyName || k == PrimaryKeyValue || k == kvm[PrimaryKeyName] {
			continue
		}
		if v == nil || v == "" {
			continue
		}
		keys = append(keys, k+" = ?")
		values = append(values, v)
	}

	//todo ,filter primary key
	sql := fmt.Sprintf("update %s set %s where %s = ?", ccKey.collection, strings.Join(keys, ","), kvm[PrimaryKeyName])
	collection := s.collection(ccKey)
	stmt, err := collection.Prepare(sql)
	if err != nil {
		logs.Errorf("buildUpdateSql prepare sql error: %s", err.Error())
		return 0, err
	}
	defer stmt.Close()

	values = append(values, kvm[PrimaryKeyValue])
	result, err := stmt.Execute(values...)
	if err != nil {
		logs.Errorf("buildUpdateSql execute sql error: %s", err.Error())
	}

	return int64(result.AffectedRows), err
}

// 构造SQL
func (s *MysqlEndpoint) buildInsertSql(ccKey cKey, row *model.RowRequest, kvm map[string]interface{}) (int64, error) {
	keys := make([]string, 0)
	tmp_values := make([]string, 0)
	values := make([]interface{}, 0)

	for k, v := range kvm {
		if k == PrimaryKeyName || k == PrimaryKeyValue {
			continue
		}
		if v == nil || v == "" {
			continue
		}
		keys = append(keys, k)
		values = append(values, v)
		tmp_values = append(tmp_values, "?")
	}

	sql := fmt.Sprintf("insert into %s (%s) values (%s)", ccKey.collection, strings.Join(keys, ","), strings.Join(tmp_values, ","))
	collection := s.collection(ccKey)
	stmt, err := collection.Prepare(sql)
	if err != nil {
		logs.Errorf("buildInsertSql prepare sql error: %s", err.Error())
		return 0, err
	}
	defer stmt.Close()

	result, err := stmt.Execute(values...)
	if err != nil {
		logs.Errorf("buildInsertSql execute sql error: %s", err.Error())
		if s.isDuplicateKeyError(err.Error()) {
			s.buildDeleteSql(ccKey, row, kvm)
			result, err = stmt.Execute(values...)
			if err != nil {
				logs.Errorf("execute  retry sql error: %s", err.Error())
				return 0, err
			}
		} else {
			return 0, err
		}
	}

	defer result.Close()
	return int64(result.AffectedRows), err

}
