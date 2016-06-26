// Copyright 2016 The Linux Foundation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package store

import "database/sql"

// Data is used to index data.
type Data struct {
	Key      string
	Value    []byte
	DataType string
	Digest   string
}

func newData(key string, value []byte) *Data {
	return &Data{
		Key:   key,
		Value: value,
	}
}

func dataRowScan(rows *sql.Rows, data *Data) error {
	// This ordering MUST match that in schema.go
	return rows.Scan(&data.Key, &data.Value, &data.DataType, &data.Digest)
}

// getDataWithPrefix returns all the datas with a key starting with the given prefix.
func getDatasWithPrefix(tx *sql.Tx, prefix, dataType string) ([]*Data, error) {
	var datas []*Data
	rows, err := tx.Query("SELECT * from data WHERE hasPrefix(key, $1) AND datatype == $2", prefix, dataType)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		data := &Data{}
		if err := dataRowScan(rows, data); err != nil {
			return nil, err
		}
		datas = append(datas, data)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return datas, err
}

// getData returns the data with the given digeskey.
func getData(tx *sql.Tx, key, dataType string) (*Data, error) {
	var b *Data
	rows, err := tx.Query("SELECT * from data WHERE key == $1 AND datatype == $2", key, dataType)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		b = &Data{}
		if err := dataRowScan(rows, b); err != nil {
			return nil, err
		}
		// No more than one row for key must exist.
		break
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return b, err
}

// listAllDatas returns all the datas for the required dataType
func listAllDataKeys(tx *sql.Tx, dataType string) ([]string, error) {
	var keys []string
	rows, err := tx.Query("SELECT key from data where datatype == $1", dataType)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return keys, err
}

// getAllDatas returns all the datas for the required dataType
func getAllDatas(tx *sql.Tx, dataType string) ([]*Data, error) {
	var datas []*Data
	rows, err := tx.Query("SELECT * from data where datatype == $1", dataType)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		data := &Data{}
		if err := dataRowScan(rows, data); err != nil {
			return nil, err
		}
		datas = append(datas, data)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return datas, err
}

// writeData adds or updates the provided data.
func writeData(tx *sql.Tx, data *Data) error {
	// ql doesn't have an INSERT OR UPDATE function so
	// it's faster to remove and reinsert the row
	_, err := tx.Exec("DELETE from data where key == $1", data.Key)
	if err != nil {
		return err
	}
	_, err = tx.Exec("INSERT into data (key, value, datatype, digest) VALUES ($1, $2, $3, $4)", data.Key, data.Value, data.DataType, data.Digest)
	if err != nil {
		return err
	}

	return nil
}

// removeData removes the data with the given key.
func removeData(tx *sql.Tx, key, dataType string) error {
	_, err := tx.Exec("DELETE from data where key == $1 AND datatype == $2", key, dataType)
	if err != nil {
		return err
	}
	return nil
}
