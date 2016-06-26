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

import (
	"database/sql"
	"errors"
	"os"
	"path/filepath"

	"github.com/coreos/rkt/pkg/lock"
	"github.com/hashicorp/errwrap"

	_ "github.com/cznic/ql/driver"
)

const (
	DbFilename = "ql.db"
)

type DB struct {
	dbdir string
	sqldb *sql.DB
}

func NewDB(dbdir string) (*DB, error) {
	if err := os.MkdirAll(dbdir, defaultPathPerm); err != nil {
		return nil, err
	}

	return &DB{dbdir: dbdir}, nil
}

func (db *DB) open() error {
	sqldb, err := sql.Open("ql", filepath.Join(db.dbdir, DbFilename))
	if err != nil {
		return err
	}
	db.sqldb = sqldb

	return nil
}

func (db *DB) close() error {
	if db.sqldb == nil {
		panic("cas db, Close called without an open sqldb")
	}

	if err := db.sqldb.Close(); err != nil {
		return errwrap.Wrap(errors.New("cas db close failed"), err)
	}
	db.sqldb = nil

	// Don't close the flock as it will be reused.
	return nil
}

func (db *DB) begin() (*sql.Tx, error) {
	return db.sqldb.Begin()
}

type txfunc func(*sql.Tx) error

// Do take an exclusive flock, opens the db, executes DoTx and then Closes the
// DB
// It's the unique function exported since it's the one that takes the
// exclusive flock.
func (db *DB) Do(fns ...txfunc) error {
	l, err := lock.ExclusiveLock(db.dbdir, lock.Dir)
	if err != nil {
		return err
	}
	defer l.Close()
	if err := db.open(); err != nil {
		return err
	}
	defer db.close()

	return db.doTx(fns...)
}

// DoTx executes the provided txfuncs inside a unique transaction.
// If one of the functions returns an error the whole transaction is rolled back.
func (db *DB) doTx(fns ...txfunc) error {
	tx, err := db.begin()
	if err != nil {
		return err
	}
	for _, fn := range fns {
		if err := fn(tx); err != nil {
			tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}
