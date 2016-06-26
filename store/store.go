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
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/coreos/rkt/pkg/lock"

	"github.com/peterbourgon/diskv"
)

const (
	defaultPathPerm = os.FileMode(0770 | os.ModeSetgid)
	defaultFilePerm = os.FileMode(0660)

	// TODO(sgotti) Handle multiple digest algorithms (sha384, sha512)
	digestPrefix = "sha256-"
	lenHash      = sha256.Size // raw byte size
	lenHexHash   = lenHash     // in hex characters
	lenDigest    = len(digestPrefix) + lenHexHash
	minLenDigest = len(digestPrefix) + 2 // at least sha256-aa

	// how many backups to keep when migrating to new db version
	backupsNumber = 5
)

var (
	ErrDigestNotFound  = errors.New("no digest found")
	ErrMultipleDigests = errors.New("found multiple digests")
	// ErrRemovingBlobContents defines an error removing non transactional (diskv) blob contents
	// When this happen there's the possibility that the store is left in an
	// unclean state (for example with some stale files). This will be fixed by store GC
	ErrRemovingBlobContents = errors.New("failed to remove blob disk contents")
)

// Store encapsulates a content-addressable-storage for storing blobs on disk.
// It's an indexed CAS with the ability to augment a blob with additional data
// and a key value store (with optional references to a blob)
type Store struct {
	dir       string
	blobStore *diskv.Diskv
	db        *DB
	// storeLock is a lock on the whole store. It's used for store migration. If
	// a previous version of rkt is using the store and in the meantime a
	// new version is installed and executed it will try migrate the store
	// during NewStore. This means that the previous running rkt will fail
	// or behave badly after the migration as it's expecting another db format.
	// For this reason, before executing migration, an exclusive lock must
	// be taken on the whole store.
	storeLock   *lock.FileLock
	blobLockDir string
}

func NewStore(storeDir string) (*Store, error) {
	// We need to allow the store's setgid bits (if any) to propagate, so
	// disable umask
	um := syscall.Umask(0)
	defer syscall.Umask(um)

	blobStore := diskv.New(diskv.Options{
		PathPerm:  defaultPathPerm,
		FilePerm:  defaultFilePerm,
		BasePath:  filepath.Join(storeDir, "blobs"),
		Transform: blockTransform,
	})

	s := &Store{
		dir:       storeDir,
		blobStore: blobStore,
	}

	s.blobLockDir = filepath.Join(storeDir, "bloblocks")
	if err := os.MkdirAll(s.blobLockDir, defaultPathPerm); err != nil {
		return nil, err
	}

	// Take a shared store lock
	var err error
	s.storeLock, err = lock.NewLock(storeDir, lock.Dir)
	if err != nil {
		return nil, err
	}
	if err := s.storeLock.SharedLock(); err != nil {
		return nil, err
	}

	db, err := NewDB(filepath.Join(storeDir, "db"))
	if err != nil {
		return nil, err
	}
	s.db = db

	needsMigrate := false
	fn := func(tx *sql.Tx) error {
		var err error
		ok, err := dbIsPopulated(tx)
		if err != nil {
			return err
		}
		// populate the db
		if !ok {
			for _, stmt := range dbCreateStmts {
				_, err = tx.Exec(stmt)
				if err != nil {
					return fmt.Errorf("stmt: %s err: %v", stmt, err)
				}
			}
			return nil
		}
		// if db is populated check its version
		version, err := getDBVersion(tx)
		if err != nil {
			return err
		}
		if version < dbVersion {
			needsMigrate = true
		}
		if version > dbVersion {
			return fmt.Errorf("current store db version: %d (greater than the current rkt expected version: %d)", version, dbVersion)
		}
		return nil
	}
	if err = db.Do(fn); err != nil {
		return nil, err
	}

	// migration is done in another transaction as it must take an exclusive
	// store lock. If, in the meantime, another process has already done the
	// migration, between the previous db version check and the below
	// migration code, the migration will do nothing as it'll start
	// migration from the current version.
	if needsMigrate {
		// Take an exclusive store lock
		err := s.storeLock.ExclusiveLock()
		if err != nil {
			return nil, err
		}
		if err := s.backupDB(); err != nil {
			return nil, err
		}
		fn := func(tx *sql.Tx) error {
			return migrate(tx, dbVersion)
		}
		if err = db.Do(fn); err != nil {
			return nil, err
		}
	}

	return s, nil
}

// Close closes a Store opened with NewStore().
func (s *Store) Close() error {
	return s.storeLock.Close()
}

// backupDB backs up current database.
func (s *Store) backupDB() error {
	backupsDir := filepath.Join(s.dir, "db-backups")
	return createBackup(s.db.dbdir, backupsDir, backupsNumber)
}

// TmpFile returns an *os.File local to the same filesystem as the Store, or
// any error encountered
func (s *Store) TmpFile() (*os.File, error) {
	dir, err := s.TmpDir()
	if err != nil {
		return nil, err
	}
	return ioutil.TempFile(dir, "")
}

// TmpDir creates and returns dir local to the same filesystem as the Store,
// or any error encountered
func (s *Store) TmpDir() (string, error) {
	dir := filepath.Join(s.dir, "tmp")
	if err := os.MkdirAll(dir, defaultPathPerm); err != nil {
		return "", err
	}
	return dir, nil
}

// ResolveDigest resolves a partial digest (of format `sha256-0c45e8c0ab2`) to a full
// digest by considering the digest prefix and using the store for resolution.
func (s *Store) ResolveDigest(inDigest string) (string, error) {
	digest, err := s.parseDigest(inDigest)
	if err != nil {
		return "", fmt.Errorf("cannot parse digest %s: %v", inDigest, err)
	}

	var blobs []*Blob
	err = s.db.Do(func(tx *sql.Tx) error {
		var err error
		blobs, err = getBlobsWithPrefix(tx, digest)
		return err
	})
	if err != nil {
		return "", fmt.Errorf("error retrieving blob infos: %v", err)
	}

	blobCount := len(blobs)
	if blobCount == 0 {
		return "", ErrDigestNotFound
	}
	if blobCount != 1 {
		return "", ErrMultipleDigests
	}
	return blobs[0].Digest, nil
}

// ReadBlob return an io.ReadCloser for the requested digest
func (s *Store) ReadBlob(digest string) (io.ReadCloser, error) {
	digest, err := s.ResolveDigest(digest)
	if err != nil {
		return nil, fmt.Errorf("error resolving digest: %v", err)
	}
	blobLock, err := lock.SharedKeyLock(s.blobLockDir, digest)
	if err != nil {
		return nil, fmt.Errorf("error locking blob: %v", err)
	}
	defer blobLock.Close()

	var blob *Blob
	err = s.db.Do(func(tx *sql.Tx) error {
		var err error
		blob, err = getBlob(tx, digest)
		if err != nil {
			return fmt.Errorf("error getting blob: %v", err)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("cannot get blob info for %q from db: %v", digest, err)
	}
	if blob == nil {
		return nil, fmt.Errorf("blob %q does not exists")
	}

	return s.blobStore.ReadStream(digest, false)
}

// WriteBlob takes a blob encapsulated in an io.Reader, calculates its digest
// and then stores it in the blob store
// mediaType can be used to define a blob mediaType for later fetching of all
// the blobs my mediaType
func (s *Store) WriteBlob(r io.ReadSeeker, mediaType string) (string, error) {
	// We need to allow the store's setgid bits (if any) to propagate, so
	// disable umask
	um := syscall.Umask(0)
	defer syscall.Umask(um)

	h := sha256.New()
	tr := io.TeeReader(r, h)
	fh, err := s.TmpFile()
	if err != nil {
		return "", fmt.Errorf("error creating temporary file: %v", err)
	}
	defer os.Remove(fh.Name())
	_, err = io.Copy(fh, tr)
	if err != nil {
		return "", fmt.Errorf("error copying blob: %v", err)
	}

	digest := s.hashToDigest(h)

	blobLock, err := lock.ExclusiveKeyLock(s.blobLockDir, digest)
	if err != nil {
		return "", fmt.Errorf("error locking blob: %v", err)
	}
	defer blobLock.Close()

	// TODO(sgotti) Avoid importing if the blob is already in the store?
	if err := s.blobStore.Import(fh.Name(), digest, true); err != nil {
		return "", fmt.Errorf("error importing blob: %v", err)
	}

	// Save blob info
	if err := s.db.Do(func(tx *sql.Tx) error {
		blob := &Blob{
			Digest:    digest,
			MediaType: mediaType,
		}
		return writeBlob(tx, blob)
	}); err != nil {
		return "", fmt.Errorf("error writing blob info: %v", err)
	}

	return digest, nil
}

// GetBlobInfo return blob info for the provided blob digest
// If the blob doesn't exists the return value will be nil
func (s *Store) GetBlobInfo(digest string) (*Blob, error) {
	var blob *Blob
	err := s.db.Do(func(tx *sql.Tx) error {
		var err error
		blob, err = getBlob(tx, digest)
		if err != nil {
			return fmt.Errorf("error getting blob: %v", err)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("cannot get blob info from db: %v", err)
	}
	return blob, nil
}

// ListBlobs returns a list of all blob digest of the given mediaType
func (s *Store) ListBlobs(mediaType string) ([]string, error) {
	var blobs []*Blob
	err := s.db.Do(func(tx *sql.Tx) error {
		var err error
		blobs, err = getAllBlobsByMediaType(tx, mediaType)
		if err != nil {
			return fmt.Errorf("error getting blob: %v", err)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("cannot get blob list from db: %v", err)
	}

	digests := make([]string, len(blobs))
	for i, b := range blobs {
		digests[i] = b.Digest
	}
	return digests, nil
}

// DeleteBlob removes a blob.
// It firstly removes the blob infos from the db, then it tries to remove the
// non transactional data.
// If some error occurs removing some non transactional data a
// ErrRemovingBlobContents is returned. The user may ignore this error since a
// followin GC will try to remove it and the blob won't be available with the
// other functions.
func (s *Store) DeleteBlob(digest string) error {
	digest, err := s.ResolveDigest(digest)
	if err != nil {
		return fmt.Errorf("error resolving digest: %v", err)
	}
	blobLock, err := lock.ExclusiveKeyLock(s.blobLockDir, digest)
	if err != nil {
		return fmt.Errorf("error locking blob: %v", err)
	}
	defer blobLock.Close()

	err = s.db.Do(func(tx *sql.Tx) error {
		if err := removeBlob(tx, digest); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("cannot remove blob with digest %s from db: %v", digest, err)
	}

	// Then remove non transactional data (diskv store)
	if err := s.blobStore.Erase(digest); err != nil {
		return ErrRemovingBlobContents
	}
	return nil
}

// SetBlobData set additional data of dataType to a blob
// It'll be removed when the blob is removed
func (s *Store) SetBlobData(digest string, data []byte, dataType string) error {
	//TODO(sgotti) implement
	return nil
}

// GetBlobData gets additional data of dataType from a blob
func (s *Store) GetBlobData(digest, dataType string) (*BlobData, error) {
	//TODO(sgotti) implement
	return nil, nil
}

// DelBlobData removes additional data of dataType from a blob
func (s *Store) DeleteBlobData(digest, dataType string) error {
	//TODO(sgotti) implement
	return nil
}

// SetData adds additional data of dataType. The data has an unique key to
// easily list and search by key
// If digest is not nil it can reference a blob digest and it'll be removed
// when the blob is removed.
// This can be useful to add custom data to the store. For example:
// * oci references where key is the ref name and data is an oci descriptor
// * remote caching information for an appc ACI store where key is the download
// URL, data contains additional data (like http caching information) and if
// references a digest to an ACI blob.
// TODO(sgotti) add a version field and a ErrConcurrentUpdate as a facility for
// clients to do optimistic locking?
// it should be useful to handle data migration without chaning the dataType
func (s *Store) SetData(key string, value []byte, dataType, digest string) error {
	if err := s.db.Do(func(tx *sql.Tx) error {
		if digest != "" {
			// Check blob existance
			blob, err := getBlob(tx, digest)
			if err != nil {
				return fmt.Errorf("error getting blob: %v", err)
			}
			if blob == nil {
				return fmt.Errorf("cannot find blob with digest: %s", digest)
			}
		}
		data := &Data{
			Key:      key,
			Value:    value,
			DataType: dataType,
			Digest:   digest,
		}
		return writeData(tx, data)
	}); err != nil {
		return fmt.Errorf("error writing data: %v", err)
	}
	return nil
}

// GetData gets additional data from a blob
func (s *Store) GetData(key, dataType string) (*Data, error) {
	var data *Data
	err := s.db.Do(func(tx *sql.Tx) error {
		var err error
		data, err = getData(tx, key, dataType)
		if err != nil {
			return fmt.Errorf("error getting data: %v", err)
		}
		if data == nil {
			return fmt.Errorf("data does not exists")
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("cannot get data for key %s and dataType %s: %v", key, dataType, err)
	}
	return data, nil
}

// DeleteData removes additional data with the specified key and dataType
func (s *Store) DeleteData(key, dataType string) error {
	if err := s.db.Do(func(tx *sql.Tx) error {
		return removeData(tx, key, dataType)
	}); err != nil {
		return fmt.Errorf("error removing data for key %s and dataType %s: %v", key, dataType, err)
	}
	return nil
}

// ListData returns a list of all data for the given dataType
func (s *Store) ListAllDataKeys(dataType string) ([]string, error) {
	var keys []string
	if err := s.db.Do(func(tx *sql.Tx) error {
		var err error
		keys, err = listAllDataKeys(tx, dataType)
		return err
	}); err != nil {
		return nil, fmt.Errorf("error getting all keys for dataType %s: %v", dataType, err)
	}
	return keys, nil
}

// GC garbage collects stale blobs contents (blobs not appering in the db)
// (this can happend if they aren't remove for whatever reason) and remove
// unused keylocks
func (s *Store) GC() error {
	//TODO(sgotti) implement stale blob removal
	return lock.CleanKeyLocks(s.blobLockDir)
}

func (s *Store) hashToDigest(h hash.Hash) string {
	sum := h.Sum(nil)
	return fmt.Sprintf("%s%x", digestPrefix, sum)
}

func (s *Store) parseDigest(digest string) (string, error) {
	// Replace possible colon with hypen
	digest = strings.Replace(digest, ":", "-", 1)
	if !strings.HasPrefix(digest, digestPrefix) {
		return "", fmt.Errorf("wrong digest prefix")
	}
	if len(digest) < minLenDigest {
		return "", fmt.Errorf("digest too short")
	}
	return digest, nil
}
