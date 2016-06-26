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

// Blob is used to index a Blob.
type Blob struct {
	Digest    string
	MediaType string
}

func newBlob(digest string, mediaType string) *Blob {
	return &Blob{
		Digest:    digest,
		MediaType: mediaType,
	}
}

func blobRowScan(rows *sql.Rows, blob *Blob) error {
	// This ordering MUST match that in schema.go
	return rows.Scan(&blob.Digest, &blob.MediaType)
}

// getBlobWithPrefix returns all the blobs with a digest starting with the given prefix.
func getBlobsWithPrefix(tx *sql.Tx, prefix string) ([]*Blob, error) {
	var blobs []*Blob
	rows, err := tx.Query("SELECT * from blobinfo WHERE hasPrefix(digest, $1)", prefix)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		blob := &Blob{}
		if err := blobRowScan(rows, blob); err != nil {
			return nil, err
		}
		blobs = append(blobs, blob)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return blobs, err
}

// getBlob returns the blob with the given digesdigest.
func getBlob(tx *sql.Tx, digest string) (*Blob, error) {
	var b *Blob
	rows, err := tx.Query("SELECT * from blobinfo WHERE digest == $1", digest)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		b = &Blob{}
		if err := blobRowScan(rows, b); err != nil {
			return nil, err
		}
		// No more than one row for digest must exist.
		break
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return b, err
}

// getAllBlobs returns all the blobs for the required mediaType
func getAllBlobsByMediaType(tx *sql.Tx, mediaType string) ([]*Blob, error) {
	var blobs []*Blob
	rows, err := tx.Query("SELECT * from blobinfo where mediatype == $1", mediaType)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		blob := &Blob{}
		if err := blobRowScan(rows, blob); err != nil {
			return nil, err
		}
		blobs = append(blobs, blob)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return blobs, err
}

// writeBlob adds or updates the provided blob.
func writeBlob(tx *sql.Tx, blob *Blob) error {
	// ql doesn't have an INSERT OR UPDATE function so
	// it's faster to remove and reinsert the row
	_, err := tx.Exec("DELETE from blobinfo where digest == $1", blob.Digest)
	if err != nil {
		return err
	}
	_, err = tx.Exec("INSERT into blobinfo (digest, mediatype) VALUES ($1, $2)", blob.Digest, blob.MediaType)
	if err != nil {
		return err
	}

	return nil
}

// removeBlob removes the blob with the given digest.
func removeBlob(tx *sql.Tx, digest string) error {
	_, err := tx.Exec("DELETE from blobinfo where digest == $1", digest)
	if err != nil {
		return err
	}
	return nil
}
