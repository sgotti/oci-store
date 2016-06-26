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

package ocistore

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/coreos/rkt/pkg/lock"
	"github.com/sgotti/oci-store/common"
	"github.com/sgotti/oci-store/store"
)

const (
	defaultPathPerm = os.FileMode(0770 | os.ModeSetgid)
	defaultFilePerm = os.FileMode(0660)

	MediaTypeDescriptor               = `application/vnd.oci.descriptor.v1+json`
	MediaTypeManifest                 = `application/vnd.oci.image.manifest.v1+json`
	MediaTypeManifestList             = `application/vnd.oci.image.manifest.list.v1+json`
	MediaTypeImageSerialization       = `application/vnd.oci.image.serialization.rootfs.tar.gzip`
	MediaTypeImageSerializationConfig = `application/vnd.oci.image.serialization.config.v1+json`
)

// OCIStore is a store for oci image spec
// (https://github.com/opencontainers/image-spec/)
// It can handle blobs and refs to descriptors
type OCIStore struct {
	bs        *store.Store
	dir       string
	gcLockDir string
}

func NewOCIStore(storeDir string) (*OCIStore, error) {
	store, err := store.NewStore(storeDir)
	if err != nil {
		return nil, err
	}

	s := &OCIStore{
		bs:  store,
		dir: storeDir,
	}

	s.gcLockDir = filepath.Join(storeDir, "gclock")
	if err := os.MkdirAll(s.gcLockDir, defaultPathPerm); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *OCIStore) Close() error {
	return s.bs.Close()
}

// ReadBlob return an io.ReadCloser for the requested digest
func (s *OCIStore) ReadBlob(digest string) (io.ReadCloser, error) {
	gcLock, err := lock.SharedLock(s.gcLockDir, lock.Dir)
	if err != nil {
		return nil, fmt.Errorf("error getting gc lock: %v", err)
	}
	defer gcLock.Close()
	return s.bs.ReadBlob(digest)
}

// WriteBlob takes a blob encapsulated in an io.Reader, calculates its digest
// and then stores it in the blob store
// mediaType can be used to define a blob mediaType for later fetching of all
// the blobs my mediaType
func (s *OCIStore) WriteBlob(r io.ReadSeeker, mediaType string) (string, error) {
	gcLock, err := lock.SharedLock(s.gcLockDir, lock.Dir)
	if err != nil {
		return "", fmt.Errorf("error getting gc lock: %v", err)
	}
	defer gcLock.Close()
	return s.bs.WriteBlob(r, mediaType)
}

// ListBlobs returns a list of all blob digest of the given mediaType
func (s *OCIStore) ListBlobs(mediaType string) ([]string, error) {
	gcLock, err := lock.SharedLock(s.gcLockDir, lock.Dir)
	if err != nil {
		return nil, fmt.Errorf("error getting gc lock: %v", err)
	}
	defer gcLock.Close()
	return s.bs.ListBlobs(mediaType)
}

// DeleteBlob removes a blob.
// It firstly removes the blob infos from the db, then it tries to remove the
// non transactional data.
// If some error occurs removing some non transactional data a
// StoreRemovalError is returned.
func (s *OCIStore) DeleteBlob(digest string) error {
	gcLock, err := lock.SharedLock(s.gcLockDir, lock.Dir)
	if err != nil {
		return fmt.Errorf("error getting gc lock: %v", err)
	}
	defer gcLock.Close()
	return s.bs.DeleteBlob(digest)
}

// GetRef get a ref to a blob, returns the blob's digest
func (s *OCIStore) GetRef(name string) (*common.Descriptor, error) {
	gcLock, err := lock.SharedLock(s.gcLockDir, lock.Dir)
	if err != nil {
		return nil, fmt.Errorf("error getting gc lock: %v", err)
	}
	defer gcLock.Close()
	data, err := s.bs.GetData(name, MediaTypeDescriptor)
	if err != nil {
		return nil, fmt.Errorf("cannot get ref: %v", err)
	}
	var d *common.Descriptor
	if err := json.Unmarshal(data.Value, &d); err != nil {
		return nil, fmt.Errorf("failed to unmarshal descriptor: %v", err)
	}
	return d, nil
}

// SetRef defines a ref to a blob
func (s *OCIStore) SetRef(name string, d *common.Descriptor) error {
	gcLock, err := lock.SharedLock(s.gcLockDir, lock.Dir)
	if err != nil {
		return fmt.Errorf("error getting gc lock: %v", err)
	}
	defer gcLock.Close()

	data, err := json.Marshal(d)
	if err != nil {
		return fmt.Errorf("failed to marshal descriptor: %v", err)
	}
	return s.bs.SetData(name, data, MediaTypeDescriptor, "")
}

// DeleteRef defines a ref to a blob
func (s *OCIStore) DeleteRef(name string) error {
	gcLock, err := lock.SharedLock(s.gcLockDir, lock.Dir)
	if err != nil {
		return fmt.Errorf("error getting gc lock: %v", err)
	}
	defer gcLock.Close()
	return s.bs.DeleteData(name, MediaTypeDescriptor)
}

// ListAllRefs retrieves all the refs
func (s *OCIStore) ListAllRefs() ([]string, error) {
	gcLock, err := lock.SharedLock(s.gcLockDir, lock.Dir)
	if err != nil {
		return nil, fmt.Errorf("error getting gc lock: %v", err)
	}
	defer gcLock.Close()
	return s.bs.ListAllDataKeys(MediaTypeDescriptor)
}

// CheckAllDataAvailable verifies that, given a ref name, all the required blobs are available in the store
func (s *OCIStore) CheckAllDataAvailable(name string) (bool, error) {
	gcLock, err := lock.SharedLock(s.gcLockDir, lock.Dir)
	if err != nil {
		return false, fmt.Errorf("error getting gc lock: %v", err)
	}
	defer gcLock.Close()

	return s.checkAllDataAvailable(name)
}

func (s *OCIStore) checkAllDataAvailable(name string) (bool, error) {
	data, err := s.bs.GetData(name, MediaTypeDescriptor)
	if err != nil {
		return false, fmt.Errorf("cannot get ref: %v", err)
	}
	var d *common.Descriptor
	if err := json.Unmarshal(data.Value, &d); err != nil {
		return false, fmt.Errorf("failed to unmarshal descriptor: %v", err)
	}

	// Check manifest availability
	minfo, err := s.bs.GetBlobInfo(pathDigest(d.Digest))
	if err != nil {
		return false, err
	}
	if minfo == nil {
		return false, fmt.Errorf("manifest is missing")
	}
	// TODO(sgotti) also handle MediaTypeManifestList
	if minfo.MediaType != MediaTypeManifest {
		return false, fmt.Errorf("wrong referenced media type")
	}
	mblob, err := s.bs.ReadBlob(d.Digest)
	if err != nil {
		return false, err
	}
	defer mblob.Close()
	var m *common.Manifest
	if err := json.NewDecoder(mblob).Decode(&m); err != nil {
		return false, fmt.Errorf("failed to unmarshal manifest: %v", err)
	}

	// Check config availability
	cinfo, err := s.bs.GetBlobInfo(pathDigest(m.Config.Digest))
	if err != nil {
		return false, err
	}
	if cinfo == nil {
		return false, fmt.Errorf("config is missing")
	}

	// Check layers availability
	for _, l := range m.Layers {
		linfo, err := s.bs.GetBlobInfo(pathDigest(l.Digest))
		if err != nil {
			return false, err
		}
		if linfo == nil {
			return false, fmt.Errorf("layer with digest %s is missing", l.Digest)
		}
	}

	return true, nil
}

// GC removes unreferenced blobs in addition the the base store GC
func (s *OCIStore) GC() error {
	// GC will remove blobs not referenced (named references to manifests
	// and manifest references to layers and configs). If between the
	// unreferenced blobs calculation and the real blob removal some of
	// these blob becomes referenced, they will be removed
	// To avoid this we have to take an exclusive gc lock

	// If GC runs when another store users is uploading some blobs, but they aren't already referenced, they will be removed.
	// To avoid this, the upload should be done in this order:
	// SetRef
	// WriteBlob for Manifest
	// WriteBlob for config and Layers

	gcLock, err := lock.ExclusiveLock(s.gcLockDir, lock.Dir)
	if err != nil {
		return fmt.Errorf("error getting gc lock: %v", err)
	}
	defer gcLock.Close()

	// Remove unreferenced manifests
	// Get all manifests
	mdigests, err := s.bs.ListBlobs(MediaTypeManifest)
	if err != nil {
		return err
	}

	// Get all refs
	refs, err := s.bs.ListAllDataKeys(MediaTypeDescriptor)
	if err != nil {
		return err
	}

	refsmap := map[string]struct{}{}
	for _, r := range refs {
		data, err := s.bs.GetData(r, MediaTypeDescriptor)
		if err != nil {
			return fmt.Errorf("cannot get ref: %v", err)
		}
		var d *common.Descriptor
		if err := json.Unmarshal(data.Value, &d); err != nil {
			return fmt.Errorf("failed to unmarshal descriptor: %v", err)
		}
		refsmap[pathDigest(d.Digest)] = struct{}{}
	}
	// Calculate and remove unreferenced manifests
	oldmdigests := []string{}
	for _, mdigest := range mdigests {
		if _, ok := refsmap[mdigest]; !ok {
			oldmdigests = append(oldmdigests, mdigest)
		}
	}

	for _, oldmdigest := range oldmdigests {
		// Ignore delete errors
		s.bs.DeleteBlob(oldmdigest)
	}

	// Remove unreferenced layers and configs
	// Get all manifests
	mdigests, err = s.bs.ListBlobs(MediaTypeManifest)
	if err != nil {
		return err
	}
	// Get all configs and layers
	cdigests, err := s.bs.ListBlobs(MediaTypeImageSerializationConfig)
	if err != nil {
		return err
	}
	ldigests, err := s.bs.ListBlobs(MediaTypeImageSerialization)
	if err != nil {
		return err
	}
	digests := append(cdigests, ldigests...)

	refsmap = map[string]struct{}{}
	for _, mdigest := range mdigests {
		mr, err := s.bs.ReadBlob(mdigest)
		if err != nil {
			return err
		}
		defer mr.Close()
		var m common.Manifest
		if err := json.NewDecoder(mr).Decode(&m); err != nil {
			return err
		}

		refsmap[pathDigest(m.Config.Digest)] = struct{}{}
		for _, l := range m.Layers {
			refsmap[pathDigest(l.Digest)] = struct{}{}
		}
	}
	// Calculate and remove unreferenced configs and layers
	olddigests := []string{}
	for _, digest := range digests {
		if _, ok := refsmap[digest]; !ok {
			olddigests = append(olddigests, digest)
		}
	}

	for _, olddigest := range olddigests {
		// Ignore delete errors
		s.bs.DeleteBlob(olddigest)
	}

	// Call base store GC
	if err := s.bs.GC(); err != nil {
		return err
	}

	return nil
}

func pathDigest(digest string) string {
	return strings.Replace(digest, ":", "-", 1)
}
