## oci-store

This is an oci store implementation

It's made of a base store on top of which additional stores can be built (perhaps also an appc store). The oci store adds OCI specific functions (primarily blobs GC) to the base store.

Tha base store is a CAS store with additional features required to handle multiple requirements (access from multiple processes, access from multiple goroutine using the same store instance, indexing, additional blob data, key/value data):

* The store data can be concurrently accessed by multiple processes and multiple goroutines (like CoreOS rkt) (locally since it's not meant to be used as a multi host shared store) 
* Store data (blobs) is indexed to be easily listed and searched (for example to resolve partial digests)
* Every blob can be augmented with additional data (useful to save additional informations (like last accessed time etc...)
* Additional key/value data (the can optionally reference a blob) can be added (useful for oci references to descriptors and for the appc spec remote caching data)


The oci store use this store and implements OCI specific function:

* Garbage collection of unreferenced manifests, configs, layers
* A check for verifying that all the blobs needed by a name reference are available in the store (TODO)

Its base comes from the experience in the CoreOS rkt store and uses big part of it.

NOTE: This is a work in progress and before a initial release I'll squash and force push to avoid multiple unuseful commits.

### Test client

A simple test client has been created to test different features.

It can import and export a directory containing a oci image layout and list references


#### Example client

Build the example client

```
cd examples/client
cd oci-fetch
go install
```

Create an oci image layout dir:

```
go get github.com/sgotti/oci-fetch # temporary fixes to be merged in github.com/containers/oci-fetch
oci-fetch docker://registry-1.docker.io/library/nginx:latest nginx.oci
mkdir nginx; cd nginx
tar xvf ../nging.oci
```

Import from oci image layout ref latest with name `nginx` in the oci store
```
./client import --sref latest --dref nginx:latest nginx/
```

List oci store manifest references

```
./client list 
```

Export image nginx with rev latest to an oci image layout in out/ dir
```
./client export --sref nginx:latest --dref latest out/ 
```

Remove image nginx with rev latest from the store
```
./client rm --ref nginx:latest 
```

Garbage collect unreferenced blobs
```
./client gc
```

