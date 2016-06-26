package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"text/tabwriter"

	"github.com/sgotti/oci-store/common"
	"github.com/sgotti/oci-store/ocistore"
	"github.com/spf13/cobra"
)

var storePath string

func main() {
	cmd := &cobra.Command{
		Use:   "test-client",
		Short: "A test client for the oci store",
	}

	stdout := log.New(os.Stdout, "", 0)
	stderr := log.New(os.Stderr, "", 0)

	cmd.AddCommand(newImportCmd(stdout, stderr))
	cmd.AddCommand(newExportCmd(stdout, stderr))
	cmd.AddCommand(newListCmd(stdout, stderr))
	cmd.AddCommand(newRmCmd(stdout, stderr))
	cmd.AddCommand(newGCCmd(stdout, stderr))

	flags := cmd.PersistentFlags()

	flags.StringVar(&storePath, "store-path", "/tmp/store", `the oci store path.`)

	if err := cmd.Execute(); err != nil {
		stderr.Println(err)
		os.Exit(1)
	}
}

type importCmd struct {
	stdout *log.Logger
	stderr *log.Logger
	sref   string
	dref   string
}

func newImportCmd(stdout, stderr *log.Logger) *cobra.Command {
	v := &importCmd{
		stdout: stdout,
		stderr: stderr,
	}

	cmd := &cobra.Command{
		Use:   "import --sref REF --dref REF dir...",
		Short: "import a ref from an image layout directory",
		Run:   v.Run,
	}

	flags := cmd.Flags()

	flags.StringVar(&v.sref, "sref", "", `the source ref.`)
	flags.StringVar(&v.dref, "dref", "", `the dest ref.`)

	return cmd
}

func (v *importCmd) Run(cmd *cobra.Command, args []string) {
	store, err := ocistore.NewOCIStore(storePath)
	if err != nil {
		fmt.Printf("NewOCIStore err: %v", err)
		return
	}

	if v.sref == "" {
		v.stderr.Fatalf("source ref must be provided")
	}
	if v.dref == "" {
		v.stderr.Fatalf("dest ref must be provided")
	}
	err = imp(store, args[0], v.sref, v.dref)

	if err != nil {
		v.stderr.Fatalf("err: %v", err)
	}
}

func pathDigest(digest string) string {
	return strings.Replace(digest, ":", "-", 1)
}

func doFile(path string, f func(io.Reader) error) error {
	r, err := os.Open(path)
	if err != nil {
		return err
	}
	defer r.Close()
	return f(r)

}

func imp(s *ocistore.OCIStore, src, sref, dref string) error {
	descPath := filepath.Join(src, "refs", sref)

	var desc common.Descriptor
	err := doFile(descPath, func(r io.Reader) error {
		return json.NewDecoder(r).Decode(&desc)
	})
	if err != nil {
		return err
	}

	fmt.Printf("desc: %v\n", desc)

	manifestPath := filepath.Join(src, "blobs", pathDigest(desc.Digest))
	manifestBlob, err := ioutil.ReadFile(manifestPath)
	if err != nil {
		return err
	}
	var m common.Manifest
	json.Unmarshal(manifestBlob, &m)

	fmt.Printf("m: %v\n", m)

	manifestDigest, err := s.WriteBlob(bytes.NewReader(manifestBlob), ocistore.MediaTypeManifest)
	if err != nil {
		return err
	}
	fmt.Printf("manifestDigest: %s\n", manifestDigest)

	configPath := filepath.Join(src, "blobs", pathDigest(m.Config.Digest))
	configBlob, err := ioutil.ReadFile(configPath)
	if err != nil {
		return err
	}

	fmt.Printf("config: %s\n", configBlob)
	configDigest, err := s.WriteBlob(bytes.NewReader(configBlob), ocistore.MediaTypeImageSerializationConfig)
	if err != nil {
		return err
	}
	fmt.Printf("configDigest: %s\n", configDigest)

	seenLayers := map[string]struct{}{}
	for _, l := range m.Layers {
		// Duplicate layers may appear in the manifest
		if _, ok := seenLayers[l.Digest]; ok {
			continue
		}
		seenLayers[l.Digest] = struct{}{}

		layerPath := filepath.Join(src, "blobs", pathDigest(l.Digest))
		lf, err := os.Open(layerPath)
		layerDigest, err := s.WriteBlob(lf, ocistore.MediaTypeImageSerialization)
		if err != nil {
			lf.Close()
			return err
		}
		lf.Close()
		fmt.Printf("layerDigest: %s\n", layerDigest)

	}

	err = s.SetRef(dref, &desc)
	if err != nil {
		return err
	}
	return nil
}

type exportCmd struct {
	stdout *log.Logger
	stderr *log.Logger
	sref   string
	dref   string
}

func newExportCmd(stdout, stderr *log.Logger) *cobra.Command {
	v := &exportCmd{
		stdout: stdout,
		stderr: stderr,
	}

	cmd := &cobra.Command{
		Use:   "export --sref REF --dref REF dir...",
		Short: "export a ref to an image layout directory",
		Run:   v.Run,
	}

	flags := cmd.Flags()

	flags.StringVar(&v.sref, "sref", "", `the source ref.`)
	flags.StringVar(&v.dref, "dref", "", `the dest ref.`)

	return cmd
}

func (v *exportCmd) Run(cmd *cobra.Command, args []string) {
	exitcode := 0

	store, err := ocistore.NewOCIStore(storePath)
	if err != nil {
		fmt.Printf("NewOCIStore err: %v", err)
		return
	}

	err = exp(store, args[0], v.sref, v.dref)

	if err != nil {
		fmt.Printf("err: %v", err)
		exitcode = 1
	}
	os.Exit(exitcode)
}

func writeFile(path string, r io.ReadCloser) error {
	defer r.Close()
	w, err := os.Create(path)
	if err != nil {
		return err
	}
	defer w.Close()
	_, err = io.Copy(w, r)
	return err
}

func exp(s *ocistore.OCIStore, dest, sref, dref string) error {
	if err := os.MkdirAll(filepath.Join(dest, "blobs"), 0755); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(dest, "refs"), 0755); err != nil {
		return err
	}

	desc, err := s.GetRef(sref)
	if err != nil {
		return err
	}

	manifestPath := filepath.Join(dest, "blobs", pathDigest(desc.Digest))
	mr, err := s.ReadBlob(desc.Digest)
	if err != nil {
		return err
	}
	manifestBlob, err := ioutil.ReadAll(mr)
	if err != nil {
		mr.Close()
		return err
	}
	mr.Close()

	if err := ioutil.WriteFile(manifestPath, manifestBlob, 0644); err != nil {
		return err
	}

	var m common.Manifest
	if err := json.Unmarshal(manifestBlob, &m); err != nil {
		return err
	}
	configPath := filepath.Join(dest, "blobs", pathDigest(m.Config.Digest))
	cr, err := s.ReadBlob(m.Config.Digest)
	if err != nil {
		return err
	}
	configBlob, err := ioutil.ReadAll(cr)
	if err != nil {
		cr.Close()
		return err
	}
	cr.Close()
	if err := ioutil.WriteFile(configPath, configBlob, 0644); err != nil {
		return err
	}

	for _, l := range m.Layers {
		layerPath := filepath.Join(dest, "blobs", pathDigest(l.Digest))
		lr, err := s.ReadBlob(l.Digest)
		if err != nil {
			return err
		}
		if err := writeFile(layerPath, lr); err != nil {
			return err
		}
	}

	descriptor := &common.Descriptor{
		MediaType: ocistore.MediaTypeManifest,
		Size:      int64(len(manifestBlob)),
		Digest:    desc.Digest,
	}
	descriptorBlob, err := json.Marshal(&descriptor)
	if err != nil {
		return err
	}
	descriptorPath := filepath.Join(dest, "refs", dref)
	if err := ioutil.WriteFile(descriptorPath, descriptorBlob, 0644); err != nil {
		return err
	}
	return nil
}

type listCmd struct {
	stdout *log.Logger
	stderr *log.Logger
}

func newListCmd(stdout, stderr *log.Logger) *cobra.Command {
	v := &listCmd{
		stdout: stdout,
		stderr: stderr,
	}

	cmd := &cobra.Command{
		Use:   "list",
		Short: "list images in the store",
		Run:   v.Run,
	}

	return cmd
}

func (v *listCmd) Run(cmd *cobra.Command, args []string) {
	store, err := ocistore.NewOCIStore(storePath)
	if err != nil {
		fmt.Printf("NewOCIStore err: %v", err)
		return
	}

	err = list(store)

	if err != nil {
		v.stderr.Fatalf("err: %v", err)
	}
}

func list(s *ocistore.OCIStore) error {
	refs, err := s.ListAllRefs()
	if err != nil {
		return err
	}

	w := new(tabwriter.Writer)

	descs := []*common.Descriptor{}
	for _, ref := range refs {
		desc, err := s.GetRef(ref)
		if err != nil {
			return err
		}
		descs = append(descs, desc)
	}
	// Format in tab-separated columns with a tab stop of 8.
	w.Init(os.Stdout, 0, 8, 0, '\t', 0)
	fmt.Fprint(w, "REF\tMANIFEST DIGEST\n")
	for i, ref := range refs {
		fmt.Fprintf(w, "%s\t%s\n", ref, descs[i].Digest)
	}
	w.Flush()

	return nil
}

type rmCmd struct {
	stdout *log.Logger
	stderr *log.Logger
	ref    string
}

func newRmCmd(stdout, stderr *log.Logger) *cobra.Command {
	v := &rmCmd{
		stdout: stdout,
		stderr: stderr,
	}

	cmd := &cobra.Command{
		Use:   "rm",
		Short: "rm images in the store",
		Run:   v.Run,
	}

	flags := cmd.Flags()

	flags.StringVar(&v.ref, "ref", "", `the ref.`)

	return cmd
}

func (v *rmCmd) Run(cmd *cobra.Command, args []string) {
	store, err := ocistore.NewOCIStore(storePath)
	if err != nil {
		fmt.Printf("NewOCIStore err: %v", err)
		return
	}

	err = rm(store, v.ref)

	if err != nil {
		v.stderr.Fatalf("err: %v", err)
	}
}

func rm(s *ocistore.OCIStore, ref string) error {
	return s.DeleteRef(ref)
}

type gcCmd struct {
	stdout *log.Logger
	stderr *log.Logger
}

func newGCCmd(stdout, stderr *log.Logger) *cobra.Command {
	v := &gcCmd{
		stdout: stdout,
		stderr: stderr,
	}

	cmd := &cobra.Command{
		Use:   "gc",
		Short: "gc images in the store",
		Run:   v.Run,
	}

	return cmd
}

func (v *gcCmd) Run(cmd *cobra.Command, args []string) {
	store, err := ocistore.NewOCIStore(storePath)
	if err != nil {
		fmt.Printf("NewOCIStore err: %v", err)
		return
	}

	err = gc(store)

	if err != nil {
		v.stderr.Fatalf("err: %v", err)
	}
}

func gc(s *ocistore.OCIStore) error {
	return s.GC()
}
