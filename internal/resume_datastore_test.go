package galexie

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/support/datastore"
)

var testSchema = datastore.DataStoreSchema{
	LedgersPerFile:    1,
	FilesPerPartition: 10,
}

// populateLedgers writes minimal ledger files for the given sequence range.
func populateLedgers(t *testing.T, ctx context.Context, ds datastore.DataStore, schema datastore.DataStoreSchema, from, to uint32) {
	t.Helper()
	for seq := from; seq <= to; seq++ {
		key := schema.GetObjectKeyFromSequenceNumber(seq)
		err := ds.PutFile(ctx, key, bytes.NewReader([]byte("x")), nil)
		require.NoError(t, err, "PutFile for ledger %d (key=%s)", seq, key)
	}
}

func TestFindLatestLedgerSequenceGCS(t *testing.T) {
	ctx := context.Background()

	server, err := fakestorage.NewServerWithOptions(fakestorage.Options{
		Scheme:     "http",
		Host:       "127.0.0.1",
		Port:       0,
		PublicHost: "127.0.0.1",
	})
	require.NoError(t, err)
	defer server.Stop()

	t.Setenv("STORAGE_EMULATOR_HOST", server.URL())

	cases := []struct {
		name       string
		bucketPath string
	}{
		{"no-prefix-no-trailing-slash", "gcs-nopfx-noslash"},
		{"no-prefix-trailing-slash", "gcs-nopfx-slash/"},
		{"prefix-no-trailing-slash", "gcs-pfx-noslash/sub"},
		{"prefix-trailing-slash", "gcs-pfx-slash/sub/"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bucket := strings.TrimRight(tc.bucketPath, "/")
			if i := strings.Index(bucket, "/"); i >= 0 {
				bucket = bucket[:i]
			}
			server.CreateBucket(bucket)

			cfg := datastore.DataStoreConfig{
				Type: "GCS",
				Params: map[string]string{
					"destination_bucket_path": tc.bucketPath,
				},
				Schema: testSchema,
			}

			ds, err := datastore.NewDataStore(ctx, cfg)
			require.NoError(t, err)

			populateLedgers(t, ctx, ds, testSchema, 2, 7)

			latest, err := datastore.FindLatestLedgerSequence(ctx, ds)
			require.NoError(t, err,
				"FindLatestLedgerSequence failed with bucket path %q", tc.bucketPath)
			require.Equal(t, uint32(7), latest,
				"should detect ledger 7 as latest with bucket path %q", tc.bucketPath)

			// Also verify FindLatestLedgerUpToSequence (bounded code path)
			latestUpTo, err := datastore.FindLatestLedgerUpToSequence(ctx, ds, 9, testSchema)
			require.NoError(t, err,
				"FindLatestLedgerUpToSequence failed with bucket path %q", tc.bucketPath)
			require.Equal(t, uint32(7), latestUpTo,
				"should detect ledger 7 as latest up to 9 with bucket path %q", tc.bucketPath)
		})
	}
}

func TestFindLatestLedgerSequenceFilesystem(t *testing.T) {
	ctx := context.Background()

	cases := []struct {
		name   string
		subdir string
		slash  bool
	}{
		{"flat-no-trailing-slash", "", false},
		{"flat-trailing-slash", "", true},
		{"subdirectory-no-trailing-slash", "sub", false},
		{"subdirectory-trailing-slash", "sub", true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if tc.subdir != "" {
				dir = filepath.Join(dir, tc.subdir)
				require.NoError(t, os.MkdirAll(dir, 0755))
			}
			if tc.slash {
				dir += "/"
			}

			cfg := datastore.DataStoreConfig{
				Type: "Filesystem",
				Params: map[string]string{
					"destination_path": dir,
				},
				Schema: testSchema,
			}

			ds, err := datastore.NewDataStore(ctx, cfg)
			require.NoError(t, err)

			populateLedgers(t, ctx, ds, testSchema, 2, 7)

			latest, err := datastore.FindLatestLedgerSequence(ctx, ds)
			require.NoError(t, err, "FindLatestLedgerSequence failed")
			require.Equal(t, uint32(7), latest, "should detect ledger 7 as latest")

			latestUpTo, err := datastore.FindLatestLedgerUpToSequence(ctx, ds, 9, testSchema)
			require.NoError(t, err, "FindLatestLedgerUpToSequence failed")
			require.Equal(t, uint32(7), latestUpTo, "should detect ledger 7 as latest up to 9")
		})
	}
}