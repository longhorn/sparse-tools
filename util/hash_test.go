package util

import (
	"os"
	"testing"

	"github.com/longhorn/sparse-tools/types"
	"github.com/stretchr/testify/require"
)

func TestSetSnapshotHashInfoToChecksumFile(t *testing.T) {
	assert := require.New(t)

	checksumFileName := RandomID(8) + types.DiskChecksumSuffix
	expectedInfo := &types.SnapshotHashInfo{
		Method:     "crc64",
		Checksum:   "abc",
		ChangeTime: "2022-11-29 16:26:40.297975939 +0000",
	}

	err := SetSnapshotHashInfoToChecksumFile(checksumFileName, expectedInfo)
	assert.Nil(err)
	defer func() {
		_ = os.RemoveAll(checksumFileName)
	}()

	info, err := GetSnapshotHashInfoFromChecksumFile(checksumFileName)
	assert.Nil(err)

	assert.Equal(info, expectedInfo)
}
