package util

import (
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/longhorn/sparse-tools/types"
	"github.com/stretchr/testify/require"
)

func randomID(randomIDLenth int) string {
	id := uuid.New().String()
	return id[:randomIDLenth]
}

func TestSetSnapshotHashInfoToChecksumFile(t *testing.T) {
	assert := require.New(t)

	checksumFileName := randomID(8) + types.DiskChecksumSuffix
	expectedInfo := &types.SnapshotHashInfo{
		Method:     "crc64",
		Checksum:   "abc",
		ChangeTime: "2022-11-29 16:26:40.297975939 +0000",
	}

	err := SetSnapshotHashInfoToChecksumFile(checksumFileName, expectedInfo)
	assert.Nil(err)
	defer os.RemoveAll(checksumFileName)

	info, err := GetSnapshotHashInfoFromChecksumFile(checksumFileName)
	assert.Nil(err)

	assert.Equal(info, expectedInfo)
}
