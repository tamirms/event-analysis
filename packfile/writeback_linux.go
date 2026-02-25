package packfile

import (
	"os"

	"golang.org/x/sys/unix"
)

// initiateWriteback asks the kernel to start flushing dirty pages in the
// given range to disk. This is non-blocking — it returns immediately.
// Errors are silently ignored because this is a performance optimization;
// correctness is ensured by the fdatasync in Finish().
func initiateWriteback(f *os.File, offset, nbytes int64) {
	unix.SyncFileRange(int(f.Fd()), offset, nbytes, unix.SYNC_FILE_RANGE_WRITE)
}
