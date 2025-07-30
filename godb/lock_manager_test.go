package godb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLockManager_Lock(t *testing.T) {
	lm := NewLockManager()
	rq := require.New(t)

	t.Run("should lock and unlock", func(t *testing.T) {
		tid1 := TransactionID(1)
		tid2 := TransactionID(2)

		ch := make(chan error, 1)

		rq.NoError(lm.Lock(tid1, heapHash{FileName: "lock and unlock", PageNo: 0}, WritePerm))
		go func() {
			ch <- lm.Lock(tid2, heapHash{FileName: "lock and unlock", PageNo: 0}, ReadPerm)
		}()

		time.Sleep(time.Millisecond * 100)

		select {
		case <-ch:
			rq.Fail("expect block")
		default:
			rq.NoError(lm.Unlock(tid1, heapHash{FileName: "lock and unlock", PageNo: 0}, WritePerm))
		}

		time.Sleep(time.Millisecond * 100)
		rq.NoError(lm.Unlock(tid2, heapHash{FileName: "lock and unlock", PageNo: 0}, ReadPerm))
	})

	t.Run("should not lock shared lock when there is write req", func(t *testing.T) {
		tid1 := TransactionID(1)
		tid2 := TransactionID(2)
		tid3 := TransactionID(3)

		ch := make(chan error, 1)

		rq.NoError(lm.Lock(tid1, heapHash{FileName: "write req", PageNo: 0}, ReadPerm))
		go func() {
			lm.Lock(tid2, heapHash{FileName: "write req", PageNo: 0}, WritePerm)
		}()

		time.Sleep(time.Millisecond * 100)
		go func() {
			ch <- lm.Lock(tid3, heapHash{FileName: "write req", PageNo: 0}, ReadPerm)
		}()

		time.Sleep(time.Millisecond * 100)

		select {
		case <-ch:
			rq.Fail("expect block")
		default:
		}
	})

	t.Run("should lock all shared when lock released", func(t *testing.T) {
		tid1 := TransactionID(1)
		tid2 := TransactionID(2)
		tid3 := TransactionID(3)

		rq.NoError(lm.Lock(tid1, heapHash{FileName: "release lock", PageNo: 0}, WritePerm))

		go lm.Lock(tid2, heapHash{FileName: "release lock", PageNo: 0}, ReadPerm)
		go lm.Lock(tid3, heapHash{FileName: "release lock", PageNo: 0}, ReadPerm)

		time.Sleep(time.Millisecond * 100)
		rq.NoError(lm.Unlock(tid1, heapHash{FileName: "release lock", PageNo: 0}, WritePerm))

		time.Sleep(time.Millisecond * 100)
		rq.NoError(lm.Unlock(tid2, heapHash{FileName: "release lock", PageNo: 0}, ReadPerm))
		rq.NoError(lm.Unlock(tid3, heapHash{FileName: "release lock", PageNo: 0}, ReadPerm))
	})

	t.Run("should lock when shared lock", func(t *testing.T) {
		tid1 := TransactionID(1)
		tid2 := TransactionID(2)

		rq.NoError(lm.Lock(tid1, heapHash{FileName: "shared lock", PageNo: 0}, ReadPerm))
		rq.NoError(lm.Lock(tid2, heapHash{FileName: "shared lock", PageNo: 0}, ReadPerm))
	})
}

func TestLockManager_Deadlock(t *testing.T) {
	lm := NewLockManager()
	rq := require.New(t)

	t.Run("should detect deadlock", func(t *testing.T) {
		tid1 := TransactionID(1)
		tid2 := TransactionID(2)

		rq.NoError(lm.Lock(tid1, heapHash{FileName: "deadlock", PageNo: 0}, WritePerm))
		rq.NoError(lm.Lock(tid2, heapHash{FileName: "deadlock", PageNo: 1}, ReadPerm))

		go func() {
			lm.Lock(tid1, heapHash{FileName: "deadlock", PageNo: 1}, WritePerm)
		}()
		time.Sleep(time.Millisecond * 500)

		rq.Error(lm.Lock(tid2, heapHash{FileName: "deadlock", PageNo: 0}, ReadPerm))
	})
}
