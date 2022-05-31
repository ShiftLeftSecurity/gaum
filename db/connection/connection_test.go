package connection

import (
	"context"
	"testing"

	"github.com/go-test/deep"
)

type fakeConn struct {
	DB
	begin    int
	commit   int
	rollback int
	isTx     bool
}

func (f *fakeConn) BeginTransaction(ctx context.Context) (DB, error) {
	f.begin++
	f.isTx = true
	return f, nil
}

func (f *fakeConn) CommitTransaction(ctx context.Context) error {
	f.commit++
	return nil
}

func (f *fakeConn) RollbackTransaction(ctx context.Context) error {
	f.rollback++
	return nil
}

func (f *fakeConn) IsTransaction() bool {
	return f.isTx
}

var _ DB = (*fakeConn)(nil)

func TestFlexibleTransactionSucceeds(t *testing.T) {
	// Multiple TX begins, multiple commits
	fc := &fakeConn{}
	ctx := context.Background()
	tx, cleanup, err := BeginTransaction(ctx, fc)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		if err := tx.CommitTransaction(ctx); err != nil {
			t.Logf("Repetitive commit N %d", i+1)
			t.Fatal(err)
		}
	}
	committed, rolledBack, err := cleanup(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !committed {
		t.Log("tx was not committed but we expected it to")
		t.FailNow()
	}
	if rolledBack {
		t.Log("tx was rolled back and we did not expect it to")
		t.FailNow()
	}

	if fc.begin != 1 {
		t.Logf("begin was called %d times in the underlying conn but we expected 1", fc.begin)
		t.FailNow()
	}

	if fc.commit != 1 {
		t.Logf("commit was called %d times in the underlying conn but we expected 1", fc.commit)
		t.FailNow()
	}

	if fc.rollback != 0 {
		t.Logf("rollback was called %d times in the underlying conn but we expected 0", fc.rollback)
		t.FailNow()
	}
}

func TestFlexibleTransactionRollbackTransaction(t *testing.T) {
	// Multiple TX begins, multiple commits
	fc := &fakeConn{}
	ctx := context.Background()
	tx, cleanup, err := BeginTransaction(ctx, fc)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		if err := tx.CommitTransaction(ctx); err != nil {
			t.Logf("Repetitive commit N %d", i+1)
			t.Fatal(err)
		}
	}
	if err := tx.RollbackTransaction(ctx); err != nil {
		t.Fatal(err)
	}
	committed, rolledBack, err := cleanup(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if committed {
		t.Log("tx was committed but we expected it not to")
		t.FailNow()
	}
	if !rolledBack {
		t.Log("tx was not rolled back and we expected it to")
		t.FailNow()
	}

	if fc.begin != 1 {
		t.Logf("begin was called %d times in the underlying conn but we expected 1", fc.begin)
		t.FailNow()
	}

	if fc.commit != 0 {
		t.Logf("commit was called %d times in the underlying conn but we expected 0", fc.commit)
		t.FailNow()
	}

	if fc.rollback != 1 {
		t.Logf("rollback was called %d times in the underlying conn but we expected 1", fc.rollback)
		t.FailNow()
	}
}

func TestFlexibleTransactionRecursive(t *testing.T) {
	// Multiple TX begins, multiple commits
	fc := &fakeConn{}
	ctx := context.Background()
	tx, cleanup, err := BeginTransaction(ctx, fc)
	if err != nil {
		t.Fatal(err)
	}

	tx, innerCleanup, err := BeginTransaction(ctx, fc)
	if err != nil {
		t.Fatal(err)
	}

	// we call it early to see if it really is a noop
	innerCommit, innerRollback, err := innerCleanup(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if innerCommit {
		t.Log("commit should not have happened on inner cleanup function")
		t.FailNow()
	}
	if innerRollback {
		t.Log("rollback should not have happened on inner cleanup function")
		t.FailNow()
	}
	// notice that we are using the inner tx
	for i := 0; i < 10; i++ {
		if err := tx.CommitTransaction(ctx); err != nil {
			t.Logf("Repetitive commit N %d", i+1)
			t.Fatal(err)
		}
	}
	committed, rolledBack, err := cleanup(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !committed {
		t.Log("tx was not committed but we expected it to")
		t.FailNow()
	}
	if rolledBack {
		t.Log("tx was rolled back and we did not expect it to")
		t.FailNow()
	}

	if fc.begin != 1 {
		t.Logf("begin was called %d times in the underlying conn but we expected 1", fc.begin)
		t.FailNow()
	}

	if fc.commit != 1 {
		t.Logf("commit was called %d times in the underlying conn but we expected 1", fc.commit)
		t.FailNow()
	}

	if fc.rollback != 0 {
		t.Logf("rollback was called %d times in the underlying conn but we expected 0", fc.rollback)
		t.FailNow()
	}
}

func TestEscapeArgsOK(t *testing.T) {
	for in, out := range map[string]string{
		"from ? where ?=?":     "from $1 where $2=$3",
		"from ? where ? \\? ?": "from $1 where $2 ? $3",
		`\\??\??`:              `\$1$2?$3`,
	} {
		t.Run("", func(t *testing.T) {
			args := []interface{}{"hello", 1, 42.}
			got, gotArgs, err := EscapeArgs(in, args)
			if err != nil {
				t.Fatal(err)
			}
			if diff := deep.Equal(args, gotArgs); diff != nil {
				t.Fatal(err)
			}
			if got != out {
				t.Errorf("expected %q, got %q", out, got)
			}
		})
	}
}
