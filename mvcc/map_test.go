package mvcc_test

import (
	"context"
	"errors"
	"mvcc-map/mvcc"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newTestMap(t *testing.T) (*mvcc.MVCCMap[string, int], context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	m := mvcc.NewMVCCMap[string, int](ctx,
		mvcc.WithGCInterval(50*time.Millisecond),
	)
	t.Cleanup(func() {
		cancel()
		m.Close()
	})
	return m, cancel
}

// TestSnapshotIsolation_NoReadSkew проверяет, что транзакция не видит
// изменений, сделанных другими транзакциями после её начала.
func TestSnapshotIsolation_NoReadSkew(t *testing.T) {
	m, _ := newTestMap(t)
	ctx := context.Background()

	// Устанавливаем начальное значение.
	setup := m.BeginTx(ctx)
	_ = setup.Put("balance", 100)
	if err := setup.Commit(); err != nil {
		t.Fatal(err)
	}

	// Начинаем "длинную" читающую транзакцию.
	reader := m.BeginTx(ctx)

	// Другая транзакция изменяет значение.
	writer := m.BeginTx(ctx)
	_ = writer.Put("balance", 200)
	if err := writer.Commit(); err != nil {
		t.Fatal(err)
	}

	// Читатель должен видеть старое значение (100), несмотря на коммит writer.
	val, ok := reader.Get("balance")
	if !ok {
		t.Fatal("key not found")
	}
	if val != 100 {
		t.Errorf("read skew detected: expected 100, got %d", val)
	}
	reader.Rollback()
}

// TestWriteWriteConflict проверяет, что две транзакции, изменяющие
// один ключ, порождают конфликт.
func TestWriteWriteConflict(t *testing.T) {
	m, _ := newTestMap(t)
	ctx := context.Background()

	tx1 := m.BeginTx(ctx)
	tx2 := m.BeginTx(ctx)

	_ = tx1.Put("counter", 1)
	_ = tx2.Put("counter", 2)

	// tx1 коммитится первым — успех.
	if err := tx1.Commit(); err != nil {
		t.Fatalf("tx1 commit failed unexpectedly: %v", err)
	}

	// tx2 должна упасть с ErrConflict.
	err := tx2.Commit()
	if err == nil {
		t.Fatal("expected conflict error, got nil")
	}
	if !errors.Is(err, mvcc.ErrConflict) {
		t.Errorf("expected ErrConflict, got: %v", err)
	}
}

// TestReadersDoNotBlockWriters проверяет отсутствие блокировок
// между читателями и писателями.
func TestReadersDoNotBlockWriters(t *testing.T) {
	m, _ := newTestMap(t)
	ctx := context.Background()

	// Запускаем 100 долгоживущих читателей.
	var wg sync.WaitGroup
	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tx := m.BeginTx(ctx)
			defer tx.Rollback()
			_, _ = tx.Get("key")
			time.Sleep(50 * time.Millisecond) // имитируем долгую транзакцию
		}()
	}

	// Писатель должен завершиться быстро, не ожидая читателей.
	done := make(chan struct{})
	go func() {
		defer close(done)
		tx := m.BeginTx(ctx)
		_ = tx.Put("key", 42)
		if err := tx.Commit(); err != nil {
			t.Errorf("writer failed: %v", err)
		}
	}()

	select {
	case <-done:
		// OK: писатель не заблокировался
	case <-time.After(10 * time.Millisecond):
		t.Error("writer was blocked by readers")
	}

	wg.Wait()
}

// TestNoMemoryLeakWithLongTransactions проверяет, что долгие транзакции
// не приводят к бесконтрольному росту числа версий.
func TestNoMemoryLeakWithLongTransactions(t *testing.T) {
	m, _ := newTestMap(t)
	ctx := context.Background()

	// Запускаем 1000 коммитов.
	for i := range 1000 {
		tx := m.BeginTx(ctx)
		_ = tx.Put("key", i)
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	// Ждём GC.
	time.Sleep(200 * time.Millisecond)

	// Проверяем, что количество версий не растёт линейно.
	// В идеале должна остаться только текущая версия.
	count := m.VersionCount()
	if count > 5 { // небольшой буфер на race window
		t.Errorf("version leak: %d versions still alive", count)
	}
}

// TestReadYourOwnWrites проверяет, что транзакция видит собственные изменения.
func TestReadYourOwnWrites(t *testing.T) {
	m, _ := newTestMap(t)

	tx := m.BeginTx(context.Background())
	defer tx.Rollback()

	_ = tx.Put("x", 42)
	val, ok := tx.Get("x")
	if !ok || val != 42 {
		t.Errorf("expected read-your-own-writes: got %v, %v", val, ok)
	}
}

// BenchmarkConcurrentReadWrite измеряет throughput при смешанной нагрузке.
func BenchmarkConcurrentReadWrite(b *testing.B) {
	ctx := context.Background()
	m := mvcc.NewMVCCMap[string, int](ctx)
	defer m.Close()

	// Предзаполняем.
	tx := m.BeginTx(ctx)
	_ = tx.Put("key", 0)
	_ = tx.Commit()

	var ops atomic.Int64

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if ops.Add(1)%10 == 0 { // 10% writes
				tx := m.BeginTx(ctx)
				_ = tx.Put("key", 1)
				_ = tx.Commit()
			} else {
				tx := m.BeginTx(ctx)
				_, _ = tx.Get("key")
				tx.Rollback()
			}
		}
	})
}
