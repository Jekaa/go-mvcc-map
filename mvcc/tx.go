package mvcc

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

// Sentinel errors для типизированной обработки на стороне вызывающего.
var (
	ErrConflict   = errors.New("mvcc: write-write conflict")
	ErrTxDone     = errors.New("mvcc: transaction already completed")
	ErrDeadlock   = errors.New("mvcc: deadlock detected")
	ErrTxCanceled = errors.New("mvcc: transaction canceled by context")
)

// txState описывает жизненный цикл транзакции конечным автоматом:
// active → committed | rolledBack
type txState uint32

const (
	txActive     txState = 0
	txCommitted  txState = 1
	txRolledBack txState = 2
)

// Tx — транзакция с snapshot isolation.
// Читает из снапшота момента BeginTx, накапливает изменения локально,
// при Commit атомарно применяет их к глобальному состоянию.
//
// Tx не потокобезопасен для одновременного использования
// из нескольких горутин — это осознанное решение:
// транзакция должна принадлежать одной горутине (как в database/sql).
type Tx[K comparable, V any] struct {
	id       uint64
	snapshot *version[K, V]          // снапшот на момент BeginTx (read-only)
	writes   map[K]versionedValue[V] // локальный write buffer
	readSet  map[K]struct{}          // ключи, которые мы читали (для будущего SI extension)

	state atomic.Uint32 // txState, атомик для безопасного чтения из detectDeadlocks

	ctx    context.Context
	cancel context.CancelFunc

	db *MVCCMap[K, V] // ссылка для Commit/Rollback
}

// Get возвращает значение ключа, видимое в рамках снапшота транзакции.
// Write buffer имеет приоритет (read-your-own-writes семантика).
func (tx *Tx[K, V]) Get(key K) (V, bool) {
	if err := tx.checkActive(); err != nil {
		var zero V
		return zero, false
	}

	// Сначала смотрим в локальный write buffer — транзакция видит
	// собственные изменения ещё до коммита.
	if vv, ok := tx.writes[key]; ok {
		tx.readSet[key] = struct{}{}
		return vv.value, true
	}

	// Затем — снапшот момента BeginTx.
	if vv, ok := tx.snapshot.data[key]; ok {
		tx.readSet[key] = struct{}{}
		return vv.value, true
	}

	var zero V
	return zero, false
}

// Put добавляет или обновляет значение в локальном write buffer.
// Изменение не видно другим транзакциям до Commit.
func (tx *Tx[K, V]) Put(key K, value V) error {
	if err := tx.checkActive(); err != nil {
		return err
	}
	if err := tx.ctx.Err(); err != nil {
		tx.Rollback()
		return fmt.Errorf("%w: %w", ErrTxCanceled, err)
	}

	tx.writes[key] = versionedValue[V]{
		value:      value,
		writerTxID: tx.id,
	}
	return nil
}

// Commit пытается применить изменения транзакции к глобальному состоянию.
// Возвращает ErrConflict, если другая транзакция изменила те же ключи
// после нашего снапшота.
func (tx *Tx[K, V]) Commit() error {
	if !tx.state.CompareAndSwap(uint32(txActive), uint32(txCommitted)) {
		return ErrTxDone
	}

	defer func() {
		tx.cancel()
		tx.db.unregisterTx(tx.id)
		tx.snapshot.refCount.Add(-1)
	}()

	if err := tx.ctx.Err(); err != nil {
		tx.state.Store(uint32(txRolledBack))
		return fmt.Errorf("%w: %w", ErrTxCanceled, err)
	}

	// Делегируем конфликт-проверку и применение изменений в MVCCMap,
	// т.к. только он владеет мьютексом над текущей версией.
	return tx.db.commit(tx)
}

// Rollback отменяет транзакцию. Безопасно вызывать несколько раз
// и после Commit (идемпотентна).
func (tx *Tx[K, V]) Rollback() {
	if !tx.state.CompareAndSwap(uint32(txActive), uint32(txRolledBack)) {
		return // уже завершена
	}
	tx.cancel()
	tx.db.unregisterTx(tx.id)
	tx.snapshot.refCount.Add(-1)
}

func (tx *Tx[K, V]) checkActive() error {
	if txState(tx.state.Load()) != txActive {
		return ErrTxDone
	}
	return nil
}

// txMeta — минимальные метаданные для deadlock detector,
// без хранения полного Tx (избегаем циклических зависимостей в GC).
type txMeta struct {
	id      uint64
	waitFor uint64 // ID транзакции, которую мы ждём (0 = никого)
	mu      sync.Mutex
}
