package mvcc

import (
	"maps"
	"sync/atomic"
)

// version представляет неизменяемый снимок данных.
// Используем copy-on-write: коммит создаёт новую version,
// не мутируя предыдущую — это обеспечивает lock-free чтение.
type version[K comparable, V any] struct {
	id   uint64
	data map[K]versionedValue[V]

	// refCount позволяет GC-горутине понять, когда версию
	// можно удалить. Атомик — чтобы не держать мьютекс при
	// инкременте/декременте в BeginTx/Commit/Rollback.
	refCount atomic.Int64
}

// versionedValue хранит значение и txID, который его записал.
// txID нужен для write-write conflict detection:
// если при коммите мы видим, что ключ изменён чужой транзакцией
// после нашего снапшота — это конфликт.
type versionedValue[V any] struct {
	value      V
	writerTxID uint64 // ID транзакции, совершившей запись
}

func newVersion[K comparable, V any](id uint64, data map[K]versionedValue[V]) *version[K, V] {
	v := &version[K, V]{
		id:   id,
		data: data,
	}
	return v
}

// clone создаёт копию данных для нового коммита.
// maps.Clone из Go 1.21 — shallow copy, что достаточно,
// т.к. V трактуется как value type (или неизменяемый указатель).
func (v *version[K, V]) clone() map[K]versionedValue[V] {
	return maps.Clone(v.data)
}
