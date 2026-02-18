package mvcc

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
)

// MVCCMap — конкурентная in-memory map с поддержкой транзакций
// и snapshot isolation.
//
// Гарантии:
//   - Читатели не блокируют писателей и наоборот
//   - Write-write конфликты обнаруживаются при Commit
//   - Старые версии автоматически освобождаются GC-горутиной
//   - Deadlock между транзакциями обнаруживается и прерывается
type MVCCMap[K comparable, V any] struct {
	// mu защищает только moment коммита:
	// проверку конфликтов и установку новой текущей версии.
	// Это узкое критическое окно — намеренно, чтобы минимизировать contention.
	mu      sync.Mutex
	current atomic.Pointer[version[K, V]] // читается без блокировки

	nextTxID      atomic.Uint64
	nextVersionID atomic.Uint64

	// activeTxs хранит метаданные активных транзакций для:
	// 1. GC: min(snapshotID среди активных) — ниже не удаляем версии
	// 2. Deadlock detection: граф ожидания
	activeTxs   map[uint64]*txMeta
	activeTxsMu sync.RWMutex

	// versions — список всех версий для GC.
	// Храним отдельно от linked list, т.к. нам нужен O(1) доступ по ID.
	versions   []*version[K, V]
	versionsMu sync.Mutex

	logger *slog.Logger

	stopGC context.CancelFunc
	gcDone chan struct{}
}

// NewMVCCMap создаёт новый MVCCMap и запускает фоновые горутины
// GC и deadlock detector.
//
// Вызывающий должен вызвать Close() для корректного завершения.
func NewMVCCMap[K comparable, V any](ctx context.Context, opts ...Option) *MVCCMap[K, V] {
	cfg := defaultConfig()
	for _, o := range opts {
		o(&cfg)
	}

	gcCtx, stopGC := context.WithCancel(ctx)

	m := &MVCCMap[K, V]{
		activeTxs: make(map[uint64]*txMeta),
		logger:    cfg.logger,
		stopGC:    stopGC,
		gcDone:    make(chan struct{}),
	}

	// Инициализируем нулевую версию (пустая карта).
	v0 := newVersion[K, V](0, make(map[K]versionedValue[V]))
	m.current.Store(v0)
	m.versions = []*version[K, V]{v0}

	go m.runGC(gcCtx, cfg.gcInterval)
	go m.runDeadlockDetector(gcCtx, cfg.deadlockCheckInterval)

	return m
}

// Close останавливает фоновые горутины. Блокируется до их завершения.
func (m *MVCCMap[K, V]) Close() {
	m.stopGC()
	<-m.gcDone
}

// BeginTx начинает новую транзакцию, захватывая снапшот текущей версии.
//
// Снапшот захватывается атомарно через atomic.Pointer — без мьютекса.
// Это ключевое свойство: readers никогда не ждут writers.
func (m *MVCCMap[K, V]) BeginTx(ctx context.Context) *Tx[K, V] {
	txID := m.nextTxID.Add(1)

	// atomic.Pointer.Load() — acquire семантика, гарантирует, что мы видим
	// все записи, которые предшествовали Store() этой версии.
	snap := m.current.Load()
	snap.refCount.Add(1) // держим версию живой, пока транзакция активна

	txCtx, cancel := context.WithCancel(ctx)

	tx := &Tx[K, V]{
		id:       txID,
		snapshot: snap,
		writes:   make(map[K]versionedValue[V]),
		readSet:  make(map[K]struct{}),
		ctx:      txCtx,
		cancel:   cancel,
		db:       m,
	}

	m.activeTxsMu.Lock()
	m.activeTxs[txID] = &txMeta{id: txID}
	m.activeTxsMu.Unlock()

	return tx
}

// commit выполняется под мьютексом для атомарной проверки конфликтов
// и установки новой версии.
//
// Почему мьютекс, а не CAS-loop?
// CAS-loop при высокой конкуренции писателей создаёт livelock.
// Мьютекс гарантирует прогресс (fairness через runtime планировщик).
// При этом критическая секция минимальна: только conflict check + pointer swap.
func (m *MVCCMap[K, V]) commit(tx *Tx[K, V]) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	current := m.current.Load()

	// Write-write conflict detection:
	// Для каждого ключа, который мы хотим записать, проверяем:
	// был ли он изменён ПОСЛЕ нашего снапшота (т.е. другой транзакцией)?
	for key := range tx.writes {
		if vv, exists := current.data[key]; exists {
			// Если writerTxID != 0 и транзакция с таким ID уже не в нашем снапшоте —
			// значит, этот ключ изменили после нашего BeginTx.
			if vv.writerTxID != 0 && current.id > tx.snapshot.id {
				// Проверяем, изменился ли именно этот ключ после нашего снапшота.
				if snapVV, inSnap := tx.snapshot.data[key]; !inSnap ||
					snapVV.writerTxID != vv.writerTxID {
					return fmt.Errorf("%w: key conflict detected during commit", ErrConflict)
				}
			}
		}
	}

	// Создаём новую версию: клонируем текущую и применяем наши изменения.
	newData := current.clone()
	for k, vv := range tx.writes {
		newData[k] = vv
	}

	newVID := m.nextVersionID.Add(1)
	newVer := newVersion[K, V](newVID, newData)

	// Store с release семантикой: все операции до этого момента
	// будут видны тем, кто сделает Load() после.
	m.current.Store(newVer)

	m.versionsMu.Lock()
	m.versions = append(m.versions, newVer)
	m.versionsMu.Unlock()

	m.logger.Debug("committed transaction",
		"txID", tx.id,
		"versionID", newVID,
		"writtenKeys", len(tx.writes),
	)

	return nil
}

func (m *MVCCMap[K, V]) unregisterTx(txID uint64) {
	m.activeTxsMu.Lock()
	delete(m.activeTxs, txID)
	m.activeTxsMu.Unlock()
}

// VersionCount возвращает количество живых версий.
// Используется в тестах и метриках для контроля утечек памяти.
func (m *MVCCMap[K, V]) VersionCount() int {
	m.versionsMu.Lock()
	defer m.versionsMu.Unlock()
	return len(m.versions)
}
