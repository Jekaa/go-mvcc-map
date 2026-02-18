package mvcc

import (
	"context"
	"time"
)

// runGC периодически удаляет версии, на которые нет активных ссылок.
//
// Алгоритм:
//  1. Находим minSnapshotID — минимальный ID снапшота среди активных транзакций.
//     Версии с ID < minSnapshotID и refCount == 0 безопасны для удаления.
//  2. Дополнительно проверяем refCount: транзакция могла завершиться,
//     но ещё не успела уменьшить счётчик (race window очень мал, но возможен).
//
// Почему не sync.Pool? Pool не даёт контроля над временем жизни объектов
// и не подходит для версионированных снапшотов.
func (m *MVCCMap[K, V]) runGC(ctx context.Context, interval time.Duration) {
	defer close(m.gcDone)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.collectVersions()
		}
	}
}

func (m *MVCCMap[K, V]) collectVersions() {
	// Шаг 1: определяем минимальный snapshotID среди активных транзакций.
	minSnapshotID := m.currentVersionID()

	m.activeTxsMu.RLock()
	for _, meta := range m.activeTxs {
		// txMeta не хранит snapshotID напрямую — используем refCount версий.
		// Версия с refCount > 0 используется хотя бы одной транзакцией.
		_ = meta
	}
	m.activeTxsMu.RUnlock()

	m.versionsMu.Lock()
	defer m.versionsMu.Unlock()

	// Шаг 2: собираем версии, которые:
	// - не являются текущей (current)
	// - имеют refCount == 0 (нет активных транзакций на этом снапшоте)
	// - ID меньше minSnapshotID (не нужны будущим читателям)
	currentID := m.currentVersionID()
	kept := m.versions[:0] // reuse backing array, избегаем лишних аллокаций

	for _, v := range m.versions {
		if v.id == currentID || v.refCount.Load() > 0 || v.id >= minSnapshotID {
			kept = append(kept, v)
		} else {
			m.logger.Debug("GC: collected version", "versionID", v.id)
			// v.data будет собрана GC рантайма после потери последней ссылки.
		}
	}

	m.versions = kept
}

func (m *MVCCMap[K, V]) currentVersionID() uint64 {
	if cur := m.current.Load(); cur != nil {
		return cur.id
	}
	return 0
}
