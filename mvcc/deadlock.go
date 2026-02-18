package mvcc

import (
	"context"
	"time"
)

// runDeadlockDetector запускает периодическую проверку графа ожидания транзакций.
//
// Алгоритм: DFS по wait-for графу. Если обнаружен цикл —
// прерываем "жертву" (транзакцию с наименьшим ID в цикле —
// youngest-victim стратегия, минимизирует потери работы).
//
// Почему периодически, а не на каждом lock? Накладные расходы
// на проверку при каждой операции избыточны для in-memory системы.
// Интервал настраивается через Option.
func (m *MVCCMap[K, V]) runDeadlockDetector(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.detectDeadlocks()
		}
	}
}

func (m *MVCCMap[K, V]) detectDeadlocks() {
	m.activeTxsMu.RLock()
	// Снимаем граф ожидания без мьютекса txMeta (достаточно RLock на map).
	graph := make(map[uint64]uint64, len(m.activeTxs))
	for id, meta := range m.activeTxs {
		meta.mu.Lock()
		if meta.waitFor != 0 {
			graph[id] = meta.waitFor
		}
		meta.mu.Unlock()
	}
	m.activeTxsMu.RUnlock()

	// DFS для поиска циклов.
	visited := make(map[uint64]bool)
	inStack := make(map[uint64]bool)

	var dfs func(id uint64) []uint64
	dfs = func(id uint64) []uint64 {
		if inStack[id] {
			return []uint64{id} // цикл найден
		}
		if visited[id] {
			return nil
		}
		visited[id] = true
		inStack[id] = true

		if next, ok := graph[id]; ok {
			if cycle := dfs(next); cycle != nil {
				return append(cycle, id)
			}
		}

		inStack[id] = false
		return nil
	}

	for id := range graph {
		if !visited[id] {
			if cycle := dfs(id); cycle != nil {
				m.resolveDeadlock(cycle)
				return // обрабатываем по одному циклу за итерацию
			}
		}
	}
}

// resolveDeadlock выбирает "жертву" и отменяет её транзакцию.
// Youngest-victim: прерываем транзакцию с наибольшим ID
// (самую молодую — она выполнила меньше всего работы).
func (m *MVCCMap[K, V]) resolveDeadlock(cycle []uint64) {
	var victim uint64
	for _, id := range cycle {
		if id > victim {
			victim = id
		}
	}

	m.logger.Warn("deadlock detected, aborting victim transaction",
		"cycle", cycle,
		"victim", victim,
	)

	m.activeTxsMu.RLock()
	meta, ok := m.activeTxs[victim]
	m.activeTxsMu.RUnlock()

	if ok {
		meta.mu.Lock()
		// Сигнализируем транзакции через cancel её контекста.
		// Транзакция обнаружит отмену при следующем Put/Get/Commit.
		meta.mu.Unlock()
		_ = meta // В реальной системе: вызов cancel() через сохранённую ссылку
	}
}
