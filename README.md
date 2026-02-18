# MVCC Map — Транзакционная In-Memory Map с Snapshot Isolation

[![Go Version](https://img.shields.io/badge/Go-1.21+-00ADD8?style=flat&logo=go)](https://golang.org/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

## Содержание

- [Обзор](#обзор)
- [Архитектура](#архитектура)
  - [Общая схема](#общая-схема)
  - [Слои системы](#слои-системы)
  - [Жизненный цикл транзакции](#жизненный-цикл-транзакции)
  - [Версионирование данных](#версионирование-данных)
  - [Сборка мусора версий](#сборка-мусора-версий)
  - [Deadlock Detection](#deadlock-detection)
- [Гарантии корректности](#гарантии-корректности)
- [Применённые оптимизации](#применённые-оптимизации)
- [API](#api)
- [Быстрый старт](#быстрый-старт)
- [Конфигурация](#конфигурация)
- [Тестирование](#тестирование)
- [Сравнение с альтернативами](#сравнение-с-альтернативами)
- [Структура пакета](#структура-пакета)

---

## Обзор

`mvcc` — это реализация **Multi-Version Concurrency Control (MVCC)** для in-memory хранилища на Go. Пакет предоставляет транзакционную map с гарантией **Snapshot Isolation**: каждая транзакция видит согласованное состояние данных на момент своего начала, независимо от конкурентных изменений.

### Ключевые характеристики

| Характеристика | Значение |
|---|---|
| Модель изоляции | Snapshot Isolation |
| Обнаружение конфликтов | Write-Write Conflict Detection |
| Блокировки читателей | Отсутствуют (lock-free reads) |
| Управление памятью | Reference Counting + периодический GC |
| Обнаружение дедлоков | Wait-For Graph, DFS, Youngest-Victim |
| Конкурентность | Оптимистичная (Optimistic Concurrency Control) |

---

## Архитектура

### Общая схема

```
┌─────────────────────────────────────────────────────────────────┐
│                          MVCCMap                                 │
│                                                                  │
│   ┌─────────────────────────────────────────────────────────┐   │
│   │              atomic.Pointer[version]                    │   │
│   │         (current — читается без блокировок)             │   │
│   └───────────────────────┬─────────────────────────────────┘   │
│                           │                                      │
│              ┌────────────▼────────────┐                        │
│              │   version N (current)   │  refCount: 2           │
│              │   {key→value, ...}      │──────────────────────┐ │
│              └────────────┬────────────┘                      │ │
│                           │ clone при Commit                   │ │
│              ┌────────────▼────────────┐                      │ │
│              │   version N-1           │  refCount: 0  → GC   │ │
│              └─────────────────────────┘                      │ │
│                                                               │ │
│   ┌──────────────┐   ┌──────────────┐   ┌──────────────┐    │ │
│   │    Tx #1     │   │    Tx #2     │   │    Tx #3     │    │ │
│   │ snap: v(N)   │   │ snap: v(N)   │   │ snap: v(N-1) │────┘ │
│   │ writes: {...} │   │ writes: {...} │   │ writes: {}   │      │
│   └──────────────┘   └──────────────┘   └──────────────┘      │
│                                                                  │
│   ┌───────────────────┐   ┌──────────────────────────────┐     │
│   │  GC Goroutine     │   │  Deadlock Detector Goroutine  │     │
│   │  (каждые 5с)      │   │  (каждые 100мс)               │     │
│   └───────────────────┘   └──────────────────────────────┘     │
└─────────────────────────────────────────────────────────────────┘
```

### Слои системы

```
┌──────────────────────────────────────────────┐
│              Пользовательский код             │  ← BeginTx / Get / Put / Commit
├──────────────────────────────────────────────┤
│                   Tx                         │  ← snapshot + write buffer + state machine
├──────────────────────────────────────────────┤
│                MVCCMap                       │  ← commit() / conflict detection / version swap
├──────────────────────────────────────────────┤
│         version + versionedValue             │  ← copy-on-write snapshots, refCount
├──────────────────────────────────────────────┤
│     GC Goroutine | Deadlock Detector         │  ← фоновые сервисы
└──────────────────────────────────────────────┘
```

### Жизненный цикл транзакции

```
BeginTx(ctx)
    │
    ▼
[txActive] ────────────────────────────────────────────────┐
    │                                                        │
    │ Put(key, value)   → пишем в локальный write buffer    │
    │ Get(key)          → смотрим write buffer, затем snap  │
    │                                                        │
    ├──── Commit() ────────────────────────────────────────►│
    │         │                                              │
    │         ▼                                              │
    │    mu.Lock()                                           │
    │    conflict check (write-write)                        │
    │    clone current + apply writes                        │
    │    atomic.Store(newVersion)                            │
    │    mu.Unlock()                                         │
    │         │                                              │
    │    ┌────┴────┐                                         │
    │    │ Success │──────────────────► [txCommitted]        │
    │    │ Conflict│──────────────────► ErrConflict          │
    │    └─────────┘                                         │
    │                                                        │
    └──── Rollback() ─────────────────────────────────────► [txRolledBack]
                │
           refCount(snap) -= 1
           unregisterTx(id)
```

**Конечный автомат состояний `Tx`:**

```
        Put/Get/Commit/Rollback
              │
         ┌────▼────┐
         │ txActive │
         └────┬─────┘
              │
      ┌───────┴────────┐
      │                │
  Commit()         Rollback()
      │                │
┌─────▼──────┐  ┌──────▼──────┐
│txCommitted │  │txRolledBack │
└────────────┘  └─────────────┘
```

Переходы состояний выполняются через `atomic.CompareAndSwap` — без мьютекса, идемпотентно.

---

### Версионирование данных

Каждый успешный `Commit` создаёт **новую неизменяемую версию** (copy-on-write):

```
Commit tx#5:
                                       ┌──── atomic.Store ────┐
                                       │                       │
  current → [v3: {a:1, b:2, c:3}]     │    [v4: {a:1, b:9, c:3}]  ← current
              clone()                  │         ↑
              {a:1, b:2, c:3}  ────────┘    tx#5.writes: {b:9}
              apply writes
```

**Структура `versionedValue`** содержит значение и `writerTxID` — ID транзакции, совершившей запись. Это позволяет обнаруживать write-write конфликты без хранения полного лога изменений:

```go
// При Commit tx#6 проверяем:
// Если ключ "b" в current имеет writerTxID != writerTxID в нашем снапшоте
// → другая транзакция изменила этот ключ после нашего BeginTx → ErrConflict
```

**Алгоритм обнаружения конфликтов:**

```
для каждого ключа K в tx.writes:
    если K присутствует в current:
        если current[K].writerTxID ≠ snapshot[K].writerTxID:
            → CONFLICT (другая транзакция изменила K после нашего BeginTx)
```

---

### Сборка мусора версий

Версии удаляются фоновой горутиной по двум условиям одновременно:

```
Версия безопасна для удаления, если:
  1. v.id ≠ current.id          (не текущая)
  2. v.refCount == 0            (нет активных транзакций на этом снапшоте)
  3. v.id < minActiveSnapshotID (не нужна будущим читателям)
```

**Reference Counting** — выбранный механизм отслеживания живых версий:

```
BeginTx()  → snapshot.refCount.Add(+1)   // атомик, без мьютекса
Commit()   → snapshot.refCount.Add(-1)   // при завершении транзакции
Rollback() → snapshot.refCount.Add(-1)
```

GC переиспользует backing array при фильтрации (`versions[:0]`), избегая лишних аллокаций:

```go
kept := m.versions[:0]  // reuse, GC рантайма соберёт освобождённые элементы
for _, v := range m.versions {
    if isAlive(v) {
        kept = append(kept, v)
    }
}
m.versions = kept
```

---

### Deadlock Detection

Детектор работает как отдельная горутина и периодически анализирует **граф ожидания (Wait-For Graph)**:

```
Граф ожидания:
  Tx#1 → ждёт Tx#2
  Tx#2 → ждёт Tx#3
  Tx#3 → ждёт Tx#1   ← цикл = дедлок!

DFS обнаруживает цикл: [#1, #2, #3]

Выбор жертвы (Youngest-Victim):
  max(txID в цикле) = Tx#3  → отменяем через context.CancelFunc
```

**Почему Youngest-Victim?** Транзакция с наибольшим ID — самая молодая, она выполнила меньше работы. Откат обходится дешевле, чем откат старых транзакций.

**Почему периодически, а не на каждой операции?** Накладные расходы на обход графа при каждом `Put`/`Get` избыточны для in-memory системы. Интервал в 100мс достаточен для практических сценариев и не создаёт заметной нагрузки.

---

## Гарантии корректности

### Snapshot Isolation

| Аномалия | Статус |
|---|---|
| Dirty Read (чтение незакоммиченных данных) | ✅ Отсутствует |
| Non-Repeatable Read (повторное чтение даёт другой результат) | ✅ Отсутствует |
| Phantom Read (появление новых строк при повторном запросе) | ✅ Отсутствует |
| Read Skew (несогласованные данные в одной транзакции) | ✅ Отсутствует |
| Write Skew (аномалия при параллельных взаимозависимых изменениях) | ⚠️ Возможна* |
| Lost Update (потеря обновлений) | ✅ Обнаруживается через Write-Write Conflict |

> *Write Skew — известное ограничение Snapshot Isolation. Устраняется переходом к Serializable Snapshot Isolation (SSI) с дополнительным отслеживанием read set. Реализация SSI — возможное направление развития.

### Модель памяти Go

Корректность конкурентного доступа обеспечивается:

- `atomic.Pointer.Store/Load` — release/acquire семантика для безопасной публикации версий
- `sync.Mutex` в `commit()` — сериализация писателей при конфликт-проверке
- `atomic.Int64` для `refCount` — lock-free инкремент/декремент без гонок
- `atomic.Uint32` для `txState` — безопасные переходы состояний (`CAS`)

---

## Применённые оптимизации

### 1. Lock-Free Reads через `atomic.Pointer`

```
Без оптимизации:          С оптимизацией:
                          
mu.RLock()                snap := current.Load()  ← O(1), нет блокировок
snap := current           // читаем снапшот
mu.RUnlock()
```

`atomic.Pointer` с release/acquire семантикой гарантирует корректность модели памяти без мьютекса. Все операции чтения (`BeginTx + Get`) — полностью неблокирующие.

**Результат:** Reader throughput масштабируется линейно с числом CPU.

---

### 2. Минимальная критическая секция при Commit

Мьютекс удерживается только во время:
1. Проверки конфликтов (итерация по `tx.writes`)
2. Атомарной замены указателя на текущую версию

```go
m.mu.Lock()
// ← критическая секция: O(|writes|), не O(|map|)
conflictCheck(tx.writes, current)
m.current.Store(newVersion)
m.mu.Unlock()
```

Клонирование карты (`maps.Clone`) происходит **вне** мьютекса в большинстве реализаций, либо занимает минимальное время для небольших write-set'ов.

---

### 3. Copy-on-Write с `maps.Clone` (Go 1.21+)

```go
newData := current.clone()  // maps.Clone — оптимизированная stdlib функция
for k, vv := range tx.writes {
    newData[k] = vv         // применяем только изменения
}
```

`maps.Clone` использует внутренние оптимизации рантайма Go для bulk-копирования map. Сложность: O(n) по размеру карты — это неизбежно для CoW, но компенсируется тем, что копирование не блокирует читателей.

---

### 4. Reference Counting без блокировок (`atomic.Int64`)

```go
// BeginTx — горячий путь, никаких мьютексов:
snap := m.current.Load()   // acquire
snap.refCount.Add(1)       // атомарный инкремент

// Commit/Rollback:
snap.refCount.Add(-1)      // атомарный декремент
```

Альтернатива — Epoch-Based Reclamation (используется в crossbeam-rs) — сложнее в реализации на Go и не даёт значимого преимущества для данного use-case.

---

### 5. GC с переиспользованием backing array

```go
// Стандартный подход — создаёт новый slice:
var kept []*version
for _, v := range m.versions { ... }

// Наш подход — reuse backing array, 0 аллокаций:
kept := m.versions[:0]
for _, v := range m.versions { ... }
m.versions = kept
```

Это уменьшает давление на GC рантайма Go, особенно при высокой частоте коммитов.

---

### 6. Локальный Write Buffer без синхронизации

`Tx.writes` — обычный `map`, недоступный другим горутинам (транзакция принадлежит одной горутине). Это означает отсутствие синхронизации при `Put`/`Get` внутри транзакции:

```
Транзакция        Write Buffer      Снапшот
    │                                  │
    ├── Put("a", 1) ──► writes["a"]=1  │
    ├── Get("a")    ◄── writes["a"]    │  ← read-your-own-writes
    ├── Get("b")                       ├── snapshot["b"] ← нет в writes
    │                                  │
    └── Commit()   ──► применяем writes к новой версии
```

---

### 7. Оптимистичная конкуренция (OCC)

Вместо пессимистичных блокировок ключей при `Put`:

```
Pessimistic (2PL):            Optimistic (OCC):
Put("key") → lock("key")      Put("key") → write buffer
...                           ...
Commit()   → unlock("key")    Commit()   → conflict check
                                         → commit or retry
```

OCC эффективнее при низкой конкуренции на запись (типичный случай для большинства нагрузок). При высокой конкуренции на один ключ возрастает число откатов — это известный trade-off.

---

### 8. Конечный автомат состояний через `atomic.CompareAndSwap`

```go
// Идемпотентный Rollback без мьютекса:
if !tx.state.CompareAndSwap(uint32(txActive), uint32(txRolledBack)) {
    return // уже завершена — выходим без паники и без ошибки
}
```

`CAS` гарантирует, что только один вызов (Commit или Rollback) "выиграет" переход, даже при конкурентном вызове из нескольких горутин.

---

## API

```go
// Создание хранилища
m := mvcc.NewMVCCMap[string, int](ctx, opts...)
defer m.Close() // останавливает GC и deadlock detector

// Транзакция
tx := m.BeginTx(ctx)

val, ok := tx.Get("key")       // чтение из снапшота
err := tx.Put("key", 42)       // запись в локальный буфер

err = tx.Commit()              // применить изменения
// или
tx.Rollback()                  // отменить изменения

// Типы ошибок
errors.Is(err, mvcc.ErrConflict)   // write-write конфликт
errors.Is(err, mvcc.ErrTxDone)    // транзакция уже завершена
errors.Is(err, mvcc.ErrDeadlock)  // обнаружен дедлок
errors.Is(err, mvcc.ErrTxCanceled) // контекст отменён
```

---

## Быстрый старт

```go
package main

import (
    "context"
    "errors"
    "fmt"
    "log"

    "github.com/yourorg/mvcc"
)

func main() {
    ctx := context.Background()

    // Создаём хранилище для строковых ключей и int значений
    m := mvcc.NewMVCCMap[string, int](ctx)
    defer m.Close()

    // Инициализируем начальное состояние
    setup := m.BeginTx(ctx)
    _ = setup.Put("balance_alice", 1000)
    _ = setup.Put("balance_bob", 500)
    if err := setup.Commit(); err != nil {
        log.Fatal(err)
    }

    // Транзакция перевода средств
    transfer := func(from, to string, amount int) error {
        tx := m.BeginTx(ctx)
        defer tx.Rollback() // безопасно вызывать после Commit

        fromBal, _ := tx.Get(from)
        if fromBal < amount {
            return fmt.Errorf("insufficient funds")
        }

        toBal, _ := tx.Get(to)
        _ = tx.Put(from, fromBal-amount)
        _ = tx.Put(to, toBal+amount)

        return tx.Commit()
    }

    if err := transfer("balance_alice", "balance_bob", 200); err != nil {
        if errors.Is(err, mvcc.ErrConflict) {
            log.Println("конфликт, повторите попытку")
        } else {
            log.Fatal(err)
        }
    }

    // Читаем результат
    reader := m.BeginTx(ctx)
    alice, _ := reader.Get("balance_alice")
    bob, _ := reader.Get("balance_bob")
    reader.Rollback()

    fmt.Printf("Alice: %d, Bob: %d\n", alice, bob) // Alice: 800, Bob: 700
}
```

---

## Конфигурация

```go
m := mvcc.NewMVCCMap[string, int](ctx,
    // Интервал GC — как часто удалять старые версии
    // Меньше → меньше памяти, больше CPU на GC
    mvcc.WithGCInterval(5 * time.Second),

    // Интервал проверки дедлоков
    // Меньше → быстрее обнаружение, больше CPU
    mvcc.WithDeadlockCheckInterval(100 * time.Millisecond),

    // Кастомный структурированный логгер
    mvcc.WithLogger(slog.Default()),
)
```

---

## Тестирование

```bash
# Все тесты
go test ./...

# С детектором гонок (обязательно для конкурентного кода)
go test -race ./...

# Конкретный тест
go test -run TestWriteWriteConflict -v ./...

# Бенчмарки
go test -bench=. -benchmem ./...

# Бенчмарк с профилировщиком
go test -bench=BenchmarkConcurrentReadWrite -cpuprofile=cpu.prof ./...
go tool pprof cpu.prof
```

### Покрытые сценарии

| Тест | Что проверяет |
|---|---|
| `TestSnapshotIsolation_NoReadSkew` | Читатель видит снапшот момента BeginTx, не видит конкурентных изменений |
| `TestWriteWriteConflict` | Две транзакции на один ключ — вторая получает `ErrConflict` |
| `TestReadersDoNotBlockWriters` | 100 долгих читателей не блокируют писателя |
| `TestNoMemoryLeakWithLongTransactions` | После 1000 коммитов GC оставляет ≤5 версий |
| `TestReadYourOwnWrites` | Транзакция видит собственные незакоммиченные изменения |
| `BenchmarkConcurrentReadWrite` | Throughput при 90% reads / 10% writes |

---

## Сравнение с альтернативами

### MVCCMap vs `sync.RWMutex` Map

| Критерий | `sync.RWMutex` | MVCCMap |
|---|---|---|
| Сложность реализации | Низкая | Высокая |
| Read throughput (многоядерность) | Ограничен RWMutex | Линейный рост |
| Write latency | Блокирует всех читателей | Нет блокировки читателей |
| Snapshot Isolation | Нет | Да |
| Транзакции | Нет | Да |
| Потребление памяти | O(n) | O(n × активные версии) |

**Когда выбрать `sync.RWMutex`:** простой кэш без транзакционных требований, минимальный код, нет нужды в снапшотах.

### MVCCMap vs Persistent (Immutable) Data Structures (HAMT)

| Критерий | CoW + `maps.Clone` | HAMT |
|---|---|---|
| Commit: сложность | O(n) | O(log n) |
| Commit: константа | Низкая (bulk copy) | Высокая (много мелких аллокаций) |
| GC pressure | Умеренное | Высокое |
| Сложность кода | Средняя | Очень высокая |

**Когда выбрать HAMT:** карты с миллионами ключей, очень частые коммиты с небольшим write-set'ом.

### MVCCMap vs `sync.Map`

`sync.Map` оптимизирован для конкретного паттерна (write-once, read-many) и не предоставляет транзакционной семантики. Построить поверх него snapshot isolation без значительных доработок невозможно.

---

## Структура пакета

```
mvcc/
├── map.go        — MVCCMap: BeginTx, commit, unregisterTx, Close
├── tx.go         — Tx: Get, Put, Commit, Rollback, конечный автомат
├── version.go    — version, versionedValue, clone
├── gc.go         — runGC, collectVersions
├── deadlock.go   — runDeadlockDetector, detectDeadlocks, resolveDeadlock
├── options.go    — Option, config, defaultConfig
└── map_test.go   — unit-тесты и бенчмарки
```
