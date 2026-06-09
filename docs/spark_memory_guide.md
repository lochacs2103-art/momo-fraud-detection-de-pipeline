# Spark Memory Management — Giải thích chi tiết

> Đây là một trong những phần quan trọng nhất khi làm việc với Spark ở production.
> 90% lỗi OOM (Out of Memory) đều do không hiểu rõ memory layout.

---

## 1. Hiểu lầm phổ biến nhất

Khi bạn set:
```yaml
spark.executor.memory: 4g
```

Nhiều người nghĩ executor có **4GB để dùng thoải mái**. Sai hoàn toàn.

Spark chia 4GB đó thành nhiều vùng khác nhau, mỗi vùng dùng cho mục đích riêng.
Nếu code của bạn dùng sai vùng → OOM dù memory vẫn còn nhiều ở vùng khác.

---

## 2. Full Memory Layout

```
┌──────────────────────────────────────────────────────────────────────┐
│                    MỘT EXECUTOR PROCESS trên OS                      │
│                                                                      │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │                   JVM HEAP (4GB)                               │  │
│  │  spark.executor.memory = 4g                                   │  │
│  │                                                               │  │
│  │  ┌─────────────────────────────────────────────────────────┐  │  │
│  │  │  Reserved Memory: 300MB  (HARDCODED, không config được) │  │  │
│  │  │  → Spark internal objects, system metadata              │  │  │
│  │  │  → Không bao giờ đụng vào                               │  │  │
│  │  └─────────────────────────────────────────────────────────┘  │  │
│  │                                                               │  │
│  │  Usable Memory = 4096MB - 300MB = 3796MB                     │  │
│  │  ┌─────────────────────────────────────────────────────────┐  │  │
│  │  │                                                         │  │  │
│  │  │  spark.memory.fraction = 0.6 (default)                  │  │  │
│  │  │                                                         │  │  │
│  │  │  ┌───────────────────────────────────────────────────┐  │  │  │
│  │  │  │         UNIFIED MEMORY POOL                       │  │  │  │
│  │  │  │         3796MB × 0.6 = 2278MB ≈ 2.2GB            │  │  │  │
│  │  │  │                                                   │  │  │  │
│  │  │  │  spark.memory.storageFraction = 0.5 (default)     │  │  │  │
│  │  │  │  ┌──────────────────┬──────────────────────────┐  │  │  │  │
│  │  │  │  │  STORAGE         │  EXECUTION               │  │  │  │  │
│  │  │  │  │  2278 × 0.5      │  2278 × 0.5              │  │  │  │  │
│  │  │  │  │  = 1139MB        │  = 1139MB                │  │  │  │  │
│  │  │  │  │                  │                          │  │  │  │  │
│  │  │  │  │  df.cache()      │  shuffle, sort           │  │  │  │  │
│  │  │  │  │  df.persist()    │  join, aggregation       │  │  │  │  │
│  │  │  │  │  broadcast vars  │  hash tables             │  │  │  │  │
│  │  │  │  │                  │                          │  │  │  │  │
│  │  │  │  │  ←── có thể mượn nhau (borrow) ──→         │  │  │  │  │
│  │  │  │  └──────────────────┴──────────────────────────┘  │  │  │  │
│  │  │  └───────────────────────────────────────────────────┘  │  │  │
│  │  │                                                         │  │  │
│  │  │  USER MEMORY (phần còn lại)                             │  │  │
│  │  │  3796MB × (1 - 0.6) = 1518MB ≈ 1.5GB                  │  │  │
│  │  │  → Python objects, UDF data, Spark internal metadata    │  │  │
│  │  │  → Bạn quản lý thủ công, Spark không track             │  │  │
│  │  └─────────────────────────────────────────────────────────┘  │  │
│  └────────────────────────────────────────────────────────────────┘  │
│                                                                      │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │  OFF-HEAP (ngoài JVM)                                          │  │
│  │  spark.executor.memoryOverhead = 1g  (bạn phải set)           │  │
│  │                                                               │  │
│  │  → Python worker subprocess (PySpark dùng cái này)            │  │
│  │  → JVM metaspace, code cache, thread stacks                   │  │
│  │  → Network buffers (shuffle, broadcast)                       │  │
│  │  → Native libraries (snappy, lz4, ...)                        │  │
│  └────────────────────────────────────────────────────────────────┘  │
│                                                                      │
│  TỔNG OS MEMORY = JVM heap + Off-heap = 4GB + 1GB = 5GB             │
└──────────────────────────────────────────────────────────────────────┘
```

---

## 3. Giải thích chi tiết từng vùng

### 3.1 Reserved Memory (300MB)

```
Không config được. Hardcoded trong Spark source code.
Spark dùng để lưu internal objects khi khởi động.
Nó bị trừ đi TRƯỚC KHI tính usable memory.
```

Tại sao cần biết? Vì khi tính toán, phải trừ nó ra:
```
executor.memory = 4GB
usable = 4096 - 300 = 3796 MB  ← không phải 4096
```

---

### 3.2 Unified Memory Pool

Đây là phần quan trọng nhất. Được chia cho 2 mục đích: **Storage** và **Execution**.

**Điểm mấu chốt: chúng có thể MỢN NHAU**

```
Scenario 1: Job chạy nhiều shuffle, ít cache
  → Execution cần nhiều memory
  → Storage đang rảnh (không có gì được cache)
  → Execution mượn hết phần Storage
  → Execution có thể dùng gần hết 2.2GB

Scenario 2: Job cache nhiều DataFrames
  → Storage cần nhiều memory
  → Execution đang rảnh
  → Storage mượn phần Execution
  → Storage có thể dùng gần hết 2.2GB

Scenario 3: Cả hai đều cần nhiều
  → Storage có guaranteed minimum (storageFraction)
  → Nếu Execution cần thêm và Storage đang dùng hết
  → Spark spill Execution data xuống disk (chậm nhưng không crash)
  → Nếu Storage cần thêm nhưng Execution đang dùng hết
  → Spark evict cached data (mất cache, phải recompute)
```

**`spark.memory.storageFraction = 0.3` (project này)**

Mình chọn 0.3 thay vì default 0.5 vì:
- Job này chỉ cache `mcc_codes` (~300 rows, cực nhỏ)
- Phần lớn là join, shuffle, aggregation → cần Execution nhiều hơn
- 0.3 → Storage guaranteed 30% (~680MB), Execution guaranteed 70% (~1.6GB)

```python
# Tính toán thực tế với config hiện tại:
executor_memory = 4 * 1024          # 4096 MB
reserved        = 300               # hardcoded
usable          = 4096 - 300        # 3796 MB

unified_pool    = 3796 * 0.6        # 2278 MB (memory.fraction)
user_memory     = 3796 * 0.4        # 1518 MB

storage_min     = 2278 * 0.3        # 683 MB  (storageFraction)
execution_min   = 2278 * 0.7        # 1595 MB

# Nhưng storage có thể dùng tối đa:
storage_max     = 2278              # toàn bộ unified pool nếu execution rảnh
execution_max   = 2278              # toàn bộ unified pool nếu storage rảnh
```

---

### 3.3 User Memory

```
User Memory = usable × (1 - memory.fraction)
            = 3796 × 0.4
            = 1518 MB ≈ 1.5GB
```

Dùng cho:
- Python objects (dictionaries, lists trong UDFs)
- Spark internal metadata (task state, metrics)
- RDD lineage graph

**Hay gặp vấn đề khi:**
- Viết UDF Python phức tạp load nhiều data vào memory
- Dùng collect() trả về list lớn
- Accumulators/Broadcast variables lớn

**Fix:** Tăng `executor.memory` (User Memory sẽ tăng theo) hoặc chuyển sang Pandas UDF.

---

### 3.4 Memory Overhead (quan trọng nhất, hay bị quên)

```yaml
spark.executor.memoryOverhead: 1g
```

Đây là **off-heap memory**, nằm NGOÀI JVM. OS allocate riêng.

**PySpark dùng cái này nhiều nhất:**

```
Python code của bạn → Python process (off-heap)
                         ↕ serialize/deserialize qua socket
JVM (Spark engine)   → chạy trong heap
```

Khi bạn viết PySpark, data phải đi qua 2 processes:
1. Spark JVM process (trong heap)
2. Python worker process (trong overhead)

Nếu `memoryOverhead` quá nhỏ → Python worker process bị OS kill → executor mất → job fail với lỗi:
```
ExecutorLostFailure: Container killed by YARN for exceeding memory limits.
X of Y GB physical memory used.
```

**Rule of thumb:**
```
memoryOverhead = max(384MB, executor.memory × 0.1)

4GB executor → max(384MB, 409MB) = 409MB minimum
→ Nên set 1GB để có buffer (đặc biệt khi dùng PySpark)
```

---

## 4. Tính Executor Count cho Production

### Scenario: Node 32 cores, 128GB RAM

```
Bước 1: Tính số executors per node
  Không dùng hết 32 cores/executor
  Lý do: HDFS DataNode chỉ cho 3 concurrent read connections per executor
  → Quá nhiều cores per executor = các cores phải đợi HDFS
  → Sweet spot: 5 cores/executor
  
  Giữ lại 1 core cho OS và Hadoop NodeManager
  executors_per_node = (32 - 1) / 5 = 6 executors

Bước 2: Tính memory per executor
  Giữ lại ~5% RAM cho OS
  usable_ram = 128 × 0.95 = 121.6GB ≈ 122GB
  
  memory_per_executor = 122 / 6 = 20.3GB
  → Set executor.memory = 18g (để 2g làm overhead)
  → Set executor.memoryOverhead = 2g
  
  Check: 18 + 2 = 20GB per executor × 6 = 120GB (còn 2GB cho OS ✓)

Bước 3: Cluster 3 nodes
  Total executors = 6 × 3 = 18
  Total cores = 5 × 18 = 90

Config:
  spark.executor.cores:              "5"
  spark.executor.memory:             "18g"
  spark.executor.memoryOverhead:     "2g"
  spark.dynamicAllocation.maxExecutors: "18"
```

### Scenario: Dev machine (project này) — 4 cores, 8GB RAM

```
executors_per_node = (4 - 1) / 2 = 1 (dùng 2 cores per executor cho dev)
memory_per_executor = 8 × 0.8 / 1 = 6.4GB
→ executor.memory = 4g (để 2g cho OS và overhead)
→ executor.memoryOverhead = 1g

Hoặc chạy local[*] mode để test:
  master: local[*]  → không cần cluster, dùng toàn bộ cores của máy
```

---

## 5. Unified Memory — Borrow Logic chi tiết

Đây là cơ chế thú vị nhất của Spark memory:

```
Trạng thái ban đầu (storageFraction = 0.5):
  ┌──────────────────────────────────────────┐
  │  Storage (50%)    │  Execution (50%)     │
  │  1139MB           │  1139MB              │
  └──────────────────────────────────────────┘

Case 1: Storage cần thêm, Execution rảnh
  → Storage mượn từ Execution
  ┌──────────────────────────────────────────┐
  │  Storage (80%)         │  Exec (20%)    │
  │  1822MB                │  456MB         │
  └──────────────────────────────────────────┘

Case 2: Execution cần nhiều hơn, muốn lấy lại từ Storage
  → Execution EVICT cached data của Storage
  → Data bị evict sẽ phải recompute nếu cần lại
  → Execution lấy lại memory đó
  ┌──────────────────────────────────────────┐
  │  Storage (30%)    │  Execution (70%)     │
  │  683MB            │  1595MB              │
  └──────────────────────────────────────────┘

Case 3: Execution cần thêm, Storage đang dùng phần mình
  (storageFraction là GUARANTEED minimum cho Storage)
  → Execution KHÔNG thể lấy phần guaranteed của Storage
  → Execution phải SPILL xuống disk
  → Chậm nhưng không crash
```

**Ý nghĩa thực tế:**
- `cache()` không đảm bảo data ở trong memory mãi
- Nếu Execution cần memory và Storage đang dùng hết unified pool → cached data bị evict
- Sau khi evict, lần sau dùng lại cache đó → Spark recompute từ đầu (chậm)
- Đây là lý do tại sao cần `unpersist()` sau khi xong: giải phóng memory cho Execution

---

## 6. Off-Heap Memory của Spark (khác với memoryOverhead)

Ngoài `memoryOverhead`, Spark 3.x còn có **Spark Off-Heap**:

```yaml
spark.memory.offHeap.enabled: "true"
spark.memory.offHeap.size:    "1g"
```

Khác nhau:
```
memoryOverhead = overhead cho JVM process (Python worker, JVM native)
               → OS allocate, ngoài tầm kiểm soát của Spark

offHeap.size   = Spark tự quản lý, dùng cho một số internal operations
               → Giảm GC pressure vì không nằm trong Java heap
               → Hữu ích khi dataset rất lớn, GC pause nhiều
```

**Khi nào bật offHeap?**
- Dataset rất lớn (hàng trăm GB per job)
- GC pause cao → thấy trong Spark UI: GC time chiếm > 10% task time
- Muốn giảm JVM heap pressure

---

## 7. OOM Troubleshooting Guide

### Lỗi thường gặp và nguyên nhân

```
Lỗi 1: Container killed by YARN for exceeding memory limits
  → OS kill executor vì dùng quá memory được cấp
  → Nguyên nhân: memoryOverhead quá nhỏ
  → Fix: tăng spark.executor.memoryOverhead từ 384m → 1g → 2g

Lỗi 2: java.lang.OutOfMemoryError: Java heap space
  → JVM heap hết
  → Nguyên nhân A: shuffle data quá lớn → Execution memory không đủ
    Fix: tăng spark.memory.fraction lên 0.7 hoặc 0.75
         hoặc tăng spark.executor.memory
         hoặc tăng spark.sql.shuffle.partitions để chia nhỏ hơn
  → Nguyên nhân B: cache nhiều DF lớn
    Fix: unpersist() sau khi xong, đừng cache transactions_df
  → Nguyên nhân C: collect() trả về list quá lớn về driver
    Fix: đừng collect() dataset lớn, dùng write() thay thế

Lỗi 3: GC overhead limit exceeded
  → GC chạy liên tục nhưng không giải phóng đủ memory
  → Nguyên nhân: quá nhiều objects nhỏ trong heap
  → Fix A: tăng executor.memory
  → Fix B: bật offHeap để giảm heap pressure
  → Fix C: dùng Kryo serializer (compact hơn Java default)

Lỗi 4: Task lost, executor dead
  → Executor bị kill giữa chừng
  → Nguyên nhân: OOM hoặc network timeout
  → Kiểm tra executor logs để xác định loại OOM
```

### Quy trình debug

```
Bước 1: Xem Spark UI → Executors tab
  → Cột "Storage Memory": đang dùng bao nhiêu / total
  → Cột "Task Time" vs "GC Time": nếu GC > 10% → heap pressure

Bước 2: Xem Spark UI → Stages tab → Stage detail
  → "Shuffle Read/Write Size": nếu lớn → Execution memory cần nhiều hơn
  → "Spill (Memory)": nếu > 0 → Execution đang spill → cần thêm memory

Bước 3: Xem executor logs
  → YARN: yarn logs -applicationId <app_id>
  → Docker: docker logs spark-worker

Bước 4: Điều chỉnh theo loại lỗi (xem bảng trên)
```

---

## 8. Memory config cho từng loại job

### Job nhiều Shuffle (groupBy lớn, sort-merge join)

```yaml
# Cần Execution nhiều hơn
spark.memory.fraction:        "0.7"   # tăng unified pool
spark.memory.storageFraction: "0.2"   # giảm storage min → execution có thêm room
spark.sql.shuffle.partitions: "400"   # chia nhỏ hơn → mỗi partition ít data hơn
```

### Job nhiều Cache (reuse DataFrame nhiều lần)

```yaml
# Cần Storage nhiều hơn
spark.memory.fraction:        "0.7"
spark.memory.storageFraction: "0.6"   # tăng storage guarantee
```

### Job này (Fraud Detection Pipeline)

```yaml
# Balance: ít cache (chỉ mcc nhỏ), nhiều join + agg
spark.memory.fraction:        "0.6"   # default
spark.memory.storageFraction: "0.3"   # giảm storage → execution có thêm room
spark.executor.memory:        "4g"
spark.executor.memoryOverhead: "1g"   # PySpark cần overhead
```

---

## 9. Quick Reference

```
executor.memory = 4GB
  └── Reserved: 300MB (hardcoded)
  └── Usable: 3796MB
        ├── Unified Pool (memory.fraction=0.6): 2278MB
        │     ├── Storage min (storageFraction=0.3): 683MB  → cache, broadcast
        │     └── Execution min (1-0.3=0.7):       1595MB  → shuffle, join, sort
        │     (có thể mượn nhau, tổng = 2278MB)
        └── User Memory (1-0.6=0.4): 1518MB → Python UDFs, internal

+ memoryOverhead = 1GB (off-heap, Python worker, JVM native)
= Total OS: 5GB per executor

Công thức tính executor per node:
  executors_per_node = (total_cores - 1) / 5
  memory_per_executor = (total_ram × 0.95) / executors_per_node
  executor.memory = memory_per_executor - overhead
  executor.memoryOverhead = max(1g, executor.memory × 0.1)
```

---

*Đọc thêm: [Spark Memory Management Overview](https://spark.apache.org/docs/latest/tuning.html#memory-management-overview)*
