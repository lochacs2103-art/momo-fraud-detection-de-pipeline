# myReadme — Nháp thay đổi & quyết định của tôi

> File này ghi lại những quyết định cá nhân của tôi trong quá trình build project.
> Mỗi lần thay đổi gì sẽ ghi vào đây trước.

---

## [Session 1] — Data Cleaning Decisions cho TRANSACTIONS

### Q1: merchant_state / zip NULL + city = ONLINE
- **Quyết định:** Flag `is_online_transaction = TRUE`, fill `merchant_state = 'ONLINE'`, fill `zip = 'ONLINE'`
- **Lý do:** NULL ở đây không phải missing data mà là business meaning — giao dịch online không có địa chỉ vật lý

### Q2: errors field — tách thành boolean columns
- **Quyết định:** Explode string "Bad PIN,Insufficient Balance" thành các cột boolean riêng:
  - `error_bad_pin`
  - `error_bad_cvv`
  - `error_bad_card_number`
  - `error_bad_expiration`
  - `error_bad_zipcode`
  - `error_insufficient_balance`
  - `error_technical_glitch`
  - `has_error` = TRUE nếu bất kỳ error nào = TRUE
- **Lý do:** String không query được hiệu quả. Boolean columns cho phép GROUP BY, filter, aggregate dễ dàng

### Q3: use_chip — encode thành integer enum
- **Quyết định:** Encode thành INT:
  - `0` = SWIPE ("Swipe Transaction")
  - `1` = CHIP ("Chip Transaction")
  - `2` = ONLINE ("Online Transaction")
  - Giữ lại cột gốc `use_chip_raw` để audit
- **Lý do:** Integer nhỏ hơn string, sort/compare nhanh hơn, ML-friendly

### Q4: zip — clean thành 5-digit string
- **Quyết định:** Strip `.0`, zero-pad nếu < 5 digits. NULL → NULL (ONLINE transactions đã xử lý ở Q1)
- **58523.0** → `"58523"`, `1234.0` → `"01234"`

### Q5: amount âm — flag is_refund
- **Quyết định:** `is_refund = TRUE` khi amount < 0. Giữ nguyên amount value (không lấy abs)
- **Lý do:** Refund là business event hợp lệ, cần giữ để phân tích refund rate

### Q6: mcc không khớp mcc_codes
- **Quyết định:** mcc khớp → `mcc_description` từ lookup. mcc không khớp → `"UNKNOWN"`. NULL → NULL
- **Lý do:** Phân biệt "không có trong lookup" vs "chưa có giá trị"

### Q7: income fields (users)
- **Quyết định:** Chỉ cast, giữ nguyên giá trị

### Q8: credit_score — thêm band
- **Quyết định:** Thêm `credit_score_band`:
  - `POOR` (300–579)
  - `FAIR` (580–669)
  - `GOOD` (670–739)
  - `VERY_GOOD` (740–799)
  - `EXCEPTIONAL` (800–850)
  - `INVALID` nếu ngoài range 300–850
- Flag `is_invalid_credit_score` = TRUE nếu ngoài range

### Q9: current_age — age group thân thiện
- **Quyết định:** Thêm `age_group`:
  - `TEEN` (< 18)
  - `YOUNG_ADULT` (18–25)
  - `ADULT` (26–40)
  - `MIDDLE_AGED` (41–60)
  - `SENIOR` (61+)

### Q10: expires — parse month/year
- **Quyết định:** Chỉ parse thành `expires_month`, `expires_year`

### Q11: card_on_dark_web
- **Quyết định:** Cast sang BOOLEAN (`"Yes"` → TRUE, `"No"` → FALSE)

### Q12: account_age_months (cards)
- **Quyết định:** Tính `account_age_months` = số tháng từ `acct_open_date` đến ngày ingest
- **Lý do:** Card mới mở (< 3 tháng) là fraud risk signal

---

## Pending

- [x] Implement clean_transactions.py với tất cả changes trên
- [x] Tạo clean_users.py
- [x] Tạo clean_cards.py
- [x] Update staging Hive DDL với các columns mới (errors exploded, age_group, credit_score_band, account_age_months, is_online_transaction, is_refund, use_chip enum)
- [x] Update dbt staging models với columns mới
- [ ] Update unit tests (amount_parser đã có; thêm tests cho clean logic nếu cần)

## E2E Pipeline (Session 4)

- `make pipeline` / `scripts/run_e2e.sh` — full backfill cho static dataset
- Docker mount code vào Spark + Airflow, dbt chạy từ airflow-webserver
- Marts: `fraud_features`, `user_daily_stats`, `merchant_risk_score` ✓

---

## [Session 2] — Hỏi & Đáp về file ingestion

---

### Câu hỏi: 4 file trong ingestion/ có vai trò gì?

**`spark_session.py` — Khởi động Spark**

Đây là file đầu tiên chạy trong bất kỳ job nào. Nhiệm vụ duy nhất: tạo ra một `SparkSession` đã được configure đầy đủ.

Tại sao cần file riêng? Vì config Spark (AQE, broadcast threshold, memory fraction...) nếu viết lại ở mỗi file thì khi cần sửa phải sửa 10 chỗ. File này là điểm duy nhất chứa toàn bộ config đó.

Pattern dùng là **Singleton** — `SparkSession.builder.getOrCreate()` đảm bảo dù bạn gọi `get_spark_session()` 10 lần thì vẫn chỉ có 1 session tồn tại. Spark không cho phép 2 sessions trong cùng 1 process.

```python
# Bất kỳ job nào cũng bắt đầu như này
spark = get_spark_session("tên_job")
# ... làm việc ...
stop_spark_session(spark)  # giải phóng resources khi xong
```

---

**`schema/transactions_schema.py` — Khai báo cấu trúc data**

File này không chạy gì cả — nó chỉ định nghĩa hình dạng của data dưới dạng `StructType`.

Có 2 schema:
- `RAW_CSV_SCHEMA` — đọc từ PostgreSQL: tất cả đều là STRING. Lý do là data thô không đáng tin cậy về kiểu dữ liệu — `amount` có thể là `"$-77.00"`, `date` có thể sai format. Đọc vào String trước, xử lý sau.
- `STAGING_SCHEMA` — sau khi clean: đã có đúng kiểu (DOUBLE, TIMESTAMP, BOOLEAN...)

Tại sao không để Spark tự `inferSchema`? Spark sẽ phải đọc toàn bộ dataset một lần chỉ để đoán kiểu dữ liệu — tốn thời gian, và hay đoán sai khi có null hoặc mixed values.

---

**`base_ingester.py` — Bộ khung chung**

Đây là abstract class — không thể dùng trực tiếp, chỉ để kế thừa. Nó định nghĩa flow cố định mà mọi ingester đều phải đi qua:

```
read_source() → _add_metadata() → add_partition_cols() → _write()
```

Mỗi bước đều bắt buộc. Nếu bạn viết một ingester mới mà "quên" thêm metadata columns hay "quên" partition — không thể quên được vì base class tự làm.

Pattern này gọi là **Template Method**: base class quyết định thứ tự các bước, subclass chỉ được fill vào nội dung từng bước.

`_add_metadata()` là bước quan trọng nhất — thêm `_ingested_at`, `_source_file`, `_batch_id` vào mỗi record. Đây là audit trail, không bao giờ xóa.

---

**`jdbc_ingester.py` — Kết nối và đọc từ PostgreSQL**

File lớn nhất, làm 2 việc chính:

Phần 1: `JDBCIngester` class — xử lý vấn đề performance của JDBC.

Vấn đề: nếu đọc 10 triệu rows qua JDBC với 1 connection thì cực chậm. Giải pháp là parallel read — tìm min/max của cột `id`, chia đều thành 8 khoảng, mỗi khoảng = 1 Spark task = 1 JDBC connection chạy song song:

```
Task 1: WHERE id BETWEEN 1       AND 1250000
Task 2: WHERE id BETWEEN 1250001 AND 2500000
...
Task 8: WHERE id BETWEEN 8750001 AND 10000000
```

Có một vấn đề nhỏ: Spark JDBC yêu cầu partition column phải là số, nhưng `id` trong PostgreSQL là TEXT. Nên dùng subquery để CAST sang BIGINT trước.

Phần 2: 5 concrete classes — mỗi class cho 1 table:
- `TransactionJDBCIngester` — partition `year/month/day` từ cột `date`
- `UserJDBCIngester` — partition `created_year/created_month` từ `birth_year/birth_month`
- `CardJDBCIngester` — partition `card_brand_part/expires_year_part` từ `card_brand` và `expires`
- `MCCJDBCIngester` — không partition (chỉ 300 rows)
- `FraudLabelJDBCIngester` — không partition

Mỗi class chỉ cần trả lời 4 câu hỏi: đọc table nào, ghi vào HDFS path nào, partition theo cột nào, và thêm partition columns như thế nào. Logic còn lại đã có trong `base_ingester.py` và `JDBCIngester`.

**Tóm tắt quan hệ giữa 4 file:**
```
spark_session.py          → tạo Spark, dùng ở mọi nơi
schema/                   → khai báo cấu trúc data, không chạy gì
base_ingester.py          → flow chuẩn, abstract
jdbc_ingester.py          → kế thừa base, implement JDBC + 5 concrete ingesters
```

---

### Câu hỏi: file jdbc_ingester.py có gì đặc biệt, có điểm nào ăn điểm khi phỏng vấn không?

Có 5 điểm đặc biệt:

---

**Điểm 1 — Parallel JDBC Read (quan trọng nhất)**

Đây là thứ phân biệt junior và senior rõ nhất.

Junior sẽ viết:
```python
df = spark.read.jdbc(url=url, table="raw_transactions", ...)
```
Một connection, đọc tuần tự từng row. Dataset 13 triệu rows → mất hàng tiếng.

File này làm khác — trước khi đọc, query min/max của `id` trước:
```python
SELECT MIN(CAST(id AS BIGINT)), MAX(CAST(id AS BIGINT)) FROM raw_transactions
```
Rồi chia đều thành 8 khoảng, mỗi khoảng = 1 Spark task = 1 JDBC connection riêng biệt chạy song song. Throughput tăng 8x.

Khi phỏng vấn hỏi *"làm sao optimize JDBC ingest?"* — đây là câu trả lời chuẩn.

---

**Điểm 2 — Giải quyết vấn đề TEXT partition column**

Spark JDBC parallel read yêu cầu partition column phải là NUMERIC. Nhưng `id` trong PostgreSQL là TEXT.

File này xử lý bằng subquery — wrap table trong một query tạo thêm cột numeric:
```python
f"(SELECT *, CAST(REPLACE({col}, '-', '0') AS BIGINT) AS {col}_numeric FROM {table}) t"
```
Rồi dùng cột `_numeric` làm partition column, sau khi đọc xong thì drop cột helper đó.

Đây là một edge case thực tế mà nhiều người không nghĩ tới. Mention cái này khi phỏng vấn là điểm cộng.

---

**Điểm 3 — fetchsize**

```python
"fetchsize": str(self.db_config.get("fetch_size", 10000))
```

Default của Spark JDBC là `fetchsize=10` — nghĩa là mỗi lần PostgreSQL gửi 10 rows cho Spark. Với 13 triệu rows thì cần 1.3 triệu round-trips.

File này set `fetchsize=10000` — giảm round-trips xuống còn 1300 lần. Ít người biết config này, nhưng impact lớn.

---

**Điểm 4 — Two-level inheritance (design pattern)**

```
BaseIngester (abstract — flow chuẩn)
    └── JDBCIngester (JDBC-specific logic)
            └── TransactionJDBCIngester
            └── UserJDBCIngester
            └── CardJDBCIngester
            └── MCCJDBCIngester
            └── FraudLabelJDBCIngester
```

Không phải 5 file độc lập, không phải 1 file khổng lồ. Mỗi tầng giải quyết đúng vấn đề của nó. Đây là Template Method + inheritance hierarchy đúng chuẩn OOP trong production code.

Khi phỏng vấn hỏi *"code của bạn extensible không?"* — bạn trả lời: thêm source mới chỉ cần tạo 1 class kế thừa `JDBCIngester`, implement 4 methods, là xong. Logic parallel read, metadata, write đều có sẵn.

---

**Điểm 5 — Event time vs Ingestion time**

```python
# Dùng event time (ngày giao dịch), KHÔNG phải _loaded_at
F.to_timestamp(F.col("date"), "yyyy-MM-dd HH:mm:ss")
→ year, month, day
```

Partition theo khi giao dịch xảy ra, không phải khi data vào hệ thống. Nếu có late arriving data (transaction ngày 1 nhưng ingest ngày 3) thì vẫn vào đúng partition `day=1`, không bị lẫn vào `day=3`.

Đây là khái niệm **event time vs processing time** — câu hỏi kinh điển trong data engineering và streaming. Biết phân biệt và áp dụng đúng là điểm rất cao.

---

**Đoạn nói khi phỏng vấn:**

*"Thay vì đọc JDBC single-threaded, tôi implement parallel read bằng cách query min/max id trước, chia thành 8 partitions tương ứng với 8 JDBC connections song song. Vấn đề là Spark JDBC yêu cầu partition column numeric nhưng id là TEXT, nên tôi wrap trong subquery để cast sang BIGINT. Ngoài ra tôi tăng fetchsize từ default 10 lên 10000 để giảm round-trips. Partition trên HDFS theo event time của giao dịch chứ không phải ingestion time, để late arriving data vào đúng partition."*

---

### Câu hỏi: schema đó chỉ là schema hay làm sạch — và tại sao làm sạch chi tiết có ý nghĩa trong data engineering?

**Schema chỉ là schema, không phải làm sạch.**

`clean_transactions.py` ban đầu chỉ làm: cast type, mask card_number, parse amount. Đó là format conversion, không phải data cleaning thực sự.

**Tại sao làm sạch chi tiết có ý nghĩa trong data engineering:**

**1. Downstream không cần biết data đến từ đâu và dưới dạng gì**

ML engineer khi lấy `is_refund` thì biết ngay đây là refund — không cần viết thêm `WHERE amount < 0`. Analyst dùng `credit_score_band = 'POOR'` thay vì phải nhớ `credit_score BETWEEN 300 AND 579`. Đây gọi là semantic enrichment — thêm ý nghĩa vào data thay vì chỉ thay đổi format.

**2. Null có nghĩa gì thực sự quan trọng hơn bạn nghĩ**

`merchant_state = NULL` ở transaction có thể là:
- Online transaction (có ý nghĩa business)
- Dữ liệu thực sự bị thiếu (data quality issue)

Nếu không phân biệt 2 trường hợp này thì khi analyst filter `merchant_state IS NOT NULL` sẽ vô tình bỏ mất toàn bộ online transactions — đây là lỗi nghiêm trọng trong phân tích.

**3. Errors dạng string không query được, boolean thì query được**

```sql
-- Không làm được hiệu quả với string:
SELECT COUNT(*) FROM transactions WHERE errors LIKE '%Bad PIN%'
-- Query này không dùng index, scan toàn bộ

-- Làm được với boolean:
SELECT COUNT(*) FROM transactions WHERE error_bad_pin = TRUE
-- Cột boolean, có statistics, Parquet predicate pushdown hoạt động
```

**4. account_age_months là fraud signal thực tế**

Thẻ mới mở < 3 tháng mà đã có giao dịch lớn là pattern của stolen card fraud. Nếu không có cột này, ML model phải tự tính từ `acct_open_date` mỗi lần — tốn compute, và mỗi người tính theo cách khác nhau dẫn đến inconsistency.

**5. Một lần làm sạch, dùng mãi mãi**

Đây là nguyên tắc cốt lõi của data engineering: **clean once, serve many**. Staging layer làm sạch một lần, sau đó warehouse/dbt/ML/analyst đều dùng data đã sạch đó — không ai phải tự clean nữa.


---

## [Session 3] — Backfill vs Daily Incremental (để giảng sau)

### Câu hỏi: Trong thực tế người ta chạy pipeline theo gì?

**Có 2 scenario hoàn toàn khác nhau:**

---

### Scenario 1: Backfill lần đầu (historical data load)

Khi hệ thống mới deploy, cần xử lý toàn bộ data cũ (ví dụ 10 năm).

**Cách sai (intern hay làm):** chạy từng ngày bằng for loop bash
- 3650 ngày × 1 phút/ngày = ~60 giờ chạy tuần tự
- Nếu máy tắt → mất hết tiến độ

**Cách đúng trong production:** Airflow backfill với max_active_runs

```bash
airflow dags backfill fraud_data_pipeline \
  --start-date 2010-01-01 \
  --end-date 2019-12-31
```

Với `max_active_runs=8`: Airflow tạo 3650 DAG runs, chạy 8 ngày song song cùng lúc.
3650 / 8 = ~456 batches → nhanh hơn 8x so với tuần tự.

**Tại sao Airflow tốt hơn bash loop:**
- Retry tự động nếu 1 ngày fail (bash loop thì mất)
- Monitor được từng ngày đã xong chưa
- Có thể resume từ ngày bị fail
- Song song thật sự, không phải chờ ngày này xong mới chạy ngày kia

---

### Scenario 2: Daily incremental (production ongoing)

Sau khi backfill xong, pipeline chạy **tự động hàng ngày**:

```
Mỗi ngày 02:00 AM:
  Airflow scheduler check → trigger DAG run cho execution_date = hôm qua
  → ingest 1 ngày mới → staging → warehouse → done
  Thời gian: 10-30 phút/ngày
```

Không cần làm gì thủ công — Airflow tự schedule theo `schedule_interval="0 2 * * *"`.

---

### Cho project này (static dataset)

Dataset không có new data coming → đây là backfill scenario thuần túy.

**Cách làm trong project (đơn giản hóa):**
- Chạy bash loop nền với `nohup`
- Mỗi iteration submit 1 Spark job cho 1 ngày
- Đủ để demo end-to-end flow

**Cách nói khi phỏng vấn:**
> "Dataset này là historical data nên tôi dùng backfill approach. Trong production, tôi sẽ dùng Airflow backfill với max_active_runs=8 để chạy song song 8 ngày cùng lúc, giảm thời gian xử lý 8 lần. Sau khi backfill xong, daily incremental chạy tự động qua Airflow scheduler."

---

### Catchup=True trong Airflow DAG là gì?

```python
DAG(
    dag_id="fraud_data_pipeline",
    schedule_interval="0 2 * * *",
    catchup=True,    # ← quan trọng
    start_date=datetime(2010, 1, 1),
)
```

`catchup=True`: khi DAG được enable lần đầu, Airflow tự động tạo DAG runs cho tất cả ngày từ `start_date` đến hôm nay — đây chính là backfill tự động.

`catchup=False`: chỉ chạy từ ngày hôm nay trở đi, bỏ qua quá khứ.

Đây là lý do tại sao architecture đã set `catchup=True` từ đầu — nó handle backfill tự động mà không cần lệnh riêng.

