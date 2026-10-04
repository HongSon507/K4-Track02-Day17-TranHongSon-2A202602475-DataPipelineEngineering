# K4-Track02-Day17 — Report cá nhân

Phần phân tích tối đa một trang, không tính output ở phần 5.
Định dạng tham chiếu và phạm vi tính trang: [SUBMISSION.md](../docs/SUBMISSION.md).

**Họ tên / MSSV:** Trần Hồng Sơn / 2A202602475
**Repo:** https://github.com/HongSon507/K4-Track02-Day17-TranHongSon-2A202602475-DataPipelineEngineering.git
**Commit bài nộp:** `fbf74549564cf474f0b59b1452db9dd4b07cd131`
**AI đã dùng và phạm vi hỗ trợ (hoặc không dùng):** Antigravity (Gemini) — Hỗ trợ đọc codebase, xác định nguyên nhân 3 lỗi, hướng dẫn sửa đổi file Python/SQL, chạy kiểm thử pipeline (verify, test, rerun, parity) và hỗ trợ viết REPORT.md.
**Nguồn tham khảo khác (nếu có):** Slide bài giảng K4-Track02-Day17, tài liệu DuckDB & dbt documentation.

## 1. Ba lỗi

Mỗi lỗi 4 dòng. Triệu chứng = thứ bạn *thấy* đầu tiên (check nào fail, số nào lạ,
checksum nào lệch) — không phải cách sửa.

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | `silver_tickets` có 24 dòng cho 12 tickets (`assert 24 == 12`); T-91 cập nhật nhiều lần bị trùng 3 dòng thay vì giữ trạng thái mới nhất; `gold_doc_chunks` bị nhân đôi (22 rows / 9 chunks). | `LOOKBACK_DAYS=0 < 3`; `u05` có sự kiện ngày 08-12 (đến ngày 08-15) chỉ đếm (2, 0) thay vì (5, 1); checksum `gold_feature_daily` lệch full recompute (`c50b8851affe != 8630e04a61d1`). | T-97 đã bị xoá trong Postgres nhưng `silver_tickets` vẫn còn nguyên dữ liệu PII của u06 (`is_deleted = False`); T-97 rò rỉ vào `gold_training_set` (1 row) và `gold_doc_chunks` (2 chunks). |
| **Nguyên nhân gốc** | Dùng lệnh `INSERT INTO` thô nên khi replay hoặc có CDC update sẽ chèn thêm bản ghi thay vì thay thế. | Sự kiện (event) có lúc đến trễ qua Kafka (lateness lên tới 3 ngày), nhưng cấu hình `LOOKBACK_DAYS = 0` nên batch chạy hàng ngày không update lại số liệu của 3 ngày trước. | Debezium delete (`op='d'`) set payload `after=null`, nên code tách `ticket_id` từ `after` trả về `null`. Lệnh lọc `WHERE ticket_id IS NOT NULL` đã vứt mất CDC delete. |
| **Cách sửa** (file, vài dòng) | Đổi `INSERT INTO` thành `MERGE INTO` trong `pipeline/silver.py:upsert_silver_tickets`, update khi `s._lsn > t._lsn`. | Đổi `LOOKBACK_DAYS = 3` (bằng `ceil(P99)`) trong `pipeline/config.py` để ghi đè (overwrite-partition) lại khoảng thời gian bị trễ. | Đổi cách lấy `ticket_id` thành `COALESCE(after->>'ticket_id', before->>'ticket_id')` trong `pipeline/staging.py:ticket_changes_sql`. |
| **Khái niệm trên slide** | Idempotency / 1 hàng = 1 thực thể (Silver) / SCD Type 1 | Late data / Event time vs Ingest time / Overwrite partition / Lookback window | CDC Delete / Tombstone / Snapshot bất biến |

## 2. Các con số

- P99 lateness đo từ Bronze: `3.00` ngày (P50: 0.00, P95: 2.90, max: 3) → `LOOKBACK_DAYS = 3`
- `submission/checksums.txt`: PASS — Gold checksum: `39e115c510ecdf526800eac227158a4f`
- `make parity`: PARITY

## 3. Lựa chọn công cụ / kỹ thuật (mỗi dòng một câu "vì sao")

- MERGE theo khoá cho `silver_tickets`, overwrite-partition cho `gold_feature_daily`: MERGE theo khoá `ticket_id` đảm bảo tính idempotent và duy trì trạng thái mới nhất duy nhất (SCD Type 1), còn overwrite-partition cho Gold cho phép cập nhật lại chính xác các ngày bị ảnh hưởng bởi late data mà không cần tính toán lại toàn bộ lịch sử.
- Tombstone thay vì xoá hẳn hàng trong Silver: Giữ lại tombstone (`is_deleted = True`) để truyền vết xoá (CDC delete) chính xác xuống các bảng downstream và audit, đồng thời null-out dữ liệu PII để đảm bảo quyền riêng tư mà không làm đứt gãy tính toàn vẹn dữ liệu.
- Snapshot training dựng lại từ Bronze "as of" ngày đó, không sửa snapshot cũ: Đảm bảo tính nhất quán và khả năng lặp lại (reproducibility) của mô hình ML theo mốc thời gian (point-in-time correctness), tránh làm làm thay đổi dữ liệu huấn luyện lịch sử khi có cập nhật hoặc feedback đến muộn.
- DuckDB (lite) / dbt (track dbt) cho bài toán cỡ này, chứ không phải Spark: Dữ liệu đơn máy xử lý cực nhanh bằng DuckDB embedded mà không gặp chi phí quản lý cluster như Spark; dbt giúp chuẩn hóa mô hình dữ liệu, quản lý DAG và tự động hóa kiểm thử schema/data.

## 4. Hai câu hỏi suy ngẫm

1. Snapshot `v2026-08-12`..`v2026-08-14` vẫn chứa văn bản của T-97 (đã bị xoá ngày 08-15). "Snapshot bất biến" và "quyền được xoá dữ liệu" mâu thuẫn — bạn xử lý thế nào?
   - **Trả lời:** Trong tuân thủ pháp lý (GDPR/CCPA), "quyền được xóa dữ liệu" có ưu tiên cao hơn nguyên tắc bất biến kỹ thuật. Xử lý bằng 2 cách: (1) **Crypto-shredding**: Mã hóa văn bản PII trong snapshot bằng encryption key riêng cho từng người dùng/ticket; khi nhận yêu cầu xóa, tiêu hủy key tương ứng khiến PII trong snapshot cũ vĩnh viễn không thể giải mã mà không hỏng cấu trúc snapshot. (2) **Snapshot Mutation & Re-versioning**: Chạy quy trình purge để xóa PII trong snapshot cũ và phát hành phiên bản sửa đổi (ví dụ `v2026-08-12.1`) kèm log audit rõ ràng.

2. Regex che được email và số điện thoại, nhưng tên "Nguyễn Văn An" vẫn còn. Bạn sẽ đặt chốt PII nào, ở tầng nào, và đo nó ra sao?
   - **Trả lời:**
     - **Vị trí chốt:** Đặt tại tầng **Silver** (bước `quality/clean` ngay sau khi ingest từ Bronze) để ngăn PII rò rỉ vào Gold/Feature Store/RAG Index.
     - **Kỹ thuật:** Kết hợp mô hình **NER (Named Entity Recognition)** NLP tiếng Việt (như PhoNLP/spaCy) để nhận diện tên người trong văn bản tự do với danh sách Lookup Table (tên từ hệ thống CRM/HR).
     - **Đo lường:** Đánh giá trên tập test mẫu được gán nhãn PII thủ công thông qua các chỉ số **Precision, Recall, và F1-Score**, ưu tiên tối ưu **Recall** đạt sát 100% để triệt tiêu nguy cơ bỏ sót PII.

## 5. Output (dán nguyên văn)

```text
$ make verify
=== verify.py — Day 17 pipeline contracts ===
  [OK ] Bronze  every daily batch landed as Parquet (7 days x 3 sources)
  [OK ] Bronze  re-landing a batch is a no-op (append-only, no duplicate file)
  [OK ] Bronze  Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there
  [OK ] Silver  silver_tickets has exactly one row per ticket_id
  [OK ] Silver  T-91 shows its latest state: high / closed / bug
  [OK ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left
  [OK ] Silver  no email / phone number survives past Bronze
  [OK ] Silver  silver_events has one row per event_id (Kafka redeliveries removed)
  [OK ] Silver  2 malformed events quarantined with a reason; the run did not halt
  [OK ] Gold    gold_feature_daily reconciles with a full recompute from Silver
  [OK ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12
  [OK ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)
  [OK ] Gold    training set uses point-in-time priority (T-91 created as 'low')
  [OK ] Gold    late feedback creates a NEW snapshot version; the old one is untouched
  [OK ] Gold    latest training snapshot excludes the deleted ticket T-97
  [OK ] Gold    deletes propagate to the RAG index: no chunk of T-97
  [OK ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks
  [OK ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build

RESULT: 18/18 checks — ALL PASS

$ make test
..................................                                       [100%]
34 passed in 2.72s

$ make rerun3
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums

$ make lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3

$ make dbt
05:34:34  Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 3.44 seconds (3.44s).
05:34:34  Completed successfully
05:34:34  Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19

$ make parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```

Nếu dùng PowerShell, ghi lệnh tương đương và output thực tế theo [SUBMISSION.md](../docs/SUBMISSION.md).
Nếu làm bonus, thêm output B1 hoặc đường dẫn bằng chứng B2 ở cuối phần này.
