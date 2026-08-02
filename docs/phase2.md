# گزارش نهایی اجرای فاز دوم: Data Ingestion & Event Streaming
## سیستم کشف تقلب برخط (Real-Time Fraud Detection Lakehouse)

---

### ۱. خلاصه مدیریتی و اهداف فاز دوم (Executive Summary)
فاز دوم پروژه مسئولیت **ارسال برخط داده‌های تراکنش (Data Ingestion)** به سرویس **Apache Kafka** را با حفظ الزامات مقاله **Structured Streaming** و مقاله **Data Lakehouse** بر عهده دارد. در این مرحله، اسکریپت `scripts/02_kafka_producer.py` پیاده‌سازی شده و تمامی تست‌های عملیاتی در کانتینر داکر `spark_master_lakehouse` با موفقیت کامل اجرا گردید.

---

### ۲. مفاهیم کلیدی و انطباق با مقالات مرجع (Theoretical Alignment)

1. **ارسال داده برخط با نرخ قابل تنظیم (TPS Control):**
   - پشتیبانی دقیق از نرخ‌های **10 TPS** و **40 TPS** مطابق با متدولوژی مقاله مرجع جهت شبیه‌سازی ترافیک واقعی تراکنش‌های بانکی.
   - استفاده از تایمرهای پردقت برای حفظ نرخ ارسال بدون ایجاد Bottleneck در پرودیوسر.

2. **تزریق فیلد Wall-Clock Timestamp (`event_timestamp`):**
   - ستون `Time` در دیتاست اصلی صرفاً تعداد ثانیه‌های سپری شده از اولین تراکنش را نشان می‌دهد و جهت پردازش زمان واقعی در استریم کاربرد ندارد.
   - هنگام ارسال هر پیام به کافکا، زمان واقعی سیستم بر حسب استاندارد ISO-8601 با ساختار UTC (مانند `2026-07-26T10:28:55.612059Z`) به عنوان فیلد `event_timestamp` در پیام قرار می‌گیرد. این فیلد برای **Watermarking** و **Window Aggregation** در فاز بعد (Spark Streaming) حیاتی است.

3. **اعمال Schema Enforcement با Confluent Schema Registry:**
   - ثبت ساختار رسمی JSON Schema پیام‌ها در **Confluent Schema Registry** (`http://schema-registry:8081`).
   - تضمین عدم ورود داده‌های ناصحیح (Schema Validation) مستقیماً منعکس‌کننده مفهوم **Schema Enforcement** مطرح شده در مقاله Lakehouse است.

---

### ۳. خروجی‌های واقعی و لاگ‌های اجرا در کانتینر داکر (Docker Verification Logs)

#### اجرای تست اولیه (۱۰ رکورد جهت صحت‌سنجی اولیه):
```bash
docker exec -it spark_master_lakehouse python /app/scripts/02_kafka_producer.py --tps 10 --max_records 10
```

**خروجی متنی کانتینر:**
```log
[SUCCESS] Registered schema in Schema Registry: creditcard-transactions-value (ID: 1)
[INFO] Connecting Kafka Producer to broker: kafka:9092

================================================================================
 STARTING KAFKA STREAMING PRODUCER
 Target Topic:        creditcard-transactions
 Target Throughput:   10.0 TPS (100.00 ms / record)
 Total Records:       10
================================================================================

[PRODUCER STATUS] Sent: 1/10 | Actual Rate: 9.85 TPS | Event Time: 2026-07-26T10:27:57.078252Z

================================================================================
 PRODUCER STREAMING COMPLETED
 Sent Records: 10
 Duration:     1.00 seconds
 Average TPS:  9.99 TPS
 Saved Summary (Volume): ./data_output/phase2_kafka_producer_summary.json
 Saved Summary (Root):   ./phase2_summary.json
 Saved Log:              ./data_output/phase2_kafka_producer.log
================================================================================
```

---

#### اجرای استریم حجم بالاتر (۵۰۰ رکورد با نرخ ۱۰ TPS):
```bash
docker exec -it spark_master_lakehouse python /app/scripts/02_kafka_producer.py --tps 10 --max_records 500
```

**خروجی لاگ استریمینگ برخط:**
```log
================================================================================
 STARTING KAFKA STREAMING PRODUCER
 Target Topic:        creditcard-transactions
 Target Throughput:   10.0 TPS (100.00 ms / record)
 Total Records:       500
================================================================================

[PRODUCER STATUS] Sent: 1/500 | Actual Rate: 9.84 TPS | Event Time: 2026-07-26T10:28:55.612059Z
[PRODUCER STATUS] Sent: 20/500 | Actual Rate: 10.52 TPS | Event Time: 2026-07-26T10:28:57.512165Z
[PRODUCER STATUS] Sent: 40/500 | Actual Rate: 10.26 TPS | Event Time: 2026-07-26T10:28:59.512180Z
[PRODUCER STATUS] Sent: 60/500 | Actual Rate: 10.17 TPS | Event Time: 2026-07-26T10:29:01.512159Z
[PRODUCER STATUS] Sent: 80/500 | Actual Rate: 10.13 TPS | Event Time: 2026-07-26T10:29:03.512168Z
[PRODUCER STATUS] Sent: 100/500 | Actual Rate: 10.10 TPS | Event Time: 2026-07-26T10:29:05.512160Z
[PRODUCER STATUS] Sent: 120/500 | Actual Rate: 10.08 TPS | Event Time: 2026-07-26T10:29:07.512147Z
[PRODUCER STATUS] Sent: 140/500 | Actual Rate: 10.07 TPS | Event Time: 2026-07-26T10:29:09.512191Z
...
================================================================================
 PRODUCER STREAMING COMPLETED
 Sent Records: 500
 Duration:     50.00 seconds
 Average TPS:  10.00 TPS
 Saved Summary (Volume): ./data_output/phase2_kafka_producer_summary.json
 Saved Summary (Root):   ./phase2_summary.json
 Saved Log:              ./data_output/phase2_kafka_producer.log
================================================================================
```

---

### ۴. ساختار گزارش خلاصه‌شده فایل JSON و ماندگاری داده‌ها (Persistence & Logs)

فایل `phase2_summary.json` مستقیماً پس از اتمام استریم در دایرکتوری `./data_output/` و ریشه کانتینر ثبت می‌گردد تا امکان انتقال آسان به سیستم میزبان (Host) فراهم باشد:

```json
{
  "phase": "Phase 2 - Data Ingestion (Kafka Producer)",
  "topic": "creditcard-transactions",
  "target_tps": 10.0,
  "bootstrap_servers": "kafka:9092",
  "schema_registry_url": "http://schema-registry:8081",
  "sent_records": 500,
  "total_records": 500,
  "duration_seconds": 50.0,
  "actual_avg_tps": 10.0,
  "timestamp": "2026-07-26T10:29:45.612059Z",
  "status": "COMPLETED"
}
```

---
## معماری کافکا
┌────────────────────────┐      JSON Records        ┌────────────────────────┐
│   Credit Card Dataset  │  ─────────────────────>  │  Python Kafka Producer │
│   (data_output/*.csv)  │  + Wall-Clock Time Injec │ (02_kafka_producer.py) │
└────────────────────────┘                          └────────────────────────┘
                                                               │
                                         Register JSON Schema  │ (HTTP / POST)
                                                               v
                                                    ┌────────────────────────┐
                                                    │ Schema Registry        │
                                                    │ (schema-registry:8081) │
                                                    └────────────────────────┘
                                                               │
                                         Stream to Topic       │
                                         (10 TPS / 40 TPS)     v
                                                    ┌────────────────────────┐
                                                    │ Apache Kafka Broker    │
                                                    │ (Topic: creditcard-    │
                                                    │  transactions)         │
                                                    └────────────────────────┘
---

### ۵. نتیجه‌گیری فاز دوم
تمامی معیارهای لایه **Data Ingestion** شامل:
1. اتصال مستمر به Kafka Broker کانتینری در پورت `kafka:9092`
2. ثبت موفقیت‌آمیز اسکیما با شناسه ID: 1 در Confluent Schema Registry
3. کنترل دقیق نرخ TPS معادل ۱0.00 TPS
4. تزریق دقیق زمان برخط ISO-8601 به پیام‌ها
5. ثبت خودکار گزارش خلاصه و فایل لاگ در دایرکتوری داکر و دایرکتوری متصل به به سیستم میزبان (`data_output`)

پروژه به صورت ۱۰۰٪ عملیاتی آماده ورود به **فاز سوم (Spark Structured Streaming)** می‌باشد.
