# Đề Xuất Bổ Sung Pipeline & Dashboard Metrics
### Healthcare Data Platform — Raw → Bronze → Silver → Gold

> **Mục đích**: Trình bày các bước và metrics **chưa có trong code hiện tại**, đề xuất bổ sung để hoàn thiện pipeline và xây dựng dashboard kiểu CDP.
> **Dữ liệu**: 4 nguồn — `patients` (55.5K), `noshows` (107K), `insurance` (1.3K), `diabetes` (2.8K)

---

## Phần 1 — Bổ Sung Tầng RAW

Code hiện tại đã làm: upload file lên MinIO, gắn `_source_file` + `load_date`, manifest track file đã ingest.

**Đề xuất thêm 3 việc:**

### 1.1. Raw Inventory Table
Mỗi lần ingest, ghi thêm 1 bản ghi vào bảng `raw_inventory` lưu trên DuckDB hoặc MongoDB:

| Trường | Ví dụ |
|---|---|
| `file_name` | `healthcare_patients.csv` |
| `ingest_timestamp` | `2024-01-15 08:30:00` |
| `row_count` | `55,500` |
| `file_size_mb` | `8.0` |
| `md5_checksum` | `a3f2...` |
| `status` | `SUCCESS` / `FAILED` |

> **Lý do**: Biết được mỗi lần chạy pipeline có bao nhiêu dòng vào, file có thay đổi không (qua checksum), dễ debug khi dữ liệu đột nhiên ít đi.

### 1.2. Schema Snapshot
Sau mỗi lần ingest, lưu danh sách cột + kiểu dữ liệu ra file JSON. Nếu lần sau schema khác (thêm/mất cột) → gửi alert.

> **Lý do**: Phát hiện sớm khi nguồn dữ liệu thay đổi cấu trúc mà không báo trước.

### 1.3. Immutable Raw Zone
Hiện tại dữ liệu raw được ghi thẳng vào bronze. Đề xuất **tách riêng raw zone** — lưu file gốc ở `s3://raw/`, không bao giờ overwrite. Bronze đọc từ raw để xử lý.

> **Lý do**: Cho phép replay lại toàn bộ pipeline từ đầu bất cứ lúc nào nếu có lỗi ở các tầng sau.

---

## Phần 2 — Bổ Sung Tầng BRONZE

Code hiện tại đã làm: định nghĩa schema, cast kiểu dữ liệu, `dropDuplicates()`, ghi parquet.

**Đề xuất thêm 5 việc:**

### 2.1. Chuẩn Hóa Tên Cột (Snake_case)
Code hiện giữ nguyên tên gốc có dấu cách và typo. Cần rename:

| Tên gốc | Tên chuẩn |
|---|---|
| `Blood Type` | `blood_type` |
| `Date of Admission` | `date_of_admission` |
| `Medical Condition` | `medical_condition` |
| `Billing Amount` | `billing_amount` |
| `Admission Type` | `admission_type` |
| `Discharge Date` | `discharge_date` |
| `Test Results` | `test_results` |
| `Hipertension` *(typo)* | `hypertension` |
| `Handcap` *(typo)* | `handicap` |
| `Date.diff` | `date_diff` |
| `PatientId` (DoubleType!) | cast sang `StringType` |

> **Lý do**: Tên cột có dấu cách gây lỗi trong SQL query; typo sẽ lan truyền lên tới dashboard.

### 2.2. Chuẩn Hóa Name Casing
Hiện chỉ có `trim()`. Cần thêm title-case normalization:
```
"Bobby JacksOn"  →  "Bobby Jackson"
"LesLie TErRy"   →  "Leslie Terry"
```
> **Lý do**: Dedup theo tên sẽ bỏ sót nếu cùng 1 người nhưng viết khác kiểu.

### 2.3. Zero-as-Null Flag (Diabetes Dataset)
Dataset `diabetes` dùng giá trị `0` để ẩn missing value:
- `Insulin = 0`: **1,330/2,768 rows (48%)** — thực tế là không đo được
- `BloodPressure = 0`: **125 rows**
- `Glucose = 0`: **18 rows**

Đề xuất thêm cột flag, **không xóa giá trị gốc**:

| Cột mới | Ý nghĩa |
|---|---|
| `insulin_is_missing` | True nếu Insulin = 0 |
| `blood_pressure_is_missing` | True nếu BloodPressure = 0 |
| `glucose_is_missing` | True nếu Glucose = 0 |

> **Lý do**: Giữ nguyên raw value, nhưng biết được đâu là missing thật sự khi lên silver/gold.

### 2.4. Quarantine Zone
Các row không pass validation (age < 0, age > 115, date không hợp lệ...) không nên bị drop silent. Đề xuất ghi vào bảng riêng `bronze_quarantine` với cột `reject_reason`.

Ví dụ trong `noshows`: có bệnh nhân **Age = 115** và **Age âm** — hiện đang lọt qua.

> **Lý do**: Không làm crash pipeline, nhưng vẫn track được dữ liệu bẩn để báo cáo.

### 2.5. Dedup Theo Natural Key
Hiện dùng `dropDuplicates()` toàn bộ cột — không phát hiện được trùng khi chỉ 1-2 cột khác nhau. Đề xuất dedup theo key có nghĩa:

| Dataset | Natural Key |
|---|---|
| `patients` | `(name_norm, date_of_admission, hospital)` |
| `noshows` | `(patient_id, appointment_id)` |
| `diabetes` | `(id)` |
| `insurance` | `(age, sex, bmi, region)` |

---

## Phần 3 — Bổ Sung Tầng SILVER

> ⚠️ **Vấn đề nghiêm trọng cần sửa trước**: Code hiện tại join `patients ↔ noshows` theo `(age, gender)` — đây là **cross join ẩn**, không có ý nghĩa business. 1 bệnh nhân 30 tuổi/nam sẽ join với **mọi** record noshows 30 tuổi/nam. Cần tách silver thành 2 domain riêng.

### 3.1. Tách Silver Thành 2 Domain Riêng

```
silver/
├── inpatient/     ← từ patients (+ enrichment)
└── outpatient/    ← từ noshows (+ enrichment)
```

`insurance` và `diabetes` dùng làm **reference/benchmark**, không force join 1-1.

### 3.2. Enrichment Fields — Inpatient Domain

| Cột mới | Công thức | Ví dụ |
|---|---|---|
| `age_group` | 0-17: Pediatric, 18-35: Young Adult, 36-55: Middle-aged, 56-70: Senior, 71+: Elderly | `"Senior"` |
| `length_of_stay` | `discharge_date - date_of_admission` (ngày) | `5` |
| `cost_per_day` | `billing_amount / length_of_stay` | `3,771` |
| `is_chronic` | `medical_condition IN ('Diabetes','Hypertension','Arthritis','Asthma')` | `True` |
| `insurance_tier` | Map provider → tier (Government/Commercial/Unknown) | `"Commercial"` |

### 3.3. Enrichment Fields — Outpatient/NoShow Domain

| Cột mới | Công thức | Ví dụ |
|---|---|---|
| `age_group` | Tương tự inpatient | `"Young Adult"` |
| `lead_time` | `appointment_day - scheduled_day` (đã có `date_diff` nhưng chưa dùng) | `7` |
| `is_high_risk_noshow` | `lead_time > 7 AND sms_received = FALSE AND scholarship = TRUE` | `True` |
| `day_of_week` | Thứ trong tuần của `appointment_day` | `"Monday"` |

### 3.4. Enrichment Fields — Diabetes Cohort

| Cột mới | Rule |
|---|---|
| `glucose_status` | Glucose < 100 → Normal, 100-125 → Prediabetes, ≥ 126 → Diabetes |
| `bmi_category` | < 18.5 → Underweight, 18.5-24.9 → Normal, 25-29.9 → Overweight, ≥ 30 → Obese |
| `diabetes_risk_tier` | Kết hợp glucose_status + outcome + BMI → Low/Medium/High |

---

## Phần 4 — Bổ Sung Tầng GOLD (Data Marts)

Code hiện chỉ có **1 KPI duy nhất**: `avg_charges + avg_billing_amount` groupBy gender. Cần xây 5 marts sau:

### Gold Mart 1: `mart_noshow_analytics`
*Phục vụ tab Engagement trên dashboard*

| Metric | Mô tả |
|---|---|
| `no_show_rate` | % Showed_up = FALSE theo tháng/neighbourhood/age_group |
| `sms_impact` | No-show rate: sms_received=TRUE vs FALSE |
| `scholarship_impact` | No-show rate: scholarship=TRUE vs FALSE |
| `avg_lead_time` | AVG(lead_time) overall và theo group |
| `lead_time_bucket` | % appointments theo 0d / 1-3d / 4-7d / 7-30d / 30d+ |
| `noshow_by_dow` | No-show rate theo thứ trong tuần |

### Gold Mart 2: `mart_patient_summary`
*Phục vụ tab Population & Engagement*

| Metric | Mô tả |
|---|---|
| `total_patients` | COUNT DISTINCT name_norm |
| `visits_per_patient` | AVG số lần vào viện |
| `avg_length_of_stay` | AVG(length_of_stay) overall + by condition |
| `avg_billing_amount` | AVG(billing_amount) overall + by condition + by hospital |
| `admission_type_mix` | % Urgent / Emergency / Elective |
| `test_result_mix` | % Normal / Abnormal / Inconclusive |
| `condition_prevalence` | % patients theo medical_condition |

### Gold Mart 3: `mart_insurance_utilization`
*Phục vụ tab Insurance*

| Metric | Mô tả |
|---|---|
| `avg_charge_by_provider` | AVG billing_amount theo Insurance Provider |
| `smoker_surcharge` | AVG charges: smoker vs non-smoker (từ insurance dataset) |
| `avg_charge_by_region` | AVG charges theo region |
| `bmi_vs_charge` | Tương quan BMI → charges theo age_group |
| `insured_rate` | % patients có insurance provider |

### Gold Mart 4: `mart_diabetes_cohort`
*Phục vụ tab Clinical*

| Metric | Mô tả |
|---|---|
| `glucose_status_distribution` | % Normal / Prediabetes / Diabetes |
| `diabetes_prevalence` | % Outcome = 1 |
| `avg_glucose_by_age_group` | AVG Glucose theo age_group |
| `high_risk_count` | COUNT patients có diabetes_risk_tier = High |
| `bmi_distribution` | Histogram BMI theo outcome |

### Gold Mart 5: `mart_data_quality`
*Phục vụ tab Data Quality trên dashboard*

| Metric | Mô tả |
|---|---|
| `ingest_success_rate` | % batches ingested thành công |
| `quarantine_rate` | % rows bị quarantine theo dataset |
| `null_rate_by_column` | % null của từng cột quan trọng |
| `duplicate_rate` | % rows là duplicate trước dedup |
| `freshness_lag_hours` | Giờ từ file source đến khi có trong gold |
| `row_count_trend` | Row count mỗi dataset theo ngày — detect volume anomaly |

### Gold Mart 6: `mart_cohort_retention`
*Phục vụ Cohort Retention chart trong Tab 5 — Segments*

Grain: 1 row = 1 bệnh nhân × cohort tháng đầu tiên khám

| Metric | Mô tả |
|---|---|
| `patient_id` | PatientId từ noshows |
| `cohort_month` | Tháng có lần khám đầu tiên (first AppointmentDay) |
| `segment` | RFM segment của bệnh nhân (Champions / Loyal / At Risk / Lost / New) |
| `retained_30d` | Boolean — có lần khám tiếp theo trong 30 ngày sau lần đầu không |
| `retained_60d` | Boolean — tương tự 60 ngày |
| `retained_90d` | Boolean — tương tự 90 ngày |
| `retention_rate_30d` | % retained_30d = TRUE trong cohort_month đó |
| `retention_rate_60d` | % retained_60d = TRUE |
| `retention_rate_90d` | % retained_90d = TRUE |

---

## Phần 5 — CDP-Style Dashboard Layout

### Tổng quan 6 tabs

```
┌─────────────────────────────────────────────────────────────────────┐
│  🏥 Healthcare Data Platform                    [Date range picker] │
├──────────┬───────────┬──────────┬───────────┬──────────┬───────────┤
│ Overview │Engagement │ Clinical │ Insurance │ Segments │Data Quality│
└──────────┴───────────┴──────────┴───────────┴──────────┴───────────┘
```

---

### Tab 1 — Overview (Population)
*Bắt chước CDP "Audience Overview"*
| Nguồn mart | Metric lấy từ mart này |
|---|---|
| `mart_patient_summary` | `total_patients`, `insured_rate`, `avg_length_of_stay`, `visits_per_patient`, `condition_prevalence`, `admission_type_mix` |
| `mart_noshow_analytics` | `no_show_rate` |
| `mart_diabetes_cohort` | `diabetes_prevalence` |

```
METRIC CARDS (4 số liệu tổng quan — Big Number in Superset):
┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
│    55,500    │ │    20.26%    │ │     78%      │ │     6.2      │
│ total_       │ │ no_show_rate │ │ insured_rate │ │avg_length_of │
│ patients     │ │              │ │              │ │   _stay      │
└──────────────┘ └──────────────┘ └──────────────┘ └──────────────┘

LINE CHART — Admission trend theo tháng
  Trục X: tháng | Trục Y: số lượt admission
  Field: visits_per_patient (mart_patient_summary, groupBy tháng)

BAR CHART — Phân bố age_group
  Trục X: age_group (Pediatric/Young Adult/Middle-aged/Senior/Elderly)
  Trục Y: số bệnh nhân | Field: enrichment field từ Silver

TREEMAP — condition_prevalence
  6 ô: Cancer/Obesity/Diabetes/Asthma/Hypertension/Arthritis
  Kích thước ô = % patients | Field: condition_prevalence (mart_patient_summary)

DONUT — test_result_mix
  3 phần: Normal / Abnormal / Inconclusive
  Field: test_result_mix (mart_patient_summary)
```
*Đã bỏ: Gender split (ít giá trị), admission_type_mix (chuyển về Tab 3)*

---

### Tab 2 — Engagement (Appointments & No-Show)
*Bắt chước CDP "Engagement & Retention"* | Nguồn: `mart_noshow_analytics`

```
METRIC CARDS (3 số liệu):
┌──────────────┐ ┌──────────────┐ ┌──────────────┐
│    20.26%    │ │   7.2 ngày   │ │    35%       │
│ no_show_rate │ │ avg_lead_time│ │ sms no-show  │
│  (overall)   │ │   (overall)  │ │ vs 20% w/SMS │
└──────────────┘ └──────────────┘ └──────────────┘

LINE CHART — no_show_rate trend theo tháng
  Trục X: tháng | Trục Y: % no-show | Đường baseline: 20.26%
  Field: no_show_rate (mart_noshow_analytics)

HEATMAP — no_show_rate theo Neighbourhood
  81 ô vuông, màu đậm = no-show cao
  Field: no_show_rate groupBy neighbourhood

GROUPED BAR — Tác động của SMS & Scholarship (gộp 2 chart cũ)
  Trục X: nhóm (SMS received / Scholarship)
  Trục Y: % no-show | 2 cột mỗi nhóm: TRUE vs FALSE
  Field: sms_impact, scholarship_impact (mart_noshow_analytics)

HORIZONTAL BAR — lead_time_bucket
  5 nhóm: 0d / 1-3d / 4-7d / 7-30d / 30d+
  Trục X: % appointments | Field: lead_time_bucket

BAR CHART — noshow_by_dow (7 thứ trong tuần)
  Trục X: thứ (Mon→Sun) | Trục Y: % no-show
  Field: noshow_by_dow (mart_noshow_analytics)
```
*Đã bỏ: Scatter avg_lead_time vs no_show_rate (quá kỹ thuật, khó đọc nhanh)*

---

### Tab 3 — Clinical Cohorts
*Bắt chước CDP "Segments"*
| Nguồn mart | Metric lấy từ mart này |
|---|---|
| `mart_patient_summary` | `condition_prevalence`, `admission_type_mix`, `avg_billing_amount`, `avg_length_of_stay`, `test_result_mix` |
| `mart_diabetes_cohort` | `glucose_status_distribution`, `avg_glucose_by_age_group`, `high_risk_count`, `diabetes_prevalence` |

```
METRIC CARDS (2 số liệu):
┌──────────────────────┐  ┌──────────────────────┐
│ high_risk_count      │  │ diabetes_prevalence   │
│ (mart_diabetes_cohort│  │ (mart_diabetes_cohort)│
└──────────────────────┘  └──────────────────────┘

TREEMAP — condition_prevalence
  6 ô: Cancer/Obesity/Diabetes/Asthma/Hypertension/Arthritis
  Field: condition_prevalence (mart_patient_summary)

STACKED BAR — admission_type_mix theo condition
  Trục X: medical_condition | Trục Y: % | Màu: Urgent/Emergency/Elective
  Field: admission_type_mix (mart_patient_summary)

DONUT — glucose_status_distribution
  3 phần: Normal (34%) / Prediabetes (31%) / Diabetes (35%)
  Field: glucose_status_distribution (mart_diabetes_cohort)

DUAL-AXIS BAR — avg_billing_amount + avg_length_of_stay theo condition
  Trục X: condition | Trục Y trái: $ billing | Trục Y phải: ngày LOS
  Field: avg_billing_amount, avg_length_of_stay (mart_patient_summary)

BAR CHART — avg_glucose_by_age_group
  Trục X: age_group | Trục Y: AVG Glucose (mg/dL)
  Field: avg_glucose_by_age_group (mart_diabetes_cohort)
```
*Đã bỏ: bmi_distribution (quá chi tiết, drilldown), test_result_mix (đã có ở Tab 1)*
*Đã thêm: admission_type_mix (chuyển từ Tab 1)*

---

### Tab 4 — Insurance & Cost
| Nguồn mart | Metric lấy từ mart này |
|---|---|
| `mart_insurance_utilization` | `insured_rate`, `smoker_surcharge`, `avg_charge_by_region`, `avg_charge_by_provider`, `bmi_vs_charge` |
| `mart_patient_summary` | `avg_billing_amount` |

```
METRIC CARDS (4 số liệu — Big Number in Superset):
┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
│   insured_rate  │ │smoker_surcharge │ │avg_charge_by_   │ │ avg_billing_    │
│  (mart_insur.)  │ │  (mart_insur.)  │ │ region (top)    │ │ amount overall  │
│      78%        │ │ $32,050 vs      │ │ (mart_insur.)   │ │ (mart_patient_  │
│                 │ │ $8,440 non-smkr │ │                 │ │  summary)       │
└─────────────────┘ └─────────────────┘ └─────────────────┘ └─────────────────┘

HORIZONTAL BAR — avg_charge_by_provider
  Trục Y: Insurance Provider | Trục X: $ avg charge
  Field: avg_charge_by_provider (mart_insurance_utilization)

BAR CHART — avg_charge_by_region (4 regions: SW/SE/NW/NE)
  Trục X: region | Trục Y: $ avg charge
  Field: avg_charge_by_region (mart_insurance_utilization)

SCATTER PLOT — bmi_vs_charge
  Trục X: BMI | Trục Y: charges ($) | Màu chấm: smoker=yes (đỏ) / smoker=no (xanh)
  Field: bmi_vs_charge (mart_insurance_utilization)

DONUT — insured_rate
  % patients theo Insurance Provider (Blue Cross/Medicare/Aetna…)
  Field: insured_rate (mart_insurance_utilization)
```
*Đã bỏ: BAR avg_billing_amount theo hospital (quá chi tiết, drilldown)*

---

### Tab 5 — Patient Segments (CDP-like)
| Nguồn | Field | Segment sử dụng field này |
|---|---|---|
| `mart_noshow_analytics` | `no_show_rate` | RFM — đo độ tin cậy |
| `mart_noshow_analytics` | `avg_lead_time` | RFM — recency proxy |
| `mart_noshow_analytics` | `is_high_risk_noshow` | Clinical Risk: High No-Show Risk |
| `mart_patient_summary` | `visits_per_patient` | RFM — frequency |
| `mart_diabetes_cohort` | `diabetes_risk_tier` | Clinical Risk: Diabetes High Risk |
| `mart_diabetes_cohort` | `high_risk_count` | Metric Card tổng hợp |
| Silver `inpatient/` | `is_chronic` | Clinical Risk: Chronic + Uninsured |
| Silver `inpatient/` | `insurance_provider` | Clinical Risk: Chronic + Uninsured |
| `mart_cohort_retention` | `retention_rate_30d`, `retention_rate_60d`, `retention_rate_90d` | Cohort Retention chart |

```
GROUPED BAR CHART — RFM Segment size
  Trục X: segment (Champions/Loyal/At Risk/Lost/New) | Trục Y: số bệnh nhân
  Định nghĩa segment:
  ┌─────────────┬────────────────────┬─────────────────────┬──────────────────┐
  │ Segment     │ visits_per_patient │ avg_lead_time        │ no_show_rate     │
  ├─────────────┼────────────────────┼─────────────────────┼──────────────────┤
  │ Champions   │ Cao                │ ≤ 30 ngày           │ Thấp             │
  │ Loyal       │ Cao                │ 30–90 ngày          │ Trung bình       │
  │ At Risk     │ Trung bình         │ > 90 ngày           │ Tăng             │
  │ Lost        │ Thấp               │ > 180 ngày          │ Cao              │
  │ New         │ = 1                │ < 30 ngày           │ Chưa có          │
  └─────────────┴────────────────────┴─────────────────────┴──────────────────┘

CLINICAL RISK SEGMENTS — 3 METRIC CARDs:
┌───────────────────────────┐  ┌──────────────────────────┐  ┌──────────────────────────┐
│ [High No-Show Risk]       │  │ [Chronic + Uninsured]    │  │ [Diabetes High Risk]     │
│ Field: is_high_risk_      │  │ Field: is_chronic=TRUE   │  │ Field: diabetes_risk_    │
│ noshow = TRUE             │  │ AND insurance_provider   │  │ tier = 'High'            │
│ Source: mart_noshow_      │  │ IS NULL                  │  │ Source: mart_diabetes_   │
│ analytics                 │  │ Source: Silver inpatient │  │ cohort                   │
└───────────────────────────┘  └──────────────────────────┘  └──────────────────────────┘

GROUPED BAR CHART — Cohort Retention
  Trục X: mốc thời gian (30d / 60d / 90d)
  Trục Y: % bệnh nhân quay lại (retention_rate_30d / 60d / 90d)
  Mỗi nhóm cột = 1 RFM segment (5 màu khác nhau)
  Field: retention_rate_30d, retention_rate_60d, retention_rate_90d (mart_cohort_retention)
  Ví dụ đọc: "Champions: 85% quay lại trong 30d"
             "At Risk: 30% quay lại trong 30d → cần can thiệp"
```

---

### Tab 6 — Data Quality
*Nguồn: `mart_data_quality` (tất cả charts trong tab này)*

```
SCORE CARD — Data Quality Score tổng hợp (0–100)
  Hiển thị số to ở giữa, màu xanh/vàng/đỏ theo ngưỡng
  Tính từ: ingest_success_rate × 0.3 + (1-quarantine_rate) × 0.3
           + (1-null_rate) × 0.2 + (1-duplicate_rate) × 0.2
  Field: tổng hợp từ mart_data_quality

METRIC CARDS (4 số liệu — Big Number in Superset):
┌──────────────────┐ ┌──────────────────┐ ┌──────────────────┐ ┌──────────────────┐
│ ingest_success_  │ │ quarantine_rate  │ │null_rate_by_col. │ │freshness_lag_hrs │
│ rate: 98%        │ │ 2%               │ │ 0.3%             │ │ 2h               │
│ mart_data_quality│ │ mart_data_quality│ │ mart_data_quality│ │ mart_data_quality│
└──────────────────┘ └──────────────────┘ └──────────────────┘ └──────────────────┘

LINE CHART — row_count_trend theo ngày × dataset
  Trục X: ngày | Trục Y: số rows
  4 đường = 4 datasets (patients / noshows / insurance / diabetes)
  Alert visual nếu row count giảm > 20% so với ngày trước
  Field: row_count_trend (mart_data_quality)

TABLE — Quarantine reject reasons
  Cột: dataset | reject_reason | count | % total rows
  Sắp xếp: count giảm dần | Highlight đỏ nếu count vượt ngưỡng
  Field: quarantine_rate (mart_data_quality)

HORIZONTAL BAR — null_rate_by_column
  Trục Y: tên cột | Trục X: % null (0–100%)
  Highlight màu đỏ cột có null_rate > 5%
  Field: null_rate_by_column (mart_data_quality)

METRIC CARD — duplicate_rate
  Hiển thị 1 số: % rows là exact duplicate trước khi dedup, per dataset
  Field: duplicate_rate (mart_data_quality)
```
*Đã bỏ: TIMELINE schema change events (Raw layer chưa build)*

## Phần 6 — Tóm Tắt Việc Cần Làm Cho Team

| # | Việc | Layer | Impact |
|---|---|---|---|
| 1 | **Sửa join logic** silver (tách 2 domain thay vì force join) | Silver | 🔴 Critical bug |
| 2 | **Rename cột** snake_case + fix typo (`Hipertension`, `Handcap`) | Bronze | 🔴 High |
| 3 | **Cast PatientId** DoubleType → StringType | Bronze | 🔴 High |
| 4 | **Thêm enrichment**: `age_group`, `is_chronic`, `length_of_stay`, `lead_time`, `glucose_status`, `bmi_category` | Silver | 🟡 High |
| 5 | **Thêm quarantine zone** cho rows fail validation | Bronze | 🟡 High |
| 6 | **Build mart_noshow_analytics** | Gold | 🟡 High |
| 7 | **Build mart_patient_summary** | Gold | 🟡 High |
| 8 | **DuckDB đọc từ gold** thay vì silver | Serving | 🟡 High |
| 9 | **Build mart_data_quality** | Gold | 🟢 Medium |
| 10 | **Build mart_diabetes_cohort** | Gold | 🟢 Medium |
| 11 | **Build mart_insurance_utilization** | Gold | 🟢 Medium |
| 12 | **Build mart_cohort_retention** (retained_30/60/90d per patient) | Gold | 🟢 Medium |
| 13 | **Zero-as-null flags** cho diabetes | Bronze | 🟢 Medium |
| 14 | **Raw Inventory table** (row count, checksum) | Raw | 🟢 Medium |
| 15 | **Schema snapshot** & alert on drift | Raw | 🔵 Low |
| 16 | **Name normalization** title-case | Bronze | 🔵 Low |
| 17 | **Segment tags** (RFM, clinical risk) | Gold | 🔵 Low |
