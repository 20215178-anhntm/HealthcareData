# Healthcare Dashboard — KPI Plots

4 plot quan trọng nhất, tất cả đọc từ Gold layer — dashboard load nhanh không cần scan Silver.

| Plot | Gold table | GroupBy | Aggregate |
|---|---|---|---|
| Plot 1 | `kpi_cost_by_condition` | `medical_condition` | `avg(billing_amount)`, `count(*)` |
| Plot 2 | `kpi_cost_by_gender` | `gender` | `avg(billing_amount)`, `avg(insurance_charge)` |
| Plot 3 | `kpi_test_results_by_condition` | `medical_condition`, `test_results` | `count(*)` |
| Plot 4 | `kpi_diabetes_by_age_group` | `age_group` (bin từ `age`) | `avg(diabetes_positive_rate)`, `avg(glucose)` |

---

## Plot 1 — Chi phí điều trị trung bình theo bệnh

> Đọc từ: **Gold** (`s3a://gold/kpi_cost_by_condition/`)

| Thuộc tính | Giá trị |
|---|---|
| **Loại chart** | Horizontal Bar Chart |
| **Tên biểu đồ** | Average Treatment Cost by Medical Condition |
| **Trục X** | `avg_treatment_cost` — đơn vị USD |
| **Trục Y** | `medical_condition` |

**Cách vẽ:**
```python
gold = spark.read.format("delta").load("s3a://gold/kpi_cost_by_condition/")
gold.orderBy(desc("avg_treatment_cost")).toPandas().plot.barh(...)
```

**Insight (data thật):**

| medical_condition | avg_treatment_cost | patient_count |
|---|---|---|
| Obesity | $25,859 | 18,254 |
| Diabetes | $25,714 | 18,394 |
| Asthma | $25,685 | 18,154 |
| Hypertension | $25,560 | 18,262 |
| Arthritis | $25,543 | 18,414 |
| Cancer | $25,206 | 18,242 |

Obesity tốn chi phí cao nhất, Cancer thấp nhất, chênh lệch ~$650. Số bệnh nhân giữa các bệnh khá đều (~18K) → data có thể là synthetic.

---

## Plot 2 — Chi phí điều trị theo giới tính

> Đọc từ: **Gold** (`s3a://gold/kpi_cost_by_gender/`)

| Thuộc tính | Giá trị |
|---|---|
| **Loại chart** | Grouped Bar Chart |
| **Tên biểu đồ** | Treatment & Insurance Cost by Gender |
| **Trục X** | `gender` |
| **Trục Y** | `avg_treatment_cost` và `avg_insurance_charges` — đơn vị USD |

**Cách vẽ:**
```python
gold = spark.read.format("delta").load("s3a://gold/kpi_cost_by_gender/")
gold.toPandas().plot.bar(x="gender", y=["avg_treatment_cost", "avg_insurance_charges"])
```

**Insight (data thật):**

| gender | avg_treatment_cost | avg_insurance_charges |
|---|---|---|
| male | $25,659 | $14,561 |
| female | $25,530 | $12,913 |

Chi phí điều trị giữa male/female chênh nhau chỉ $130, nhưng **chi phí bảo hiểm chênh $1,648** — nam đóng nhiều bảo hiểm hơn nữ ~13%. Đây là insight đáng chú ý cho equity analysis.

---

## Plot 3 — Kết quả xét nghiệm theo bệnh

> Đọc từ: **Gold** (`s3a://gold/kpi_test_results_by_condition/`)

| Thuộc tính | Giá trị |
|---|---|
| **Loại chart** | Stacked Bar Chart |
| **Tên biểu đồ** | Test Results Distribution by Medical Condition |
| **Trục X** | `medical_condition` |
| **Trục Y** | Tỉ lệ phần trăm (%) |
| **Màu sắc** | `test_results`: Normal / Abnormal / Inconclusive |

**Cách vẽ:**
```python
gold = spark.read.format("delta").load("s3a://gold/kpi_test_results_by_condition/")
pivot = gold.toPandas().pivot(index="medical_condition", columns="test_results", values="count")
pivot.div(pivot.sum(axis=1), axis=0).plot.bar(stacked=True)
```

**Insight (data thật):** Cần plot ra để xem bệnh nào có tỉ lệ Abnormal cao nhất. Sơ bộ Arthritis có 6,306 Abnormal (cao nhất), Cancer có 6,022 Normal (cao tương đối). Phục vụ Clinical QA — bệnh có tỉ lệ Abnormal cao bất thường cần review lại quy trình chẩn đoán.

---

## Plot 4 — Glucose và tỉ lệ tiểu đường theo nhóm tuổi

> Đọc từ: **Gold** (`s3a://gold/kpi_diabetes_by_age_group/`)

| Thuộc tính | Giá trị |
|---|---|
| **Loại chart** | Dual-axis Line + Bar Chart |
| **Tên biểu đồ** | Diabetes Risk Profile by Age Group |
| **Trục X** | `age_group` (13–29 / 30–44 / 45–59 / 60–74 / 75–89) |
| **Trục Y trái** | `diabetes_positive_rate` — Bar |
| **Trục Y phải** | `avg_glucose` — Line |

**Cách vẽ:**
```python
gold = spark.read.format("delta").load("s3a://gold/kpi_diabetes_by_age_group/")
gold.orderBy("age_group").toPandas()  # plot dual-axis
```

**Insight (data thật):**

| age_group | diabetes_positive_rate | avg_glucose | patient_count |
|---|---|---|---|
| 13–29 | 23.1% | 116.3 | 19,178 |
| 30–44 | **49.6%** | 126.7 | 24,212 |
| 45–59 | **54.4%** | 135.2 | 24,514 |
| 60–74 | 28.6% | 135.0 | 24,054 |
| 75–89 | 0.0% | 134.0 | 17,762 |

**Insight quan trọng:** Nhóm **45–59 tuổi có tỉ lệ tiểu đường cao nhất (54.4%)**, kèm theo glucose trung bình cũng cao nhất (135). Nhóm 75–89 có 0% là do dataset diabetes (Pima Indians) không có data người trên 75 tuổi → enrichment không khớp. Phục vụ chương trình tầm soát target nhóm 30–59 tuổi.
