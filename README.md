# 📊 Polyglot Persistence Analytics System — Walmart UK

## 🚀 Project Overview
This project implements a **production-grade polyglot persistence architecture** to address Walmart UK's **£450M annual customer churn problem** using advanced data engineering and analytics.

By combining **SQL Server (structured transactional data)** with **MongoDB (unstructured sentiment data)**, the system demonstrates how modern retail analytics bridges the gap between *what customers do* and *why they do it*.

### 🔑 Key Innovation — Common Identifiers
The architecture leverages:
- `ProductID`
- `CustomerID`

These act as **shared keys across SQL Server and MongoDB**, enabling:
- Cross-database analytics  
- Seamless joins without duplication  
- Unified customer + product insights  

---

## 🎯 Business Problem

### ❗ Challenge
- **68% annual churn rate**
- **£450M revenue loss**

### 🔍 Root Cause
A disconnect between:
- **Transactional data (SQL)** → *What customers buy*  
- **Sentiment data (MongoDB)** → *Why they stay or leave*

### 💡 Solution
A **polyglot persistence system** combining:

| System        | Purpose |
|---------------|--------|
| **SQL Server** | Orders, inventory, financial transactions (ACID compliance) |
| **MongoDB**   | Reviews, ratings, sentiment (flexible schema) |

### 📈 Business Outcome
- Early identification of **at-risk products**
- Proactive quality improvements  
- Prevention of revenue loss before impact  

---

## 🏗️ System Architecture

### 🧱 Data Model

```
SQL SERVER (ACID)
────────────────────────────────────────────
Customer ─┐
Store     ├──► Order ◄── OrderLine ──► Inventory
Supplier  ┘
Product ────────────────────────────┘

             │
             ▼
Common Identifiers: ProductID, CustomerID
             │
             ▼

MONGODB (Flexible Schema)
────────────────────────────────────────────
Product_catalogue
 ├─ ProductID
 ├─ images[]
 ├─ specifications{}
 └─ nutritional_info{}

Customer_reviews
 ├─ ProductID
 ├─ CustomerID
 ├─ rating (1–5)
 ├─ sentiment
 └─ review_text
```

---

## 🧰 Technology Stack

| Layer            | Technology                | Purpose |
|------------------|--------------------------|--------|
| **Frontend**     | Dash + Plotly            | Interactive analytics dashboard |
| **Backend**      | Python 3.9+              | Data processing & business logic |
| **OLTP Database**| SQL Server               | Structured transactional data |
| **Document DB**  | MongoDB                  | Unstructured sentiment data |
| **Integration**  | pyodbc + pymongo         | Database connectivity |
| **UI Styling**   | Dash Bootstrap Components| Responsive design |

---

## ✨ Key Features

### 📊 1. Real-Time KPI Dashboard

| KPI | Value | Change |
|-----|------|--------|
| 💰 Total Revenue | £84.0K | ▼ 12.8% |
| 🛒 Avg Basket Size | £339 | ▼ 12.1% |
| 🔁 Repeat Customer Rate | 8.9% | ▼ 13.8% |
| ⭐ NPS Score | 45 | ➖ 0.0% |

**Includes:**
- Large KPI cards  
- Trend sparklines  
- Period-over-period comparison  
- Color-coded performance indicators  

---

### 📦 2. Product Performance Matrix

**Polyglot Insight Engine:**  
Combines SQL revenue + MongoDB sentiment

```
          High Satisfaction
              ▲
              │   💎 Hidden Gems     ✅ Winners
              │
              │
              │   ❌ Laggards        ⚠️ At Risk
              └──────────────────────────────► Revenue
```

**Business Value:**
- ⚠️ *At Risk*: High revenue + low rating → Immediate intervention  
- 💎 *Hidden Gems*: Low revenue + high rating → Boost marketing  
- ✅ *Winners*: Maintain inventory & promotions  

---

### 🌍 3. Regional Performance Analysis

- Revenue by region (SQL aggregation)
- Average basket size insights  

**Insight:**  
North East leads in revenue (£13,943) but declining basket value → acquisition without retention

---

### 💬 4. Sentiment Analysis (MongoDB)

| Sentiment | % | Reviews |
|----------|---|--------|
| Positive | 65.8% | 169 |
| Negative | 21.0% | 54 |
| Neutral  | 13.2% | 34 |

**Key Insight:**
Sentiment predicts revenue changes **2–3 weeks in advance** (correlation: 0.73)

---

### 📊 5. Category Performance Scorecard

Combines:
- Revenue (SQL)
- Satisfaction (MongoDB)

**Color Logic:**
- 🟢 ≥ 4.0 → Strong  
- 🟡 3.0–3.9 → Neutral  
- 🔴 < 3.0 → At Risk  

**Finding:**
Electronics → Highest revenue (£38,887) but lowest satisfaction (3.1★)

---

### 📈 6. Revenue Trend Analysis

- Monthly revenue (SQL)  
- Average rating (MongoDB)  

**Includes:**
- 3-month moving average  
- Long-term benchmark  
- Dual-axis visualization  

---

## 🧠 Key Technical Highlights

- 🔗 Cross-database joins using shared identifiers  
- ⚡ Real-time analytics pipeline  
- 🧩 Polyglot persistence architecture  
- 📊 Business-ready dashboards  
- 📉 Predictive insight from sentiment data  

---

## 💼 Why This Project Stands Out

This project demonstrates:
- End-to-end data engineering + analytics capability  
- Real-world business impact (£450M problem)  
- Advanced architecture (SQL + NoSQL integration)  
- Strong product thinking  

---

## 📌 Future Enhancements

- ML-based churn prediction model  
- Real-time streaming (Kafka / Spark)  
- Customer segmentation  
- Recommendation systems  

---

## 📎 Author

Ashish Siwach  
MSc Business Analytics — University of Exeter  
