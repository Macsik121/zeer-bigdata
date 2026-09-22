# Zeer Marketplace — Log Analytics & ML Pipeline Design

**Version:** 1.0  
**Date:** 2026-09-22  
**Author:** Data Engineering Team  
**Status:** Design Phase

---

## 1. Log Taxonomy & Schema Definitions

### 1.1 Action Logs (Web UI — User & Admin)

```json
{
  "event_id": "uuid-v4",
  "timestamp": "2026-09-22T15:30:00.123Z",
  "user_id": "user_12345",
  "session_id": "sess_abc123",
  "action_type": "enum[REGISTER|LOGIN|KEY_ACTIVATION|PASSWORD_RESET|SUBSCRIPTION_PURCHASE|UNBIND_REQUEST|PROMO_ACTIVATE|DELETE_ITEM|ADD_ITEM|USER_EDIT|PRODUCT_EDIT|LOGOUT|ADMIN_ACTION]",
  "action_category": "enum[AUTH|COMMERCE|ADMIN|ACCOUNT|SECURITY]",
  "product_id": "prod_csgo_30d",
  "product_name": "CS:GO 30 Days",
  "promo_code": "SUMMER2026",
  "key_code": "XXX-XXX-XXX",
  "admin_user_id": "admin_001",
  "target_user_id": "user_12345",
  "metadata": {
    "ip": "192.168.1.100",
    "user_agent": "Mozilla/5.0...",
    "platform": "web",
    "browser": "Chrome 118",
    "referrer": "/dashboard"
  },
  "status": "enum[SUCCESS|FAILED|PENDING]",
  "error_code": "enum[INVALID_KEY|EXPIRED|ALREADY_USED|INSUFFICIENT_FUNDS|PERMISSION_DENIED]",
  "duration_ms": 245
}
```

**Action Categories Mapping:**
| Action | Category | Business Domain |
|--------|----------|-----------------|
| Registration, Login, Logout, Password Reset | AUTH | Identity |
| Key Activation, Promo Activation, Subscription Purchase | COMMERCE | Revenue |
| Unbind Request | SECURITY | Account Security |
| Add/Delete Items, Product Edit, User Edit | ADMIN | Catalog Management |
| Admin Actions (ban, etc.) | SECURITY | Moderation |

---

### 1.2 Crash Logs (Game Client — Loader API)

```json
{
  "event_id": "uuid-v4",
  "timestamp": "2026-09-22T15:35:00.456Z",
  "user_id": "user_12345",
  "steam_id": "76561198123456789",
  "product_id": "prod_csgo_30d",
  "product_name": "CS:GO Changer",
  "exception_code": "0xC0000005",
  "exception_description": "Access violation reading location 0x00000000",
  "time_in_game_seconds": 1245,
  "full_stack_trace": "Module: client.dll\nFunction: C_BaseEntity::GetAbsOrigin\nOffset: 0x12345\nRegisters: EAX=0x00000000...",
  "game_version": "1.38.7.9",
  "cheat_version": "2.1.4",
  "os_info": {
    "name": "Windows 10 Pro",
    "version": "10.0.19045",
    "build": "22H2"
  },
  "hardware": {
    "cpu": "Intel i7-9700K",
    "gpu": "NVIDIA RTX 3070",
    "ram_gb": 32
  },
  "inject_session_id": "inj_sess_789"
}
```

**Exception Code Taxonomy (Windows):**
| Code | Name | Category | Typical Cause |
|------|------|----------|---------------|
| 0xC0000005 | EXCEPTION_ACCESS_VIOLATION | Memory | Null ptr, buffer overflow |
| 0xC000001D | EXCEPTION_ILLEGAL_INSTRUCTION | CPU | Bad opcode, DEP violation |
| 0xC00000FD | EXCEPTION_STACK_OVERFLOW | Memory | Infinite recursion |
| 0x80000003 | EXCEPTION_BREAKPOINT | Debug | Anti-debug trigger |
| 0xC0000409 | STATUS_STACK_BUFFER_OVERRUN | Security | /GS cookie check failed |

---

### 1.3 Inject Logs (DLL Injection — Loader API)

```json
{
  "event_id": "uuid-v4",
  "timestamp": "2026-09-22T15:32:00.789Z",
  "user_id": "user_12345",
  "steam_id": "76561198123456789",
  "product_id": "prod_csgo_30d",
  "product_name": "CS:GO Changer",
  "hwid": "HWID-ABC-123-DEF",
  "windows_name": "Windows 10 Pro 22H2",
  "ip": "192.168.1.100",
  "location": "Moscow, RU",
  "inject_stage": "enum[PRELOAD|INJECT|POST_INJECT|HEARTBEAT]",
  "status": "enum[SUCCESS|FAILED|BLOCKED]",
  "error_code": "enum[HWID_MISMATCH|NO_LICENSE|FROZEN|WRONG_CREDENTIALS|BANNED|ANTICHEAT_DETECTED|INJECTION_FAILED]",
  "loader_version": "3.2.1",
  "driver_version": "1.0.5",
  "anticheat_detected": ["VAC", "Vanguard", "BattlEye"],
  "injection_method": "enum[MANUAL_MAP|THREAD_HIJACK|VEH|KERNEL]",
  "duration_ms": 1250
}
```

**Inject Stage Pipeline:**
```
PRELOAD → (license check) → INJECT → (dll mapping) → POST_INJECT → HEARTBEAT (periodic)
```

---

### 1.4 Loader API → Action Log Mapping

| API Endpoint | Failure Condition | Action Log Entry |
|--------------|-------------------|------------------|
| `/inject_dll_preload` | HWID mismatch | `LOGIN_LOADER_FAILED: "Invalid HWID"` |
| `/inject_dll_preload` | No subscription | `PRODUCT_LAUNCH_FAILED: "No license for CS:GO"` |
| `/inject_dll_preload` | Frozen sub | `PRODUCT_LAUNCH_FAILED: "Subscription frozen"` |
| `/inject_dll_preload` | Wrong credentials | `LOGIN_LOADER_FAILED: "Wrong user parameters"` |
| `/inject_dll_preload` | User banned | `LOGIN_LOADER_FAILED: "User banned"` |
| `/inject_dll_preload` | Success | `PRODUCT_LAUNCH_SUCCESS: "License verified for CS:GO"` |
| `/log_inject_hacks` | Same failures | Same action logs |
| `/log_inject_hacks` | Success | **Inject Log** created |
| `/crash_logs` | Any crash | **Crash Log** created |
| `/ban_user` | Admin action | `USER_BANNED: "Blocked - [Ban]"` |

---

## 2. Analytics Use Cases by Domain

### 2.1 Business Analytics (Revenue & Growth)

| Metric | Description | Query Pattern | Frequency |
|--------|-------------|---------------|-----------|
| **DAU/MAU** | Daily/Monthly Active Users | Count distinct user_id from Action Logs (LOGIN, any action) | Daily |
| **Registration Funnel** | Register → Email Verify → First Login → First Purchase | Cohort analysis on Action Logs | Weekly |
| **Conversion Rate** | Visitors → Registered → Paying | Funnel: REGISTER → SUBSCRIPTION_PURCHASE | Daily |
| **ARPU/ARPPU** | Average Revenue Per User / Paying User | Sum purchase amounts / user counts | Monthly |
| **Churn Rate** | Users inactive > 30 days | Last action timestamp analysis | Weekly |
| **LTV Prediction** | Lifetime value per cohort | Regression on purchase history, session frequency | Monthly |
| **Promo Code Effectiveness** | Codes used, revenue attributed | GROUP BY promo_code on PROMO_ACTIVATE | Per campaign |
| **Key Activation Rate** | Keys sold vs activated | KEY_ACTIVATION / keys issued | Daily |

### 2.2 Product Analytics (Usage & Engagement)

| Metric | Description | Data Source | Frequency |
|--------|-------------|-------------|-----------|
| **Product Popularity** | Launches per product | Inject Logs (SUCCESS) + Action Logs (SUBSCRIPTION_PURCHASE) | Hourly |
| **Session Duration** | Time in game per session | Crash Logs (time_in_game_seconds) + Inject heartbeats | Per session |
| **Feature Usage** | Which cheat features used | Inject Logs (product_id variants) | Daily |
| **Platform Distribution** | Windows versions, builds | Inject Logs (windows_name) | Weekly |
| **Steam ID Linkage** | Unique steam accounts per user | Inject/Crash Logs (steam_id) | Daily |
| **HWID Diversity** | Unique HWIDs per user (account sharing detection) | Inject Logs (hwid) | Real-time alert |

### 2.3 Technical Analytics (Stability & Performance)

| Metric | Description | Data Source | Alert Threshold |
|--------|-------------|-------------|-----------------|
| **Crash Rate** | Crashes / 1000 injections | Crash Logs count / Inject Logs (SUCCESS) | > 5% |
| **Crash by Exception** | Top exception codes | Crash Logs GROUP BY exception_code | New code appears |
| **Crash by Version** | Regression detection | Crash Logs GROUP BY cheat_version, game_version | Spike after update |
| **Injection Success Rate** | Successful / attempted | Inject Logs status=SUCCESS / total | < 95% |
| **Injection Latency** | P50/P95/P99 duration_ms | Inject Logs duration_ms | P99 > 5000ms |
| **Anti-cheat Detections** | VAC/Vanguard/BattlEye flags | Inject Logs anticheat_detected | Any detection |
| **Loader Version Adoption** | Users on latest loader | Inject Logs loader_version | < 80% on latest |

### 2.4 Security & Fraud Analytics

| Use Case | Description | Data Sources | Detection Method |
|----------|-------------|--------------|------------------|
| **Account Sharing** | Multiple HWIDs per user | Inject Logs (hwid, user_id) | > 3 HWIDs/week → flag |
| **Credential Stuffing** | Failed logins from same IP | Action Logs (LOGIN_FAILED) + Inject (WRONG_CREDENTIALS) | Rate limiting + IP reputation |
| **License Abuse** | One subscription, many HWIDs | Inject Logs (HWID_MISMATCH + success) | Correlation analysis |
| **Ban Evasion** | Banned user returns | Action Logs (USER_BANNED) + new registration | Device fingerprinting |
| **Injection Anomalies** | Unusual injection patterns | Inject Logs (timing, method, stage failures) | Isolation Forest |
| **Crash Exploits** | Crafted crashes for code exec | Crash Logs (stack trace patterns) | Signature matching |

---

## 3. ML & Advanced Analytics Pipeline

### 3.1 ML Use Cases Priority Matrix

| Priority | Use Case | Type | Input Features | Target | Business Impact |
|----------|----------|------|----------------|--------|-----------------|
| **P0** | Crash Classification | Multi-class | exception_code, stack_trace_embedding, version, hw | Root cause category | Reduce MTTR 50% |
| **P0** | Injection Failure Prediction | Binary | hwid_history, license_status, os_version, av_list | Will fail? | Proactive support |
| **P1** | User Churn Prediction | Binary | action_frequency, session_duration, purchase_history, support_tickets | Churn in 30d | Retention campaigns |
| **P1** | Fraud/Account Sharing Detection | Anomaly | hwid_count, ip_diversity, geo_velocity, time_patterns | Anomaly score | Security |
| **P2** | LTV Prediction | Regression | first_purchase, frequency, product_mix, engagement | Predicted 12mo revenue | Marketing budget |
| **P2** | Product Recommendation | Collaborative | user_product_matrix, co-purchase | Next product | Upsell revenue |
| **P3** | Optimal Injection Timing | Reinforcement | game_state, anticheat_status, time_of_day | Best inject window | Success rate ↑ |

---

### 3.2 Feature Engineering Pipeline

```python
# Feature Engineering for ML Models
# Location: zeer-bigdata/analytics/features/

class FeatureEngineer:
    """Transform raw logs into ML-ready feature matrices"""
    
    # User-level features (daily snapshot)
    USER_FEATURES = [
        # Behavioral
        "actions_last_7d", "actions_last_30d",
        "unique_products_used_7d", "unique_products_used_30d",
        "total_session_time_7d", "avg_session_time",
        "injection_success_rate_7d", "crash_rate_7d",
        
        # Temporal
        "days_since_registration", "days_since_last_action",
        "activity_ratio_weekday_weekend", "preferred_hour_utc",
        
        # Commercial
        "total_spent_usd", "active_subscriptions", "subscription_tier",
        "promo_codes_used", "keys_activated",
        
        # Technical
        "primary_os", "primary_windows_build", "steam_account_age_days",
        "unique_hwids_30d", "unique_ips_30d",
        
        # Security
        "failed_login_count_7d", "hwid_mismatch_count_7d",
        "ban_history_count", "is_banned",
    ]
    
    # Session-level features (per injection)
    SESSION_FEATURES = [
        "product_id", "loader_version", "driver_version",
        "windows_build", "anticheat_list",
        "injection_method", "stage_reached",
        "time_since_last_inject_hours",
        "concurrent_sessions",
    ]
    
    # Crash-level features (per crash)
    CRASH_FEATURES = [
        "exception_code", "exception_code_category",
        "stack_trace_tfidf_vector",  # 100-dim embedding
        "time_in_game_seconds", "game_version", "cheat_version",
        "os_build", "cpu_vendor", "gpu_vendor",
        "crashes_same_user_7d", "crashes_same_product_7d",
    ]
```

---

### 3.3 Model Training Architecture

```scala
// Spark ML Pipeline Structure
// Location: zeer-bigdata/analytics/src/main/scala/com/zeer/ml/

object CrashClassifier {
  // Multi-class: Memory / CPU / Security / Anti-cheat / Game / Unknown
  val pipeline = new Pipeline()
    .setStages(Array(
      // Text features from stack trace
      new RegexTokenizer().setInputCol("full_stack_trace").setOutputCol("tokens"),
      new StopWordsRemover().setInputCol("tokens").setOutputCol("filtered"),
      new HashingTF().setInputCol("filtered").setOutputCol("tf").setNumFeatures(10000),
      new IDF().setInputCol("tf").setOutputCol("tfidf"),
      
      // Categorical features
      new StringIndexer().setInputCol("exception_code").setOutputCol("exc_idx"),
      new StringIndexer().setInputCol("game_version").setOutputCol("gv_idx"),
      new StringIndexer().setInputCol("cheat_version").setOutputCol("cv_idx"),
      new StringIndexer().setInputCol("os_build").setOutputCol("os_idx"),
      
      // Numerical features
      new VectorAssembler()
        .setInputCols(Array("tfidf", "exc_idx", "gv_idx", "cv_idx", "os_idx", 
                           "time_in_game_seconds", "crashes_same_user_7d"))
        .setOutputCol("features"),
      
      // Classifier
      new RandomForestClassifier()
        .setLabelCol("root_cause_category")
        .setFeaturesCol("features")
        .setNumTrees(200)
        .setMaxDepth(15)
        .setImpurity("gini")
    ))
}

object InjectionFailurePredictor {
  // Binary: will next injection fail?
  val pipeline = new Pipeline()
    .setStages(Array(
      // User history aggregations
      new StringIndexer().setInputCol("product_id").setOutputCol("pid_idx"),
      new StringIndexer().setInputCol("loader_version").setOutputCol("lv_idx"),
      new StringIndexer().setInputCol("windows_build").setOutputCol("wb_idx"),
      
      // Anti-cheat presence as multi-hot
      new MultiHotEncoder().setInputCol("anticheat_list").setOutputCol("av_vec"),
      
      new VectorAssembler()
        .setInputCols(Array("pid_idx", "lv_idx", "wb_idx", "av_vec",
                           "unique_hwids_30d", "failed_login_count_7d",
                           "hwid_mismatch_count_7d", "days_since_registration"))
        .setOutputCol("features"),
      
      new GBTClassifier()
        .setLabelCol("injection_failed")
        .setFeaturesCol("features")
        .setMaxIter(100)
        .setMaxDepth(8)
    ))
}

object UserChurnPredictor {
  // Binary: churn in next 30 days
  val pipeline = new Pipeline()
    .setStages(Array(
      new VectorAssembler()
        .setInputCols(Array(
          "actions_last_7d", "actions_last_30d",
          "total_session_time_7d", "avg_session_time",
          "injection_success_rate_7d", "crash_rate_7d",
          "days_since_last_action", "active_subscriptions",
          "total_spent_usd", "failed_login_count_7d",
          "unique_hwids_30d"
        ))
        .setOutputCol("features"),
      
      new LogisticRegression()
        .setLabelCol("churned_30d")
        .setFeaturesCol("features")
        .setRegParam(0.01)
        .setElasticNetParam(0.5)
        .setMaxIter(100)
    ))
}
```

---

### 3.4 Training Data Preparation (Spark Jobs)

```scala
// Daily feature materialization job
// Runs at 03:00 UTC, outputs to /zeer/ml/features/

object DailyFeatureMaterialization {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Zeer Daily Feature Materialization")
      .getOrCreate()
    
    val snapshotDate = args(0)  // "2026-09-22"
    val lookbackDays = args(1).toInt  // 30
    
    // 1. Load raw logs
    val actions = spark.read.parquet(s"/zeer/analytics/user-actions/$snapshotDate")
    val injects = spark.read.parquet(s"/zeer/analytics/injects/$snapshotDate")
    val crashes = spark.read.parquet(s"/zeer/analytics/crashes/$snapshotDate")
    
    // 2. User-level aggregations
    val userFeatures = actions.filter(col("timestamp") >= date_sub(snapshotDate, lookbackDays))
      .groupBy("user_id")
      .agg(
        count("*").as("actions_last_30d"),
        countDistinct("product_id").as("unique_products_used_30d"),
        sum("duration_ms").as("total_session_time_30d"),
        max("timestamp").as("last_action_ts"),
        // ... more aggregations
      )
    
    // 3. Session-level features (per injection attempt)
    val sessionFeatures = injects
      .withColumn("injection_failed", when(col("status") =!= "SUCCESS", 1).otherwise(0))
      .select("user_id", "product_id", "loader_version", "windows_name", 
              "anticheat_detected", "injection_method", "status", "duration_ms")
    
    // 4. Crash features with text embedding prep
    val crashFeatures = crashes
      .withColumn("root_cause_category", classifyException(col("exception_code"), col("full_stack_trace")))
      .select("user_id", "product_id", "exception_code", "full_stack_trace",
              "time_in_game_seconds", "game_version", "cheat_version", "os_info")
    
    // 5. Write feature stores
    userFeatures.write.mode("overwrite").parquet(s"/zeer/ml/features/user_features/$snapshotDate")
    sessionFeatures.write.mode("overwrite").parquet(s"/zeer/ml/features/session_features/$snapshotDate")
    crashFeatures.write.mode("overwrite").parquet(s"/zeer/ml/features/crash_features/$snapshotDate")
    
    spark.stop()
  }
  
  // Exception classification UDF
  def classifyException(code: String, trace: String): String = {
    code match {
      case "0xC0000005" => "MEMORY_ACCESS_VIOLATION"
      case "0xC000001D" => "ILLEGAL_INSTRUCTION"
      case "0xC00000FD" => "STACK_OVERFLOW"
      case "0x80000003" => "DEBUG_BREAKPOINT"
      case "0xC0000409" => "STACK_BUFFER_OVERRUN"
      case _ if trace.contains("VAC") || trace.Contains("Vanguard") => "ANTICHEAT_DETECTION"
      case _ => "UNKNOWN"
    }
  }
}
```

---

### 3.5 Model Serving & Inference

```python
# Real-time inference API (FastAPI on Master Node)
# Location: zeer-bigdata/api/ml_inference.py

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import mlflow.pyfunc
import pandas as pd

app = FastAPI(title="Zeer ML Inference")

# Load models at startup
crash_classifier = mlflow.pyfunc.load_model("models:/crash-classifier/Production")
injection_predictor = mlflow.pyfunc.load_model("models:/injection-failure-predictor/Production")
churn_predictor = mlflow.pyfunc.load_model("models:/churn-predictor/Production")

class CrashPredictRequest(BaseModel):
    exception_code: str
    stack_trace: str
    game_version: str
    cheat_version: str
    os_build: str
    time_in_game_seconds: int
    user_id: str

class InjectionPredictRequest(BaseModel):
    user_id: str
    product_id: str
    loader_version: str
    windows_build: str
    anticheat_list: list[str]

class ChurnPredictRequest(BaseModel):
    user_id: str

@app.post("/predict/crash-category")
async def predict_crash_category(req: CrashPredictRequest):
    features = prepare_crash_features(req.dict())
    prediction = crash_classifier.predict(features)
    probabilities = crash_classifier.predict_proba(features)
    return {
        "predicted_category": prediction[0],
        "confidence": float(max(probabilities[0])),
        "all_probabilities": dict(zip(crash_classifier.classes_, probabilities[0].tolist()))
    }

@app.post("/predict/injection-failure")
async def predict_injection_failure(req: InjectionPredictRequest):
    features = prepare_injection_features(req.dict())
    prob_failure = injection_predictor.predict_proba(features)[0][1]
    return {
        "failure_probability": float(prob_failure),
        "risk_level": "HIGH" if prob_failure > 0.7 else "MEDIUM" if prob_failure > 0.3 else "LOW",
        "recommendation": "Delay injection, update loader" if prob_failure > 0.7 else "Proceed normally"
    }

@app.post("/predict/user-churn")
async def predict_user_churn(req: ChurnPredictRequest):
    features = prepare_churn_features(req.user_id)
    prob_churn = churn_predictor.predict_proba(features)[0][1]
    return {
        "churn_probability": float(prob_churn),
        "risk_segment": "HIGH" if prob_churn > 0.6 else "MEDIUM" if prob_churn > 0.3 else "LOW",
        "suggested_actions": ["Offer discount", "Send engagement email"] if prob_churn > 0.6 else []
    }
```

---

## 4. Data Pipeline Architecture (Lambda/Kappa)

### 4.1 Unified Pipeline Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         ZEER LOG PROCESSING PIPELINE                         │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌────────────────────┐    │
│  │ Zeer Web │    │  Game    │    │  Loader  │    │  Admin Panel       │    │
│  │  UI      │    │  Client  │    │  API     │    │  (ban, edit)       │    │
│  └────┬─────┘    └────┬─────┘    └────┬─────┘    └─────────┬──────────┘    │
│       │               │               │                      │              │
│       ▼               ▼               ▼                      ▼              │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                    NODE.JS API COLLECTOR (Master)                     │   │
│  │  REST endpoints → validation → enrichment → Kafka producer           │   │
│  └────────────────────────────┬────────────────────────────────────────┘   │
│                               │                                             │
│              ┌────────────────┼────────────────┐                           │
│              ▼                ▼                ▼                           │
│       ┌─────────────┐ ┌─────────────┐ ┌─────────────┐                     │
│       │ Kafka Topic │ │ Kafka Topic │ │ Kafka Topic │                     │
│       │user-events  │ │crash-events │ │inject-events│                     │
│       │ partitions=6│ │ partitions=6│ │ partitions=6│                     │
│       └──────┬──────┘ └──────┬──────┘ └──────┬──────┘                     │
│              │               │               │                             │
│              ▼               ▼               ▼                             │
│  ┌────────────────────────────────────────────────────────────────────┐   │
│  │              FLUME AGENTS (Worker1, Worker2) - HA                   │   │
│  │  KafkaSource → MemoryChannel → HDFS Sink (partitioned by topic/date)│   │
│  └────────────────────────────┬────────────────────────────────────────┘   │
│                               │                                             │
│              ┌────────────────┼────────────────┐                           │
│              ▼                ▼                ▼                           │
│  ┌──────────────────┐ ┌──────────────────┐ ┌──────────────────┐           │
│  │ HDFS Raw Zone    │ │ HDFS Raw Zone    │ │ HDFS Raw Zone    │           │
│  │ /zeer/logs/      │ │ /zeer/logs/      │ │ /zeer/logs/      │           │
│  │ user-events/     │ │ crash-events/    │ │ inject-events/ │           │
│  │ YYYY/MM/DD/      │ │ YYYY/MM/DD/      │ │ YYYY/MM/DD/      │           │
│  └────────┬─────────┘ └────────┬─────────┘ └────────┬─────────┘           │
│           │                    │                    │                      │
│           └────────────────────┼────────────────────┘                      │
│                                ▼                                           │
│  ┌────────────────────────────────────────────────────────────────────┐   │
│  │           SPARK ON YARN — STREAMING + BATCH LAYER                   │   │
│  │                                                                     │   │
│  │  STREAMING (continuous):                                           │   │
│  │  • Crash Streaming Analyzer → /zeer/analytics/crashes/realtime    │   │
│  │  • Injection Anomaly Detector → /zeer/analytics/injects/realtime  │   │
│  │                                                                     │   │
│  │  BATCH (hourly/daily):                                             │   │
│  │  • UserActionsAnalyzer → /zeer/analytics/user-actions/            │   │
│  │  • CrashBatchAnalyzer → /zeer/analytics/crashes/daily/            │   │
│  │  • InjectionBatchAnalyzer → /zeer/analytics/injects/daily/        │   │
│  │  • FeatureMaterialization → /zeer/ml/features/                    │   │
│  │  • ModelTraining (weekly) → /zeer/ml/models/                      │   │
│  └────────────────────────────┬────────────────────────────────────────┘   │
│                               │                                             │
│              ┌────────────────┼────────────────┐                           │
│              ▼                ▼                ▼                           │
│  ┌──────────────────┐ ┌──────────────────┐ ┌──────────────────┐           │
│  │ HDFS Processed   │ │ HDFS Models      │ │ Hive Tables      │           │
│  │ /zeer/analytics/ │ │ /zeer/models/    │ │ (External on HDFS)           │
│  └────────┬─────────┘ └────────┬─────────┘ └────────┬─────────┘           │
│           │                    │                    │                      │
│           └────────────────────┼────────────────────┘                      │
│                                ▼                                           │
│  ┌────────────────────────────────────────────────────────────────────┐   │
│  │                    QUERY & SERVING LAYER                            │   │
│  │  • Hive/Trino for BI (Superset, Metabase)                          │   │
│  │  • Node.js API for real-time lookups                               │   │
│  │  • ML Inference API for predictions                                │   │
│  │  • Grafana for operational dashboards                              │   │
│  └────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

### 4.2 Processing SLA Matrix

| Layer | Latency Target | Throughput | Recovery |
|-------|----------------|------------|----------|
| **Ingestion (API → Kafka)** | < 100ms p99 | 10K events/min | At-least-once, idempotent |
| **Streaming (Crash/Inject)** | < 5 min | 100 events/min | Checkpoint every 1 min |
| **Batch (Daily Analytics)** | < 1 hour | Full dataset | Reprocess from raw |
| **Feature Materialization** | < 2 hours | 30-day window | Idempotent daily |
| **Model Training** | < 4 hours | Weekly | Retrain from scratch |
| **Inference API** | < 50ms p99 | 100 req/sec | Stateless, horizontal scale |

---

## 5. Data Quality & Governance

### 5.1 Data Contracts (Schema Registry)

```json
// Avro schemas registered in Confluent Schema Registry / custom store
{
  "zeer-user-events": {
    "type": "record",
    "name": "UserEvent",
    "namespace": "com.zeer.analytics",
    "fields": [
      {"name": "event_id", "type": "string"},
      {"name": "timestamp", "type": {"type": "long", "logicalType": "timestamp-millis"}},
      {"name": "user_id", "type": "string"},
      {"name": "action_type", "type": {"type": "enum", "name": "ActionType", 
        "symbols": ["REGISTER","LOGIN","KEY_ACTIVATION","PASSWORD_RESET",
                   "SUBSCRIPTION_PURCHASE","UNBIND_REQUEST","PROMO_ACTIVATE",
                   "DELETE_ITEM","ADD_ITEM","USER_EDIT","PRODUCT_EDIT","LOGOUT"]}},
      {"name": "status", "type": ["null", "string"], "default": null},
      {"name": "metadata", "type": ["null", {"type": "map", "values": "string"}]}
    ]
  }
}
```

### 5.2 Data Quality Checks (Great Expectations / Custom)

```python
# Data quality validation in Spark jobs
# Runs before writing to processed zone

EXPECTATIONS = {
    "user_events": [
        ("event_id", "not_null", {}),
        ("timestamp", "not_null", {}),
        ("user_id", "not_null", {}),
        ("action_type", "in_set", {"values": VALID_ACTION_TYPES}),
        ("status", "in_set", {"values": ["SUCCESS", "FAILED", "PENDING"]}),
        ("timestamp", "between", {"min": "2020-01-01", "max": "now+1day"}),
    ],
    "crash_events": [
        ("exception_code", "not_null", {}),
        ("exception_code", "matches_regex", {"pattern": "^0x[0-9A-F]{8}$"}),
        ("steam_id", "matches_regex", {"pattern": "^7656119[0-9]{10}$"}),
        ("time_in_game_seconds", "between", {"min": 0, "max": 86400}),
    ],
    "inject_events": [
        ("hwid", "not_null", {}),
        ("steam_id", "matches_regex", {"pattern": "^7656119[0-9]{10}$"}),
        ("inject_stage", "in_set", {"values": ["PRELOAD","INJECT","POST_INJECT","HEARTBEAT"]}),
        ("status", "in_set", {"values": ["SUCCESS","FAILED","BLOCKED"]}),
    ]
}
```

---

## 6. Implementation Roadmap

### Phase 1: Foundation (Weeks 1-2)
- [ ] Define Avro/JSON schemas for all 3 log types
- [ ] Implement Node.js API with validation & Kafka producer
- [ ] Deploy Kafka topics with proper partitioning
- [ ] Configure Flume agents for Kafka → HDFS
- [ ] Basic HDFS directory structure + Hive external tables

### Phase 2: Core Analytics (Weeks 3-4)
- [ ] UserActionsAnalyzer batch job (daily)
- [ ] CrashStreamingAnalyzer (5-min windows)
- [ ] InjectionBatchAnalyzer (daily)
- [ ] Hive views for business dashboards
- [ ] Grafana dashboards for operational metrics

### Phase 3: ML Platform (Weeks 5-6)
- [ ] Feature materialization job (daily)
- [ ] Crash classifier training + evaluation
- [ ] Injection failure predictor training
- [ ] MLflow model registry setup
- [ ] FastAPI inference service deployment

### Phase 4: Advanced Analytics (Weeks 7-8)
- [ ] Churn prediction model
- [ ] Fraud/anomaly detection (Isolation Forest)
- [ ] A/B testing framework for injector changes
- [ ] Alerting on ML predictions (high-risk users)
- [ ] Documentation & runbooks

### Phase 5: Production Hardening (Weeks 9-10)
- [ ] Schema evolution strategy
- [ ] Data quality monitoring + alerting
- [ ] Chaos testing pipeline resilience
- [ ] Performance optimization (partition pruning, caching)
- [ ] Security audit (PII handling, encryption)

---

## 7. Key Metrics to Track (North Stars)

| North Star Metric | Definition | Target | Owner |
|-------------------|------------|--------|-------|
| **Injection Success Rate** | Successful injections / Total attempts | > 98% | Platform |
| **Crash-Free Sessions** | Sessions without crash / Total sessions | > 95% | Engineering |
| **Time to Insight** | Log ingestion → Dashboard update | < 5 min | Data Eng |
| **Model Prediction Accuracy** | Crash classifier F1, Injection predictor AUC | F1>0.85, AUC>0.9 | ML Eng |
| **Fraud Detection Rate** | Caught account sharing / Total sharing | > 90% | Security |

---

## 8. Privacy & Compliance Notes

| Data Type | PII Fields | Retention | Encryption | Access Control |
|-----------|------------|-----------|------------|----------------|
| Action Logs | user_id, ip, email (in metadata) | 365 days agg, 90 days raw | At rest (HDFS) + in transit (TLS) | Role-based (admin only) |
| Crash Logs | steam_id, ip, hw info | 180 days | At rest + in transit | Engineering + Support |
| Inject Logs | steam_id, hwid, ip, location | 180 days | At rest + in transit | Engineering + Security |

**GDPR/152-FZ Considerations:**
- Right to deletion: purge user_id from all zones on request
- Data minimization: don't log full stack traces in production (hash instead)
- Consent: Terms of Service covers analytics processing

---

## 9. Appendix: Query Examples

### 9.1 Business Questions → SQL

```sql
-- Daily Active Users by Product
SELECT 
    DATE(timestamp) as day,
    product_id,
    COUNT(DISTINCT user_id) as dau
FROM zeer_user_events
WHERE action_type IN ('PRODUCT_LAUNCH_SUCCESS', 'INJECT_SUCCESS')
  AND timestamp >= DATE_SUB(CURRENT_DATE, 30)
GROUP BY day, product_id
ORDER BY day DESC, dau DESC;

-- Crash Rate by Cheat Version (last 7 days)
SELECT 
    cheat_version,
    COUNT(*) as crash_count,
    COUNT(DISTINCT user_id) as affected_users,
    COUNT(*) / SUM(COUNT(*)) OVER() as pct_of_total
FROM zeer_crash_events
WHERE timestamp >= DATE_SUB(CURRENT_DATE, 7)
GROUP BY cheat_version
ORDER BY crash_count DESC;

-- Account Sharing Detection (users with >3 HWIDs in 7 days)
SELECT 
    user_id,
    COUNT(DISTINCT hwid) as unique_hwids,
    COUNT(DISTINCT ip) as unique_ips,
    MAX(timestamp) as last_seen
FROM zeer_inject_events
WHERE timestamp >= DATE_SUB(CURRENT_DATE, 7)
  AND status = 'SUCCESS'
GROUP BY user_id
HAVING COUNT(DISTINCT hwid) > 3
ORDER BY unique_hwids DESC;

-- Injection Failure Reasons Breakdown
SELECT 
    error_code,
    COUNT(*) as count,
    COUNT(DISTINCT user_id) as affected_users
FROM zeer_inject_events
WHERE status = 'FAILED'
  AND timestamp >= DATE_SUB(CURRENT_DATE, 1)
GROUP BY error_code
ORDER BY count DESC;
```

---

**End of Design Document**

*Next Steps: Review with stakeholders → Prioritize Phase 1 tasks → Begin implementation in `zeer-bigdata/analytics/`*

📋 Design Summary

1. Three Log Schemas Defined (JSON + field descriptions)

┌───────────┬───────────────────────┬─────────────────────────────────────────────────────────────────┐
│ Log Type  │        Source         │                           Key Fields                            │
├───────────┼───────────────────────┼─────────────────────────────────────────────────────────────────┤
│ Action    │ Web UI (User/Admin)   │ 12 action types, categories (AUTH/COMMERCE/ADMIN/SECURITY),     │
│ Logs      │                       │ status, error codes                                             │
├───────────┼───────────────────────┼─────────────────────────────────────────────────────────────────┤
│ Crash     │ Game Client (Loader   │ Exception codes (Windows taxonomy), stack traces, game/cheat    │
│ Logs      │ API)                  │ versions, HW specs                                              │
├───────────┼───────────────────────┼─────────────────────────────────────────────────────────────────┤
│ Inject    │ DLL Injection (Loader │ HWID, Steam ID, inject stages (PRELOAD→INJECT→POST_INJECT),     │
│ Logs      │  API)                 │ anti-cheat detection                                            │
└───────────┴───────────────────────┴─────────────────────────────────────────────────────────────────┘

2. Loader API → Action Log Mapping (Your spec implemented)

- /inject_dll_preload failures → Action Logs with specific error categories
- /log_inject_hacks success → Inject Log created
- /crash_logs → Crash Log created
- /ban_user → Action Log USER_BANNED

3. Analytics Use Cases (4 Domains, 20+ Metrics)

┌───────────┬──────────────────────────────────────────────────────────────────────────────────────────┐
│  Domain   │                                       Key Metrics                                        │
├───────────┼──────────────────────────────────────────────────────────────────────────────────────────┤
│ Business  │ DAU/MAU, Registration Funnel, Conversion, ARPU, Churn, LTV, Promo effectiveness          │
├───────────┼──────────────────────────────────────────────────────────────────────────────────────────┤
│ Product   │ Product popularity, Session duration, Platform dist, HWID diversity (account sharing)    │
├───────────┼──────────────────────────────────────────────────────────────────────────────────────────┤
│ Technical │ Crash rate, Exception code ranking, Version regression, Injection latency P99,           │
│           │ Anti-cheat flags                                                                         │
├───────────┼──────────────────────────────────────────────────────────────────────────────────────────┤
│ Security  │ Account sharing (>3 HWIDs), Credential stuffing, License abuse, Ban evasion, Injection   │
│           │ anomalies                                                                                │
└───────────┴──────────────────────────────────────────────────────────────────────────────────────────┘

4. ML Pipeline (7 Use Cases, Prioritized)

┌──────────┬────────────────────────┬───────────────────────┬────────────────────────────────────────┐
│ Priority │         Model          │         Type          │                 Target                 │
├──────────┼────────────────────────┼───────────────────────┼────────────────────────────────────────┤
│ P0       │ Crash Classifier       │ Multi-class (6        │ Root cause from stack trace +          │
│          │                        │ categories)           │ exception code                         │
├──────────┼────────────────────────┼───────────────────────┼────────────────────────────────────────┤
│ P0       │ Injection Failure      │ Binary                │ Will next injection fail? (HWID        │
│          │ Predictor              │                       │ history, AV list, OS)                  │
├──────────┼────────────────────────┼───────────────────────┼────────────────────────────────────────┤
│ P1       │ User Churn Predictor   │ Binary                │ Churn in 30 days                       │
├──────────┼────────────────────────┼───────────────────────┼────────────────────────────────────────┤
│ P1       │ Fraud/Account Sharing  │ Anomaly (Isolation    │ Anomaly score per session              │
│          │                        │ Forest)               │                                        │
├──────────┼────────────────────────┼───────────────────────┼────────────────────────────────────────┤
│ P2       │ LTV Prediction         │ Regression            │ 12-month revenue                       │
├──────────┼────────────────────────┼───────────────────────┼────────────────────────────────────────┤
│ P2       │ Product Recommendation │ Collaborative         │ Next product upsell                    │
├──────────┼────────────────────────┼───────────────────────┼────────────────────────────────────────┤
│ P3       │ Optimal Injection      │ RL                    │ Best injection window                  │
│          │ Timing                 │                       │                                        │
└──────────┴────────────────────────┴───────────────────────┴────────────────────────────────────────┘

5. Spark ML Code Ready (Scala + Python)

- Feature engineering classes (user/session/crash level)
- 3 complete Pipeline definitions (RandomForest, GBT, LogisticRegression)
- Daily feature materialization job
- FastAPI inference service for real-time predictions

6. Lambda Architecture Pipeline

Web UI + Game Client + Loader API
        ↓
Node.js API Collector (Master)
        ↓
Kafka (3 topics × 6 partitions × RF=3)
        ↓
Flume HA (Worker1, Worker2) → HDFS Raw Zone
        ↓
Spark on YARN:
  • Streaming: Crash (5-min windows), Inject anomaly
  • Batch: User actions, Crash daily, Inject daily, Features ↓
HDFS Processed + Hive Tables + ML Models
        ↓
Query Layer: Hive/Trino, Node.js API, ML Inference API, Grafana

7. SLA Matrix & Data Quality

- Ingestion <100ms p99, Streaming <5min, Batch <1hr
- Great Expectations-style validation rules for all 3 log types
- Schema registry (Avro) with evolution strategy

8. 10-Week Implementation Roadmap (5 phases)

9. 5 North Star Metrics with targets

10. Privacy/Compliance (GDPR, 152-FZ) matrix---

Ready for implementation. The zeer-bigdata/analytics/ folder now has the design. Want me to scaffold the Spark job structure (SBT project, Scala sources) or the Node.js API endpoints for the Loader API integration next?
