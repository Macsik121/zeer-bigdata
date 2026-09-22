# Clickflow Analytics Implementation Plan for Zeer Marketplace

## Context
The goal is to implement **conversion-focused clickflow analytics** to track the user journey funnel: landing → product view → purchase → activation. This will help optimize conversion rates and understand user behavior patterns.

**Chosen approach**: Page-level clickflow tracking with separate analytical database.

**Key terminology**: The "more complex concept" mentioned is called **Event Tracking** (or Behavioral Analytics / Product Analytics) — granular tracking of clicks, scrolls, swipes, hovers, form interactions. This is what tools like Mixpanel, Amplitude, PostHog do. For now we implement clickflow (page views + navigation), with architecture ready to extend to event tracking later.

---

## Architecture Overview

```
┌─────────────────┐     ┌──────────────────┐     ┌────────────────────┐
│   Frontend      │     │   API Gateway    │     │  Analytics DB      │
│   (React)       │────▶│   (Express)      │────▶│  (ClickHouse/      │
│   - Tracker     │     │   /analytics     │     │   TimescaleDB/     │
│   - Queue       │     │   - Validation   │     │   PostgreSQL)      │
│   - Batcher     │     │   - Enrichment   │     │   - Events table   │
└─────────────────┘     │   - Batching     │     │   - Sessions table │
                        └──────────────────┘     │   - Users table    │
                                                 └────────────────────┘
```

---

## Implementation Plan

### Phase 1: Analytics Database Setup (Week 1)

#### 1.1 Choose and Provision Database
**Recommendation**: **ClickHouse** — columnar, fast for analytical queries, handles high write throughput, SQL-compatible, cost-effective.

Alternative: **TimescaleDB** (PostgreSQL extension) if team prefers PostgreSQL ecosystem.

#### 1.2 Schema Design
```sql
-- ClickHouse schema
CREATE TABLE events (
    event_id UUID DEFAULT generateUUIDv4(),
    event_type String,           -- 'page_view', 'click', 'form_submit', etc.
    event_name String,           -- 'product_view', 'purchase_start', 'purchase_complete'
    page_path String,            -- '/dashboard/products/zeer-csgo'
    referrer_path String,        -- previous page
    user_id Nullable(String),    -- hashed/anonymized
    session_id String,           -- session identifier
    anonymous_id String,         -- for unauthenticated users
    timestamp DateTime64(3),
    date Date DEFAULT toDate(timestamp),
    hour UInt8 DEFAULT toHour(timestamp),
    
    -- Enrichment fields
    user_agent String,
    browser String,
    os String,
    device_type String,          -- 'desktop', 'mobile', 'tablet'
    country String,
    city String,
    ip_hash String,              -- hashed IP for privacy
    
    -- UTM / attribution
    utm_source Nullable(String),
    utm_medium Nullable(String),
    utm_campaign Nullable(String),
    utm_content Nullable(String),
    utm_term Nullable(String),
    
    -- Custom properties (JSON)
    properties String,           -- JSON string for flexible properties
    
    -- Funnel-specific
    funnel_name Nullable(String),  -- 'purchase', 'signup', 'activation'
    funnel_step Nullable(UInt8),   -- 1, 2, 3...
    funnel_step_name Nullable(String)
) ENGINE = MergeTree()
PARTITION BY (date, hour)
ORDER BY (event_name, timestamp, user_id)
TTL date + INTERVAL 90 DAY DELETE;

CREATE TABLE sessions (
    session_id String,
    user_id Nullable(String),
    anonymous_id String,
    started_at DateTime64(3),
    ended_at Nullable(DateTime64(3)),
    duration_seconds Nullable(UInt32),
    pages_viewed UInt16 DEFAULT 0,
    entry_page String,
    exit_page Nullable(String),
    country String,
    device_type String,
    browser String,
    is_bounce Boolean DEFAULT false,
    date Date DEFAULT toDate(started_at)
) ENGINE = MergeTree()
PARTITION BY date
ORDER BY (session_id, started_at)
TTL date + INTERVAL 90 DAY DELETE;

CREATE TABLE users_analytics (
    user_id String,
    first_seen DateTime64(3),
    last_seen DateTime64(3),
    total_sessions UInt32 DEFAULT 0,
    total_events UInt32 DEFAULT 0,
    first_funnel_completed Nullable(String),  -- e.g., 'purchase'
    first_funnel_completed_at Nullable(DateTime64(3)),
    lifetime_value Nullable(Float64),
    status String,               -- 'active', 'churned', 'banned'
    country String,
    date Date DEFAULT toDate(first_seen)
) ENGINE = ReplacingMergeTree()
ORDER BY user_id;
```

### Phase 2: Backend API for Analytics (Week 1-2)

#### 2.1 Create Analytics Endpoint in Express API
**File**: `zeer-marketplace-api/db_actions/api_loader/analytics.js` (new)

```javascript
// POST /api_loader/analytics/batch
// Accepts batch of events from frontend
// Validates, enriches, forwards to ClickHouse
```

**Key responsibilities**:
- Receive batched events from frontend
- Validate payload structure
- Enrich with geo/IP data (reuse `getLocationByIP`)
- Parse user agent (reuse `detectBrowser`)
- Hash PII (IP, user ID)
- Batch insert to ClickHouse
- Return acceptance response quickly (< 50ms)

#### 2.2 Add ClickHouse Client
**File**: `zeer-marketplace-api/db_actions/db/clickhouse.js` (new)

```javascript
const { createClient } = require('@clickhouse/client');

const clickhouse = createClient({
    url: process.env.CLICKHOUSE_URL || 'http://localhost:8123',
    username: process.env.CLICKHOUSE_USER || 'default',
    password: process.env.CLICKHOUSE_PASSWORD || '',
    database: process.env.CLICKHOUSE_DB || 'zeer_analytics',
    compression: { request: true, response: true }
});
```

#### 2.3 Register Route in server.js
```javascript
const analyticsRouter = require('./db_actions/api_loader/analytics');
app.use('/api_loader/analytics', analyticsRouter);
```

### Phase 3: Frontend Tracking Library (Week 2)

#### 3.1 Create Analytics Tracker Module
**File**: `zeer-marketplace-ui/src/analytics/tracker.js` (new)

```javascript
class AnalyticsTracker {
    constructor(options = {}) {
        this.apiEndpoint = options.apiEndpoint || '/api_loader/analytics/batch';
        this.batchSize = options.batchSize || 10;
        this.flushInterval = options.flushInterval || 5000; // 5 seconds
        this.queue = [];
        this.sessionId = this.getOrCreateSessionId();
        this.anonymousId = this.getOrCreateAnonymousId();
        this.userId = null;
        this.pageLoadTime = Date.now();
        this.lastActivity = Date.now();
        
        this.initAutoFlush();
        this.initPageViewTracking();
        this.initVisibilityTracking();
        this.initBeforeUnload();
    }
    
    // Core tracking methods
    trackPageView(pagePath, properties = {})
    trackEvent(eventName, properties = {})
    trackFunnelStep(funnelName, stepNumber, stepName, properties = {})
    identify(userId, traits = {})
    reset() // on logout
    
    // Queue management
    addToQueue(event)
    flush()
    
    // Enrichment
    enrichEvent(event)
}
```

#### 3.2 Auto-Track Page Views
- Hook into React Router navigation (use `withRouter` or `useLocation` hook)
- Track: `page_view` with `page_path`, `referrer_path`, `load_time`
- Capture UTM parameters from URL on first visit

#### 3.3 Track Key Funnel Events
| Funnel | Steps | Events |
|--------|-------|--------|
| **Purchase** | 1. Product list view → 2. Product detail view → 3. Buy modal open → 4. Payment redirect → 5. Payment success | `product_list_view`, `product_detail_view`, `purchase_initiated`, `payment_redirect`, `purchase_completed` |
| **Signup** | 1. Landing → 2. Signup modal open → 3. Form submit → 4. Email verify → 5. First login | `signup_started`, `signup_submitted`, `email_verified`, `first_login` |
| **Activation** | 1. Dashboard → 2. Download loader → 3. Loader auth → 4. Product inject | `loader_downloaded`, `loader_auth_success`, `product_injected` |

#### 3.4 Integration Points in Existing Components

**Dashboard.jsx** — track product views, buy clicks:
```javascript
// In buyProduct() - track purchase_initiated
analytics.trackFunnelStep('purchase', 3, 'purchase_initiated', {
    product_title: title,
    product_cost: cost,
    days: days
});

// In getProducts() - track product_list_view
analytics.trackPageView('/dashboard/products', { products_count: products.length });
```

**ProductInfo.jsx** — track product detail view:
```javascript
// componentDidMount
analytics.trackFunnelStep('purchase', 2, 'product_detail_view', {
    product_title: product.title,
    product_for: product.product_for
});
```

**Lobby.jsx** — track popular product views, buy clicks

**Home.jsx** — track landing page view, signup/login clicks

**Signin/Signup modals** — track auth funnel

### Phase 4: Session Management (Week 2)

#### 4.1 Session Tracking Logic
- Session starts on first page view
- Session ends after 30 min inactivity (configurable)
- Track `session_start`, `session_end` events
- Calculate session duration, pages viewed, bounce detection

#### 4.2 Identity Management
- **Anonymous users**: Track with `anonymous_id` (stored in localStorage)
- **Authenticated users**: Call `identify(userId)` on login, merge with anonymous history
- **Logout**: Call `reset()` to clear userId, keep anonymous_id

### Phase 5: Data Pipeline & Enrichment (Week 2-3)

#### 5.1 IP Geolocation Enrichment
Reuse existing `getLocationByIP` logic in analytics endpoint.

#### 5.2 User Agent Parsing
Reuse existing `detectBrowser` logic.

#### 5.3 PII Protection
- Hash IP addresses with salt (SHA-256)
- Hash user IDs (or use internal numeric IDs)
- Don't store raw emails, names in analytics DB

### Phase 6: Query & Visualization Layer (Week 3-4)

#### 6.1 GraphQL Queries for Admin Panel
Add to existing GraphQL schema:

```graphql
type AnalyticsEvent {
    eventId: ID!
    eventType: String!
    eventName: String!
    pagePath: String
    timestamp: DateTime!
    userId: String
    sessionId: String!
    properties: JSON
}

type FunnelConversion {
    funnelName: String!
    totalStarted: Int!
    step1: FunnelStep!
    step2: FunnelStep!
    step3: FunnelStep!
    step4: FunnelStep!
    step5: FunnelStep!
    overallConversionRate: Float!
}

type FunnelStep {
    stepNumber: Int!
    stepName: String!
    count: Int!
    conversionFromPrevious: Float!
    conversionFromStart: Float!
    avgTimeToNextStep: Float  # seconds
}

type SessionStats {
    totalSessions: Int!
    avgSessionDuration: Float!
    avgPagesPerSession: Float!
    bounceRate: Float!
    sessionsByDevice: [DeviceStat!]!
    sessionsByCountry: [CountryStat!]!
    sessionsByBrowser: [BrowserStat!]!
}

type PageViewStats {
    pagePath: String!
    views: Int!
    uniqueVisitors: Int!
    avgTimeOnPage: Float!
    bounceRate: Float!
    exitRate: Float!
}
```

#### 6.2 Admin Panel Dashboard Components
**New files**:
- `zeer-marketplace-ui/src/AdminPanel/Analytics/FunnelAnalytics.jsx` — funnel visualization
- `zeer-marketplace-ui/src/AdminPanel/Analytics/PageViews.jsx` — top pages, paths
- `zeer-marketplace-ui/src/AdminPanel/Analytics/Sessions.jsx` — session metrics
- `zeer-marketplace-ui/src/AdminPanel/Analytics/UserJourney.jsx` — sankey/flow diagram

Reuse existing `Graph` component for charts, add new visualization types.

### Phase 7: Testing & Rollout (Week 4)

#### 7.1 Test Scenarios
- [ ] Page view tracking on all routes
- [ ] Funnel events fire at correct steps
- [ ] Batch sending works (network tab)
- [ ] Data appears in ClickHouse
- [ ] GraphQL queries return correct data
- [ ] Admin panel visualizations render
- [ ] No performance impact on main app
- [ ] GDPR/privacy compliance (opt-out, data deletion)

#### 7.2 Gradual Rollout
1. Deploy to staging, verify data flow
2. Enable for 10% of users (feature flag)
3. Monitor error rates, performance
4. Full rollout

---

## Critical Files to Create/Modify

### New Files
1. `zeer-marketplace-api/db_actions/db/clickhouse.js` — ClickHouse client
2. `zeer-marketplace-api/db_actions/api_loader/analytics.js` — Analytics API endpoint
3. `zeer-marketplace-ui/src/analytics/tracker.js` — Frontend tracker class
4. `zeer-marketplace-ui/src/analytics/index.js` — Export singleton instance
5. `zeer-marketplace-ui/src/analytics/funnelEvents.js` — Funnel event definitions
6. `zeer-marketplace-ui/src/AdminPanel/Analytics/FunnelAnalytics.jsx`
7. `zeer-marketplace-ui/src/AdminPanel/Analytics/PageViews.jsx`
8. `zeer-marketplace-ui/src/AdminPanel/Analytics/Sessions.jsx`
9. `zeer-marketplace-ui/src/AdminPanel/Analytics/UserJourney.jsx`
10. `zeer-marketplace-ui/src/AdminPanel/Analytics/AnalyticsDashboard.jsx` — main container

### Modified Files
1. `zeer-marketplace-api/server.js` — Register analytics route
2. `zeer-marketplace-api/.env` — Add ClickHouse credentials
3. `zeer-marketplace-ui/src/Routing.jsx` — Initialize tracker, track route changes
4. `zeer-marketplace-ui/src/Dashboard/Dashboard.jsx` — Track product views, purchases
5. `zeer-marketplace-ui/src/Dashboard/Products.jsx` — Track product list views, buy clicks
6. `zeer-marketplace-ui/src/Dashboard/ProductInfo.jsx` — Track product detail views
7. `zeer-marketplace-ui/src/Dashboard/Lobby.jsx` — Track popular product views
8. `zeer-marketplace-ui/src/Home/Home.jsx` — Track landing page, signup/login clicks
9. `zeer-marketplace-ui/src/Home/Signin.jsx` — Track login funnel
10. `zeer-marketplace-ui/src/Home/Signup.jsx` — Track signup funnel
11. GraphQL schema (location TBD) — Add analytics queries

---

## Verification Plan

### Unit/Integration Tests
```bash
# Backend
cd zeer-marketplace-api && npm test  # Add tests for analytics endpoint

# Frontend  
cd zeer-marketplace-ui && npm test  # Add tests for tracker
```

### Manual Verification Checklist
1. **Start stack**: `docker-compose up -d clickhouse` (add to docker-compose.yml)
2. **Start API**: `cd zeer-marketplace-api && npm start`
3. **Start UI**: `cd zeer-marketplace-ui && npm start`
4. **Navigate funnel**: Home → Dashboard → Products → Product Detail → Buy → Payment
5. **Check network tab**: Verify `/api_loader/analytics/batch` requests with payloads
6. **Query ClickHouse**: `SELECT * FROM events ORDER BY timestamp DESC LIMIT 100`
7. **Open Admin Panel**: Navigate to new Analytics section, verify charts render

### Performance Benchmarks
- Frontend tracker overhead: < 1ms per event
- Batch send latency: < 100ms p99
- API response time: < 50ms p99
- ClickHouse insert throughput: > 10k events/sec

---

## Future Extensions (Post-MVP)

1. **Event Tracking** — Upgrade to element-level tracking (clicks, scrolls, form interactions)
2. **Session Replay** — Integrate rrweb or similar for session recording
3. **Real-time Dashboard** — WebSocket updates for live monitoring
4. **Cohort Analysis** — Retention curves, LTV by cohort
5. **A/B Testing Framework** — Feature flag + analytics integration
6. **Alerting** — Anomaly detection on funnel drop-offs
7. **Data Export** — Scheduled exports to S3/BigQuery for ML

---

## Estimated Timeline

| Phase | Duration | Dependencies |
|-------|----------|--------------|
| 1. Database Setup | 2-3 days | ClickHouse provisioning |
| 2. Backend API | 3-4 days | Phase 1 |
| 3. Frontend Tracker | 3-4 days | Phase 2 (API ready) |
| 4. Session Management | 2 days | Phase 3 |
| 5. Data Pipeline | 2-3 days | Phase 2, 3 |
| 6. Query & Visualization | 4-5 days | Phase 1, 5 |
| 7. Testing & Rollout | 3-4 days | All phases |
| **Total** | **~3-4 weeks** | |

---

## Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| ClickHouse not available | High | Fallback to PostgreSQL/TimescaleDB; same interface |
| High write volume | Medium | Batch inserts, async processing, buffer in Redis |
| Privacy compliance | High | PII hashing, opt-out mechanism, data retention policy |
| Frontend performance | Medium | Non-blocking tracker, requestIdleCallback, small batches |
| Data quality | Medium | Schema validation, integration tests, monitoring dashboards |

---

## Next Steps

1. **Confirm database choice** (ClickHouse vs TimescaleDB vs PostgreSQL)
2. **Provision database** (local Docker for dev, cloud for prod)
3. **Create tracker module** — start with minimal page view tracking
4. **Integrate with routing** — auto-track all page views
5. **Add funnel events** — purchase funnel first (highest value)
6. **Build admin dashboard** — reuse existing Graph component patterns

---

*Saved for future consideration — not for immediate implementation.*