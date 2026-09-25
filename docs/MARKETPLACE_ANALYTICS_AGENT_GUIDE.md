# Marketplace Analytics Agent — Руководство по использованию

## Обзор

**marketplace-analytics-expert** — специализированный агент для аналитики e-commerce систем и маркетплейсов. Он обладает глубокими знаниями в области product analytics, business intelligence, и data-driven decision making, специально настроенный для работы с маркетплейсом Zeer.

## Когда использовать этого агента

### Основные сценарии применения

1. **Определение метрик для анализа**
   - Когда нужно понять, что именно измерять в системе
   - Выбор North Star метрики для бизнеса
   - Построение дерева метрик (метрики-драйверы)

2. **Проектирование системы аналитики**
   - Разработка event tracking схемы
   - Выбор аналитических инструментов и баз данных
   - Дизайн дашбордов для stakeholders

3. **Диагностика проблем конверсии**
   - Анализ воронок конверсии (funnel analysis)
   - Поиск узких мест в user journey
   - Сегментация пользователей для выявления паттернов

4. **Анализ пользовательского поведения**
   - Clickflow и event tracking
   - Cohort analysis (когортный анализ)
   - RFM-анализ для сегментации клиентов

5. **Оптимизация бизнес-метрик**
   - Увеличение LTV (Customer Lifetime Value)
   - Снижение CAC (Customer Acquisition Cost)
   - Повышение retention rate и снижение churn

## Как вызвать агента

### Через Claude Code CLI

```bash
# Пример 1: Определение метрик
/agent marketplace-analytics-expert "Какие метрики нужно собирать для маркетплейса читов Zeer?"

# Пример 2: Диагностика проблемы
/agent marketplace-analytics-expert "Низкая конверсия на этапе checkout — как диагностировать причину?"

# Пример 3: Проектирование event tracking
/agent marketplace-analytics-expert "Спроектируй event tracking схему для отслеживания пути пользователя от лендинга до активации ключа"

# Пример 4: Анализ логов
/agent marketplace-analytics-expert "Проанализируй Action Logs и предложи топ-5 метрик для мониторинга"
```

### Через Agent tool в коде

```javascript
// В другом агенте или скрипте
const analyticsExpert = await agent(
  "Определи North Star метрику для маркетплейса Zeer и её драйверы",
  {
    agentType: 'marketplace-analytics-expert',
    schema: METRICS_SCHEMA
  }
);
```

## Компетенции агента

### 1. Product Analytics & User Behavior

**Что умеет:**
- Проектировать clickflow и event tracking системы
- Строить воронки конверсии (conversion funnels)
- Проводить cohort analysis и RFM-анализ
- Анализировать session data (device, geo, UTM)

**Примеры задач:**
- "Построй воронку конверсии для покупки подписки на 7 дней"
- "Как сегментировать пользователей по поведению?"
- "Какие события нужно трекать для анализа user journey?"

### 2. E-commerce Metrics & KPIs

**Основные метрики, с которыми работает:**

**Revenue Metrics:**
- GMV (Gross Merchandise Value)
- AOV (Average Order Value)
- LTV (Customer Lifetime Value)
- Take Rate (комиссия маркетплейса)

**Conversion Metrics:**
- Overall Conversion Rate
- View-to-Cart Rate
- Checkout Abandonment Rate

**User Engagement:**
- DAU/MAU/WAU
- Retention Rate (Day 1, 7, 30)
- Churn Rate
- Repeat Purchase Rate

**Acquisition:**
- CAC (Customer Acquisition Cost)
- LTV/CAC Ratio
- Payback Period

**Примеры задач:**
- "Как рассчитать LTV для пользователей с подпиской на читы?"
- "Какой должен быть целевой LTV/CAC ratio?"
- "Как измерить retention rate для нашего маркетплейса?"

### 3. Специфика маркетплейса Zeer

Агент знает особенности Zeer marketplace:

**Zeer-specific Analytics:**
- **Loader Downloads**: метрики скачивания лоадера
- **Activation Rate**: процент активировавших ключ после покупки
- **Injection Success Rate**: успешность инжектов (из Inject Logs)
- **Crash Analysis**: анализ крашей по играм и конфигурациям (из Crash Logs)
- **HWID Reset Requests**: частота запросов на сброс привязки
- **Product Popularity by Game**: популярность продуктов по играм

**Subscription Analytics:**
- Распределение по длительности (1/7/30 дней)
- Renewal Rate (процент продлений)
- Subscription Churn
- Revenue by Subscription Tier

**Security & Fraud Detection:**
- Failed Login Attempts
- Multiple HWID Bindings (подозрительная активность)
- Refund Abuse
- Payment Fraud patterns

**Примеры задач:**
- "Как анализировать Crash Logs для выявления проблемных конфигураций?"
- "Какие метрики показывают fraud в HWID bindings?"
- "Спроектируй дашборд для мониторинга activation rate по играм"

### 4. Data Pipeline & Architecture

**Что умеет:**
- Проектировать data pipeline от событий до BI tools
- Выбирать инструменты (ClickHouse, Kafka, Flume)
- Разрабатывать event schema
- Интегрировать аналитику с существующей инфраструктурой

**Примеры задач:**
- "Как организовать pipeline от Frontend Events до ClickHouse?"
- "Какую event schema использовать для отслеживания покупок?"
- "Интеграция Google Analytics с нашей системой — что учесть?"

### 5. Dashboards & Reporting

**Типы дашбордов, которые проектирует:**
- **Executive Dashboard**: высокоуровневые KPI для топ-менеджмента
- **Product Dashboard**: метрики товаров и категорий
- **Marketing Dashboard**: эффективность каналов привлечения
- **Operations Dashboard**: операционные метрики
- **Real-time Dashboard**: мониторинг в реальном времени

**Примеры задач:**
- "Спроектируй Executive Dashboard для маркетплейса Zeer"
- "Какие метрики показывать на Real-time Dashboard?"
- "Настрой алерты на критичные метрики (падение conversion, рост crash rate)"

## Интеграция с другими агентами

### Совместная работа

**marketplace-analytics-expert** координируется с другими агентами проекта:

**С big-data-engineer:**
```bash
# Сначала аналитик определяет метрики
/agent marketplace-analytics-expert "Определи топ-10 метрик для сбора из Action Logs"

# Затем Big Data инженер настраивает инфраструктуру
/agent big-data-engineer "Настрой Flume для сбора метрик: <список метрик>"
```

**С nodejs-express-backend-expert:**
```bash
# Аналитик проектирует event tracking
/agent marketplace-analytics-expert "Спроектируй event schema для API сбора событий"

# Backend разработчик реализует API
/agent nodejs-express-backend-expert "Реализуй Express API для приёма событий по схеме: <schema>"
```

**С postgresql-database-designer:**
```bash
# Аналитик определяет структуру аналитической БД
/agent marketplace-analytics-expert "Спроектируй схему таблиц для ClickHouse под clickflow analytics"

# Database designer адаптирует под PostgreSQL (если нужно)
/agent postgresql-database-designer "Адаптируй эту схему под TimescaleDB"
```

**С react-frontend-expert:**
```bash
# Аналитик определяет, какие события трекать
/agent marketplace-analytics-expert "Какие frontend события нужно трекать для анализа user journey?"

# React разработчик интегрирует трекинг
/agent react-frontend-expert "Интегрируй event tracking в React компоненты: <список событий>"
```

## Типичные запросы и ответы

### Пример 1: Определение метрик

**Запрос:**
```
Какие метрики нужно собирать для маркетплейса Zeer, чтобы оптимизировать конверсию?
```

**Ответ агента будет включать:**
- North Star Metric (например, GMV или Active Buyers)
- Дерево метрик-драйверов
- Приоритет сбора данных (High/Medium/Low)
- Рекомендуемые дашборды
- События для трекинга

### Пример 2: Диагностика проблемы

**Запрос:**
```
У нас низкая конверсия на этапе checkout (10%). Как диагностировать причину?
```

**Ответ агента будет включать:**
- Гипотезы причин (сложная форма, долгая загрузка, проблемы с оплатой)
- Данные для анализа (логи, события, сегменты пользователей)
- План действий (построить воронку, сегментировать, сравнить с бенчмарками)
- Метрики для мониторинга

### Пример 3: Проектирование аналитики

**Запрос:**
```
Спроектируй систему аналитики для отслеживания успешности активации ключей после покупки
```

**Ответ агента будет включать:**
- Event schema (purchase_complete, key_activated, injection_success)
- Data pipeline (Frontend → API → Kafka → ClickHouse)
- Метрики (Activation Rate, Time-to-Activate, Success Rate by Game)
- Дашборд mockup
- Алерты на критичные значения

## Формат ответов агента

Агент структурирует ответы в виде frameworks, списков и диаграмм:

### Структура для "Что анализировать?"

```markdown
## Ключевые метрики для <контекст>

### North Star Metric
- **Метрика**: GMV
- **Формула**: Sum(Orders.amount)
- **Почему**: Отражает реальный объём бизнеса

### Метрики-драйверы
1. **Traffic (DAU × Session Length)**
   - Формула: Daily Active Users × Avg Session Duration
   - Текущее значение: N/A
   - Target: 1000 DAU, 5 min/session
   - Как влияет: Больше трафика → больше потенциальных покупок

2. **Conversion Rate**
   - Формула: Purchases / Visitors
   - Target: 3-5%
   - Декомпозиция: ...

### Приоритет сбора данных
- [x] **High priority**: Page views, Add-to-cart, Purchase events
- [ ] **Medium priority**: Search queries, Filter usage
- [ ] **Low priority**: Scroll depth, Mouse tracking
```

### Структура для диагностики

```markdown
## Анализ проблемы: [описание]

### Гипотезы
1. **Сложная форма оплаты**: Много полей, непонятная валидация
   - Как проверить: Измерить время на странице checkout, анализ ошибок формы
   - Ожидаемые findings: Drop-off на конкретных полях

### Данные для анализа
- [x] Логи: Action Logs за последние 30 дней
- [x] События: checkout_started, payment_method_selected, checkout_completed
- [x] Временной диапазон: 2026-08-25 — 2026-09-25
- [x] Сегменты: New users vs Returning, Desktop vs Mobile

### План действий
1. Построить воронку checkout
2. Сегментировать по device type
3. Измерить время между этапами
4. Сравнить с бенчмарком (20-40% checkout conversion)
5. A/B тест упрощённой формы
```

## Best Practices

### 1. Всегда начинай с бизнес-целей

Не спрашивай "какие метрики собирать вообще", а формулируй:
```
У нас цель — увеличить recurring revenue на 30%. Какие метрики помогут это отслеживать и оптимизировать?
```

### 2. Используй конкретный контекст

Вместо:
```
Как анализировать пользователей?
```

Спрашивай:
```
Как сегментировать пользователей маркетплейса Zeer, чтобы найти группу с самым высоким LTV?
```

### 3. Запрашивай actionable insights

Вместо:
```
Расскажи про A/B тестирование
```

Спрашивай:
```
Спроектируй A/B тест для проверки гипотезы: упрощение формы checkout повысит конверсию на 5%
```

### 4. Итеративно уточняй

Начни с high-level:
```
Какую систему аналитики построить для Zeer?
```

Затем углубляйся:
```
Спроектируй event schema для этой системы
Какие дашборды нужны для разных stakeholders?
Настрой алерты на критичные метрики
```

## Ограничения

Агент **НЕ выполняет**:
- Настройку инфраструктуры (Hadoop, Kafka, Flume) — используй `big-data-engineer`
- Написание кода API или фронтенда — используй соответствующих агентов
- Проектирование схемы реляционных БД — используй `postgresql-database-designer`

Агент **специализируется на**:
- Проектировании аналитических решений
- Определении метрик и KPI
- Диагностике проблем через данные
- Рекомендациях по инструментам и подходам

## Примеры реальных задач для Zeer

### Задача 1: Анализ Crash Logs

```bash
/agent marketplace-analytics-expert "Проанализируй, какие метрики можно извлечь из Crash Logs для улучшения стабильности продукта. Какие дашборды нужны разработчикам?"
```

**Ожидаемый результат:**
- Метрики: Crash Rate по играм, по версиям ОС, по конфигурациям GPU
- Дашборд: Real-time crash monitoring с фильтрами
- Алерты: Spike в crash rate > 5%
- Event schema для сбора крашей

### Задача 2: Оптимизация воронки покупки

```bash
/agent marketplace-analytics-expert "Построй полную воронку конверсии от landing page до активации ключа. Где обычно самые большие drop-offs в маркетплейсах читов?"
```

**Ожидаемый результат:**
- Воронка: Landing → Category → Product → Add-to-Cart → Checkout → Payment → Download Loader → Key Activation
- Бенчмарки по каждому этапу
- Гипотезы причин drop-off
- План A/B тестов

### Задача 3: Retention analysis

```bash
/agent marketplace-analytics-expert "Как измерить retention для пользователей с подпиской на 30 дней? Какой должен быть target renewal rate?"
```

**Ожидаемый результат:**
- Метрика: Day 1/7/30 retention, Renewal Rate
- Когортный анализ по месяцам регистрации
- Сравнение retention между тарифами (1 день vs 30 дней)
- Рекомендации по улучшению

## Заключение

**marketplace-analytics-expert** — мощный инструмент для всех задач, связанных с аналитикой маркетплейса Zeer. Используй его для:
- Стратегического планирования (какие метрики важны)
- Тактической диагностики (почему падает конверсия)
- Проектирования систем (как собирать и анализировать данные)

Комбинируй с другими агентами для end-to-end реализации: от определения метрик до настройки инфраструктуры и написания кода.

---

**Версия**: 1.0  
**Дата**: 2026-09-25  
**Автор**: Максим Салий
