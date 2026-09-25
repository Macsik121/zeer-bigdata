# Zeer Marketplace — Каталог метрик Big Data

**Версия:** 1.0
**Дата:** 2026-09-24
**Статус:** Проектирование
**Основание:** `LOG_ANALYTICS_DESIGN.md` (схемы логов), `servers-infrastructure-software-implementation-plan.md` (инфраструктура кластера)

---

## 1. Назначение документа

Документ фиксирует **перечень метрик**, которые система обработки больших данных Zeer Marketplace вычисляет на основе трёх потоков логов. Каталог является входными данными для проектирования конвейера обработки (`DATA_PIPELINE_DESIGN.md`) и определяет, какие витрины должны быть построены на слое Mart.

### 1.1 Принципы отбора метрик

1. **Вычислимость** — каждая метрика считается только из полей, объявленных в схемах логов (`LOG_ANALYTICS_DESIGN.md`, раздел 1). Полей, которых нет в схеме, метрики не используют.
2. **Действие** — метрика, по отклонению которой никто не предпринимает действий, в каталог не включается. У каждой строки есть владелец и runbook.
3. **Трассируемость** — для каждой метрики указан источник (endpoint / таблица), способ сбора и job, который её вычисляет.

### 1.2 Структура описания метрики (6 обязательных полей)

| № | Поле | Содержание |
|---|------|-----------|
| 1 | **Что измеряем** | Формула или определение показателя |
| 2 | **Зачем** | Управленческий или инженерный вопрос, на который отвечает метрика |
| 3 | **Откуда берём** | Тип лога, endpoint-источник, конкретные поля |
| 4 | **Как собираем** | Режим (push/pull), интервал, тип агрегации, окно |
| 5 | **Как интерпретировать** | Норма / Warning / Critical с числовыми порогами |
| 6 | **Что делать при отклонении** | Канал алерта, runbook, владелец |

Дополнительные колонки `ID` и `Job` служат для трассируемости и в обязательный минимум не входят.

### 1.3 Источники данных

| Лог | Источник | Endpoint | Объём (оценка) |
|-----|----------|----------|----------------|
| **Action Logs** | Web API (GraphQL) | GraphQL-мутации + `/api_loader/block_user` | ~10 000 событий/сутки |
| **Crash Logs** | Loader API | `POST /api_loader/crash_logs` | ~200–500 событий/сутки |
| **Inject Logs** | Loader API | `POST /api_loader/inject_dll_preload`, `POST /api_loader/log_inject_hacks` | ~5 000–15 000 событий/сутки |

### 1.4 Роли-владельцы

| Роль | Зона ответственности |
|------|---------------------|
| **Product** | Бизнес-показатели, конверсия, удержание |
| **Platform** | Стабильность лоадера, инжект, совместимость |
| **Security** | Фрод, шаринг аккаунтов, обход банов |
| **Data Eng** | Работоспособность самого конвейера, качество данных |
| **Support** | Реакция на обращения, вызванные деградацией метрик |

---

## 2. Каталог метрик

### 2.1 Домен A — Бизнес-метрики (Revenue & Growth)

| ID | 1. Что измеряем | 2. Зачем / какой вопрос решает | 3. Откуда берём (лог, endpoint, поля) | 4. Как собираем | 5. Интерпретация (норма / warning / critical) | 6. Действие при отклонении | Job |
|----|-----------------|-------------------------------|---------------------------------------|-----------------|---------------------------------------------|---------------------------|-----|
| **A1** | **DAU / MAU и Stickiness**<br>`DAU = COUNT(DISTINCT user_id) за сутки`<br>`Stickiness = DAU / MAU` | Растёт ли живая аудитория? Не маскирует ли разовый приток отток постоянных пользователей? | Action Logs (GraphQL, любое событие) + Inject Logs (`/log_inject_hacks`, `status='SUCCESS'`).<br>Поля: `user_id`, `timestamp` | Push → Kafka → HDFS.<br>Batch, 1×/сут в 03:10.<br>`COUNT DISTINCT` по окну 1 сут и 30 сут | Норма: Stickiness ≥ 0.25<br>Warning: 0.15–0.25 или падение DAU >15% н/н<br>Critical: < 0.15 или падение DAU >30% н/н | Дашборд Product Overview.<br>Warning → еженедельный разбор.<br>Critical → алерт в `#zeer-product`, RB-01 «Падение активной аудитории».<br>**Владелец: Product** | `UserActionsBatchJob` |
| **A2** | **Воронка регистрация → оплата**<br>Конверсия по шагам: `REGISTER → LOGIN → SUBSCRIPTION_PURCHASE` | Где теряются новые пользователи до первой оплаты? Какой шаг чинить в первую очередь? | Action Logs.<br>Поля: `action_type ∈ {REGISTER, LOGIN, SUBSCRIPTION_PURCHASE}`, `status='SUCCESS'`, `user_id`, `timestamp` | Push → Kafka → HDFS.<br>Batch, 1×/сут.<br>Когортный анализ: когорта по дате регистрации, окно наблюдения 7 сут | Норма: REGISTER→LOGIN ≥ 70 %, LOGIN→PURCHASE ≥ 8 %<br>Warning: REGISTER→LOGIN 50–70 % или LOGIN→PURCHASE 4–8 %<br>Critical: REGISTER→LOGIN < 50 % или LOGIN→PURCHASE < 4 % | Дашборд Funnel.<br>Critical → алерт `#zeer-product`, RB-02 «Обвал воронки»: проверить работоспособность формы регистрации и платёжного шлюза.<br>**Владелец: Product** | `FunnelBatchJob` |
| **A3** | **ARPPU**<br>`SUM(product_cost) / COUNT(DISTINCT платящих user_id)` за месяц | Сколько приносит один платящий пользователь? Работают ли изменения в тарифной сетке? | Action Logs.<br>Поля: `action_type='SUBSCRIPTION_PURCHASE'`, `status='SUCCESS'`, `product_id`, `user_id`, сумма из `metadata` | Push → Kafka → HDFS.<br>Batch, 1×/сут (нарастающим итогом за календарный месяц) | Норма: ARPPU в пределах ±10 % от медианы за 3 месяца<br>Warning: отклонение 10–25 %<br>Critical: падение > 25 % м/м | Дашборд Revenue.<br>Warning → разбор product-mix.<br>Critical → алерт `#zeer-product`, RB-03 «Падение ARPPU».<br>**Владелец: Product** | `RevenueBatchJob` |
| **A4** | **Churn Rate 30d**<br>Доля пользователей без единого события за 30 суток от активных на начало окна | Сколько пользователей мы теряем? Нужна ли кампания удержания? | Action Logs + Inject Logs.<br>Поля: `user_id`, `MAX(timestamp)` | Batch, 1×/нед (воскресенье 04:00).<br>Скользящее окно 30 сут | Норма: ≤ 10 % в месяц<br>Warning: 10–20 %<br>Critical: > 20 % | Дашборд Retention.<br>Warning → сегментация ушедших по продукту/версии лоадера.<br>Critical → алерт `#zeer-product`, RB-04 «Всплеск оттока»: проверить корреляцию с A2/C1/C3.<br>**Владелец: Product** | `ChurnBatchJob` |
| **A5** | **Эффективность промокодов**<br>Количество активаций и выручка в разрезе `promo_code` | Какие кампании окупаются? Не эксплуатируется ли код массово? | Action Logs.<br>Поля: `action_type='PROMO_ACTIVATE'`, `promo_code`, `user_id`, `status` | Push → Kafka → HDFS.<br>Batch, 1×/сут.<br>`GROUP BY promo_code` + `COUNT DISTINCT user_id` | Норма: доля активаций одного кода ≤ 30 % от всех<br>Warning: 30–50 % или активаций больше планового лимита кампании<br>Critical: > 50 % либо > 3 активаций одного кода с одного `metadata.ip` | Дашборд Campaigns.<br>Critical → алерт `#zeer-security`, RB-05 «Злоупотребление промокодом»: деактивировать код.<br>**Владелец: Product + Security** | `PromoBatchJob` |
| **A6** | **Key Activation Rate**<br>`COUNT(KEY_ACTIVATION SUCCESS) / COUNT(сгенерированных ключей)` | Доходят ли выданные ключи до активации? Нет ли потерь при выдаче? | Action Logs (`action_type='KEY_ACTIVATION'`, `key_code`, `status`) + Loader API `/generate_key_product` | Batch, 1×/сут.<br>Отношение за скользящее окно 7 сут | Норма: ≥ 85 %<br>Warning: 70–85 %<br>Critical: < 70 % | Дашборд Commerce.<br>Warning → проверить корректность доставки ключей.<br>Critical → алерт `#zeer-support`, RB-06 «Ключи не активируются».<br>**Владелец: Product + Support** | `CommerceBatchJob` |

---

### 2.2 Домен B — Продуктовые метрики (Usage & Engagement)

| ID | 1. Что измеряем | 2. Зачем / какой вопрос решает | 3. Откуда берём (лог, endpoint, поля) | 4. Как собираем | 5. Интерпретация (норма / warning / critical) | 6. Действие при отклонении | Job |
|----|-----------------|-------------------------------|---------------------------------------|-----------------|---------------------------------------------|---------------------------|-----|
| **B1** | **Популярность продукта**<br>Число успешных запусков в разрезе `product_id` | Какие продукты реально используют, а какие только покупают? Куда вкладывать разработку? | Inject Logs, `/log_inject_hacks`.<br>Поля: `product_id`, `status='SUCCESS'`, `user_id`, `timestamp` | Push → Kafka → HDFS.<br>Batch, 1×/час.<br>`GROUP BY product_id` + `COUNT DISTINCT user_id` | Норма: распределение стабильно, ни один продукт не теряет > 20 % запусков н/н<br>Warning: падение 20–40 % по продукту<br>Critical: падение > 40 % или полное отсутствие запусков > 6 ч при ненулевых подписках | Дашборд Product Usage.<br>Critical → алерт `#zeer-platform`, RB-07 «Продукт не запускается»: проверить C4/C6 по этому `product_id`.<br>**Владелец: Platform + Product** | `InjectionBatchJob` |
| **B2** | **Средняя длительность игровой сессии**<br>`AVG(time_in_game_seconds)`, а также P50/P90 | Насколько продукт удерживает в игре? Короткие сессии — признак нестабильности | Crash Logs, `/crash_logs`.<br>Поле: `time_in_game_seconds`, `product_id`, `cheat_version` | Push → Kafka → HDFS.<br>Batch, 1×/сут.<br>Перцентили по `product_id` и `cheat_version` | Норма: P50 ≥ 1200 с (20 мин)<br>Warning: P50 600–1200 с<br>Critical: P50 < 600 с либо падение > 40 % после релиза | Дашборд Product Usage.<br>Critical → алерт `#zeer-platform`, RB-08 «Короткие сессии»: сверить с C3 (регрессия версии).<br>**Владелец: Platform** | `CrashBatchJob` |
| **B3** | **Распределение платформ**<br>Доли `windows_name` / `os_info.build` среди активных пользователей | Какие сборки Windows поддерживать и на каких тестировать релиз? | Inject Logs (`windows_name`) + Crash Logs (`os_info.name`, `os_info.version`, `os_info.build`) | Batch, 1×/нед.<br>`GROUP BY windows_build`, доля от общего | Норма: топ-3 сборки покрывают ≥ 80 % аудитории<br>Warning: появление новой сборки с долей > 5 %<br>Critical: новая сборка > 10 % при повышенном крэш-рейте на ней | Дашборд Compatibility.<br>Warning → добавить сборку в матрицу тестирования.<br>Critical → RB-09 «Несовместимость со сборкой Windows».<br>**Владелец: Platform** | `PlatformBatchJob` |
| **B4** | **Adoption версии лоадера**<br>Доля пользователей на последней `loader_version` | Обновляются ли клиенты? Можно ли выводить старую версию из поддержки? | Inject Logs, `/inject_dll_preload`.<br>Поля: `loader_version`, `user_id` | Batch, 1×/сут.<br>Доля `DISTINCT user_id` на актуальной версии за 7 сут | Норма: ≥ 80 % на последней версии через 14 суток после релиза<br>Warning: 60–80 %<br>Critical: < 60 % | Дашборд Release Health.<br>Warning → напоминание об обновлении в UI.<br>Critical → алерт `#zeer-platform`, RB-10 «Клиенты не обновляются»: проверить механизм автообновления.<br>**Владелец: Platform** | `InjectionBatchJob` |
| **B5** | **Связность Steam-аккаунтов**<br>`COUNT(DISTINCT steam_id)` на одного `user_id` за 30 сут | Сколько игровых аккаунтов на одну учётку? База для метрики шаринга D1 | Inject Logs + Crash Logs.<br>Поля: `user_id`, `steam_id` | Batch, 1×/сут.<br>`COUNT DISTINCT steam_id` по `user_id`, окно 30 сут | Норма: ≤ 2 Steam-аккаунта на пользователя<br>Warning: 3–5<br>Critical: > 5 | Дашборд Security Overview.<br>Warning → в наблюдение.<br>Critical → передать в D1, RB-11 «Мультиаккаунт».<br>**Владелец: Security** | `SecurityBatchJob` |

---

### 2.3 Домен C — Технические метрики (Stability & Performance)

| ID | 1. Что измеряем | 2. Зачем / какой вопрос решает | 3. Откуда берём (лог, endpoint, поля) | 4. Как собираем | 5. Интерпретация (норма / warning / critical) | 6. Действие при отклонении | Job |
|----|-----------------|-------------------------------|---------------------------------------|-----------------|---------------------------------------------|---------------------------|-----|
| **C1** | **Crash Rate**<br>`COUNT(crash) / COUNT(inject SUCCESS) × 1000` — крэшей на 1000 инжектов | Насколько стабилен продукт? Главный индикатор качества релиза | Crash Logs (`/crash_logs`) ÷ Inject Logs (`/log_inject_hacks`, `status='SUCCESS'`) | Streaming, окно 5 мин (watermark 2 мин) + batch-сверка 1×/сут | Норма: < 20 ‰ (2 %)<br>Warning: 20–50 ‰ (2–5 %)<br>Critical: > 50 ‰ (5 %) — порог из `LOG_ANALYTICS_DESIGN.md` | Grafana Stability + алерт.<br>Warning → разбор в течение рабочего дня.<br>Critical → немедленный алерт `#zeer-platform`, RB-12 «Всплеск крэшей»: кандидат на откат релиза.<br>**Владелец: Platform** | `CrashStreamingJob` + `CrashBatchJob` |
| **C2** | **Распределение по exception_code**<br>Топ кодов исключений и их доли | Какая причина крэшей доминирует? Появился ли новый класс сбоя? | Crash Logs.<br>Поля: `exception_code`, `exception_description`, `full_stack_trace` | Streaming, окно 5 мин + batch 1×/сут.<br>`GROUP BY exception_code`, категоризация по таксономии Windows | Норма: распределение соответствует базовой линии за 30 сут<br>Warning: доля одного кода выросла > 1.5×<br>Critical: появился код, отсутствовавший в базовой линии, с долей > 10 % | Grafana Stability.<br>Critical → алерт `#zeer-platform`, RB-13 «Новый класс крэша»: завести дефект со stack trace.<br>**Владелец: Platform** | `CrashStreamingJob` |
| **C3** | **Crash Rate по версиям**<br>Crash Rate в разрезе `cheat_version` × `game_version` | Не сломал ли новый релиз стабильность? Регрессия после обновления игры? | Crash Logs.<br>Поля: `cheat_version`, `game_version`, `exception_code` | Batch, 1×/час.<br>Сравнение текущей версии с предыдущей за сопоставимое окно | Норма: рост ≤ 10 % относительно предыдущей версии<br>Warning: рост 10–50 %<br>Critical: рост > 50 % или > 2× на новой версии | Grafana Release Health.<br>Critical → алерт `#zeer-platform`, RB-14 «Регрессия релиза»: блокировать раскатку, откатить версию.<br>**Владелец: Platform** | `CrashBatchJob` |
| **C4** | **Injection Success Rate**<br>`COUNT(status='SUCCESS') / COUNT(*)` по Inject Logs | Работает ли основная функция продукта? Ключевая North Star метрика | Inject Logs, `/inject_dll_preload` + `/log_inject_hacks`.<br>Поля: `status`, `error_code` | Streaming, окно 5 мин (watermark 2 мин) + batch 1×/сут | Норма: ≥ 98 % (North Star)<br>Warning: 95–98 %<br>Critical: < 95 % — порог из `LOG_ANALYTICS_DESIGN.md` | Grafana Platform + алерт.<br>Warning → разбор по `error_code`.<br>Critical → немедленный алерт `#zeer-platform`, RB-15 «Инжект не проходит»: проверить античиты (C7) и версии (B4).<br>**Владелец: Platform** | `InjectionStreamingJob` |
| **C5** | **Задержка инжекта**<br>P50 / P95 / P99 по `duration_ms` | Быстро ли запускается продукт? Не деградирует ли производительность? | Inject Logs.<br>Поле: `duration_ms`, `injection_method`, `product_id` | Streaming, окно 5 мин.<br>Приближённые перцентили (`approx_percentile`) | Норма: P95 < 3000 мс, P99 < 5000 мс<br>Warning: P99 5000–8000 мс<br>Critical: P99 > 8000 мс | Grafana Platform.<br>Warning → профилирование метода инжекта.<br>Critical → алерт `#zeer-platform`, RB-16 «Деградация времени инжекта».<br>**Владелец: Platform** | `InjectionStreamingJob` |
| **C6** | **Прохождение стадий инжекта**<br>Конверсия `PRELOAD → INJECT → POST_INJECT → HEARTBEAT` | На каком именно шаге ломается запуск? Лицензия, маппинг DLL или удержание? | Inject Logs.<br>Поля: `inject_stage`, `status`, `error_code`, `inject_session_id` | Batch, 1×/час + streaming-срез 5 мин.<br>Воронка по `inject_session_id` | Норма: каждый переход ≥ 95 %<br>Warning: любой переход 85–95 %<br>Critical: любой переход < 85 % | Grafana Platform.<br>Critical → алерт `#zeer-platform`, RB-17 «Обрыв на стадии инжекта» с указанием стадии.<br>**Владелец: Platform** | `InjectionBatchJob` |
| **C7** | **Структура отказов Loader API**<br>Доли `error_code ∈ {HWID_MISMATCH, NO_LICENSE, FROZEN, WRONG_CREDENTIALS, BANNED, ANTICHEAT_DETECTED, INJECTION_FAILED}` | Отказ вызван бизнес-правилом (нет лицензии) или техническим сбоем? Куда направлять поддержку? | Inject Logs, `status ∈ {FAILED, BLOCKED}`.<br>Поле: `error_code` | Streaming, окно 5 мин + batch 1×/сут.<br>`GROUP BY error_code` | Норма: доля технических отказов (`INJECTION_FAILED`) < 2 % от всех попыток<br>Warning: 2–5 %<br>Critical: > 5 % либо рост `ANTICHEAT_DETECTED` > 3× за сутки | Grafana Platform.<br>Critical (`ANTICHEAT_DETECTED`) → алерт `#zeer-platform` **и** `#zeer-security`, RB-18 «Детект античита»: приостановить раскатку.<br>**Владелец: Platform** | `InjectionStreamingJob` |

---

### 2.4 Домен D — Безопасность и фрод (Security & Fraud)

| ID | 1. Что измеряем | 2. Зачем / какой вопрос решает | 3. Откуда берём (лог, endpoint, поля) | 4. Как собираем | 5. Интерпретация (норма / warning / critical) | 6. Действие при отклонении | Job |
|----|-----------------|-------------------------------|---------------------------------------|-----------------|---------------------------------------------|---------------------------|-----|
| **D1** | **Шаринг аккаунта**<br>`COUNT(DISTINCT hwid)` на `user_id` за 7 сут | Одну подписку используют несколько человек? Прямые потери выручки | Inject Logs.<br>Поля: `user_id`, `hwid`, `status='SUCCESS'`, `timestamp` | Streaming (детект в near-real-time) + batch 1×/сут для подтверждения.<br>Окно 7 сут | Норма: ≤ 2 HWID<br>Warning: 3 HWID — порог из `LOG_ANALYTICS_DESIGN.md`<br>Critical: > 3 HWID | Дашборд Security.<br>Warning → в очередь на проверку.<br>Critical → алерт `#zeer-security`, RB-19 «Шаринг аккаунта»: сброс привязки или блокировка.<br>**Владелец: Security** | `SecurityStreamingJob` |
| **D2** | **Подбор учётных данных**<br>Число `WRONG_CREDENTIALS` и неуспешных `LOGIN` с одного `ip` за 10 мин | Идёт ли брутфорс или credential stuffing? | Action Logs (`action_type='LOGIN'`, `status='FAILED'`, `metadata.ip`) + Inject Logs (`error_code='WRONG_CREDENTIALS'`, `ip`) | Streaming, окно 10 мин (watermark 2 мин).<br>`COUNT` по `ip` | Норма: ≤ 5 неудач с IP за 10 мин<br>Warning: 6–20<br>Critical: > 20 либо > 10 разных `user_id` с одного IP | Алерт `#zeer-security`.<br>Warning → временный rate limit.<br>Critical → блокировка IP, RB-20 «Брутфорс».<br>**Владелец: Security** | `SecurityStreamingJob` |
| **D3** | **Частота HWID-несоответствий**<br>Доля `error_code='HWID_MISMATCH'` от всех попыток инжекта | Массовая смена железа, попытка обхода привязки или сбой сбора HWID? | Inject Logs.<br>Поля: `error_code='HWID_MISMATCH'`, `user_id`, `hwid` | Streaming, окно 5 мин + batch 1×/сут | Норма: < 1 % попыток<br>Warning: 1–3 %<br>Critical: > 3 % либо > 5 несоответствий у одного `user_id` за сутки | Дашборд Security.<br>Critical → алерт `#zeer-security`, RB-21 «Аномалия HWID»: отличить сбой сбора HWID от обхода.<br>**Владелец: Security + Platform** | `SecurityStreamingJob` |
| **D4** | **Обход блокировки**<br>Появление нового `user_id` с `hwid` / `steam_id` / `ip`, ранее принадлежавшими заблокированному аккаунту | Возвращается ли забаненный пользователь под новой учёткой? | Action Logs (`action_type='ADMIN_ACTION'` c баном, `target_user_id`) + Inject Logs (`hwid`, `steam_id`, `ip`) | Batch, 1×/сут.<br>Join нового `user_id` с историей идентификаторов забаненных за 90 сут | Норма: совпадений нет<br>Warning: совпадение по одному идентификатору (`ip`)<br>Critical: совпадение по `hwid` или `steam_id` | Дашборд Security.<br>Critical → алерт `#zeer-security`, RB-22 «Обход бана»: блокировка нового аккаунта.<br>**Владелец: Security** | `SecurityBatchJob` |
| **D5** | **Геоаномалия (невозможная скорость)**<br>Смена `location` у одного `user_id` за интервал, недостижимый физически | Одновременное использование учётки из разных регионов — признак шаринга или компрометации | Inject Logs.<br>Поля: `user_id`, `location`, `ip`, `timestamp` | Streaming, окно 1 ч.<br>Сравнение последовательных событий по `user_id` | Норма: смена региона не чаще 1 раза за 24 ч<br>Warning: 2 региона за 6 ч<br>Critical: 2 региона за интервал < 1 ч | Дашборд Security.<br>Critical → алерт `#zeer-security`, RB-23 «Геоаномалия»: сверить с D1.<br>**Владелец: Security** | `SecurityStreamingJob` |
| **D6** | **Активность административных действий**<br>Число `ADMIN_ACTION` в разрезе `admin_user_id` | Не злоупотребляет ли администратор правами? Аудит привилегированных операций | Action Logs.<br>Поля: `action_type='ADMIN_ACTION'`, `admin_user_id`, `target_user_id`, `action_category='ADMIN'` | Batch, 1×/сут.<br>`GROUP BY admin_user_id`, сравнение с базовой линией за 30 сут | Норма: в пределах 2σ от личной базовой линии<br>Warning: 2–3σ<br>Critical: > 3σ либо массовые операции над > 20 `target_user_id` за час | Журнал аудита + дашборд Security.<br>Critical → алерт `#zeer-security`, RB-24 «Аномалия админ-активности».<br>**Владелец: Security** | `SecurityBatchJob` |

---

### 2.5 Домен E — Метрики самого конвейера (Data Platform SLA)

Домен обеспечивает доверие к метрикам A–D: если конвейер деградировал, остальные показатели считать нельзя.

| ID | 1. Что измеряем | 2. Зачем / какой вопрос решает | 3. Откуда берём (лог, endpoint, поля) | 4. Как собираем | 5. Интерпретация (норма / warning / critical) | 6. Действие при отклонении | Job |
|----|-----------------|-------------------------------|---------------------------------------|-----------------|---------------------------------------------|---------------------------|-----|
| **E1** | **Лаг Kafka-консьюмера**<br>`consumer_lag` группы `flume-consumer` по трём топикам | Успевает ли конвейер за потоком событий? Не копится ли необработанный backlog? | Kafka JMX → Prometheus, брокеры `zeer-worker1..3:9092`.<br>Группа `flume-consumer` | Pull, Prometheus, интервал 15 с.<br>`MAX(lag)` по партициям | Норма: < 1 000 сообщений<br>Warning: 1 000–10 000<br>Critical: > 10 000 либо непрерывный рост > 15 мин | Grafana Data Platform.<br>Critical → алерт `#zeer-dataeng`, RB-25 «Отставание консьюмера»: увеличить число Flume-агентов.<br>**Владелец: Data Eng** | Prometheus (вне Spark) |
| **E2** | **Time to Insight**<br>Задержка от записи события до появления в витрине | Насколько свежи данные на дашбордах? North Star конвейера | Метка `timestamp` события против времени коммита микробатча Structured Streaming | Streaming, вычисляется в каждом микробатче | Норма: < 5 мин (North Star)<br>Warning: 5–15 мин<br>Critical: > 15 мин | Grafana Data Platform.<br>Critical → алерт `#zeer-dataeng`, RB-26 «Деградация свежести данных»: проверить E1 и очередь YARN.<br>**Владелец: Data Eng** | все streaming-job'ы |
| **E3** | **Доля записей, прошедших контроль качества**<br>`COUNT(valid) / COUNT(total)` на Data Quality Gate перед слоем Core | Можно ли доверять витринам? Не изменилась ли схема логов незаметно? | Результаты DQ-проверок (правила из `LOG_ANALYTICS_DESIGN.md`, раздел 5.2) | Batch, на каждом прогоне ELT | Норма: ≥ 99 %<br>Warning: 95–99 %<br>Critical: < 95 % | Grafana Data Platform.<br>Warning → карантин отклонённых записей в Quarantine-зону.<br>Critical → остановка публикации в Mart, алерт `#zeer-dataeng`, RB-27 «Отказ контроля качества».<br>**Владелец: Data Eng** | `DataQualityGateJob` |
| **E4** | **Доля пропущенных запусков job'ов**<br>Число job'ов, не завершившихся в срок SLA | Считаются ли метрики вообще? Не молчит ли дашборд из-за упавшего job'а? | YARN REST API `/ws/v1/cluster/apps`, статусы приложений | Pull, Prometheus, интервал 60 с | Норма: 100 % job'ов в SLA<br>Warning: 1 пропуск в сутки<br>Critical: ≥ 2 пропуска подряд или падение streaming-job'а | Grafana Data Platform.<br>Critical → алерт `#zeer-dataeng`, RB-28 «Сбой job'а»: перезапуск с checkpoint.<br>**Владелец: Data Eng** | Prometheus + YARN API |

---

## 3. Сводка каталога

| Домен | Метрик | Режим вычисления | Основной потребитель |
|-------|--------|------------------|---------------------|
| A — Бизнес | 6 | Batch (сут / нед) | Product |
| B — Продукт | 5 | Batch (час / сут / нед) | Platform, Product |
| C — Технические | 7 | Streaming (5 мин) + Batch | Platform |
| D — Безопасность | 6 | Streaming + Batch | Security |
| E — Конвейер | 4 | Prometheus + Streaming | Data Eng |
| **Итого** | **28** | | |

### 3.1 Распределение по режимам обработки

| Режим | Метрики | Обоснование выбора |
|-------|---------|-------------------|
| **Structured Streaming**, окно 5–10 мин | C1, C2, C4, C5, C7, D1, D2, D3, D5, E2 | Требуют реакции в пределах минут: деградация инжекта, всплеск крэшей, брутфорс |
| **Spark Batch**, 1×/час | B1, B4, C3, C6 | Нужен более широкий контекст, но решение принимается в тот же день |
| **Spark Batch**, 1×/сут | A1, A2, A3, A5, A6, B2, D4, D6, E3 | Когортные и накопительные показатели; суточное окно — естественная единица |
| **Spark Batch**, 1×/нед | A4, B3 | Медленно меняющиеся характеристики аудитории |
| **Prometheus (pull)** | E1, E4 | Инфраструктурная телеметрия, берётся из JMX и YARN REST API, минуя HDFS |

### 3.2 Покрытие North Star метрик

Метрики из раздела 7 `LOG_ANALYTICS_DESIGN.md` закрыты каталогом полностью:

| North Star | Целевое значение | Метрика каталога |
|-----------|------------------|------------------|
| Injection Success Rate | > 98 % | **C4** |
| Crash-Free Sessions | > 95 % | **C1** (обратная величина) |
| Time to Insight | < 5 мин | **E2** |
| Fraud Detection Rate | > 90 % | **D1**, **D3**, **D4** |
| Model Prediction Accuracy | F1 > 0.85 | вне границ каталога — этап ML |

---

## 4. Матрица трассируемости «метрика → источник → витрина»

| Метрика | Топик Kafka | Слой Core (Hive) | Витрина Mart |
|---------|-------------|------------------|--------------|
| A1, A2, A3, A5, A6 | `zeer-user-events` | `core_user_actions` | `mart_business_daily` |
| A4 | `zeer-user-events` + `zeer-injection-events` | `core_user_actions`, `core_injections` | `mart_retention_weekly` |
| B1, B4 | `zeer-injection-events` | `core_injections` | `mart_product_usage_hourly` |
| B2 | `zeer-crash-events` | `core_crashes` | `mart_product_usage_daily` |
| B3 | `zeer-injection-events` + `zeer-crash-events` | `core_injections`, `core_crashes` | `mart_platform_weekly` |
| B5, D1, D3, D4, D5, D6 | `zeer-injection-events` + `zeer-user-events` | `core_injections`, `core_user_actions` | `mart_security_events` |
| C1, C2, C3 | `zeer-crash-events` | `core_crashes` | `mart_stability_5min`, `mart_stability_daily` |
| C4, C5, C6, C7 | `zeer-injection-events` | `core_injections` | `mart_injection_5min`, `mart_injection_hourly` |
| D2 | `zeer-user-events` + `zeer-injection-events` | `core_user_actions`, `core_injections` | `mart_security_events` |
| E1, E4 | — (JMX / YARN REST) | — | Prometheus TSDB |
| E2, E3 | все топики | служебные таблицы DQ | `mart_pipeline_health` |

---

## 5. Ограничения каталога

1. **Стоимость покупки** (метрика A3) в схеме Action Logs отдельным полем не объявлена — берётся из `metadata`. При реализации требуется зафиксировать поле `product_cost` в контракте данных, иначе A3 не вычисляется.
2. **`anticheat_detected`** — массив; при подсчёте C7 запись с несколькими детектами учитывается в каждой категории, поэтому сумма долей превышает 100 %. Это осознанное решение, в витрине фиксируется отдельной пометкой.
3. **Geo по IP** (метрика D5) зависит от точности справочника GeoIP; при использовании приватных диапазонов (`127.0.0.1`, `192.168.*`) метрика не считается и запись отбрасывается на DQ Gate.
4. **Пороги** первой версии заданы экспертно на основе `LOG_ANALYTICS_DESIGN.md`. После накопления 30 суток исторических данных подлежат пересчёту по фактическим перцентилям.

---

*Каталог является входными данными для `DATA_PIPELINE_DESIGN.md` — проектирования конвейера сбора, хранения, преобразования и обработки.*
