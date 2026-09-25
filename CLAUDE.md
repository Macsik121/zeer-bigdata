# CLAUDE.md — Инструкции для Claude Code

## О проекте

**Zeer-BigData** — дипломный проект по обработке больших данных логов маркетплейса Zeer Marketplace. Проект включает физическую инфраструктуру из 4 узлов (1 Master + 3 Worker) с Apache Hadoop и экосистемой больших данных для сбора, обработки и анализа логов системы.

### Структура проекта

```
zeer-bigdata/
├── .claude/
│   └── agents/
│       ├── big-data-engineer.md              # Специализированный агент для Big Data
│       └── marketplace-analytics-expert.md   # Эксперт по аналитике маркетплейса
├── analytics/                       # Аналитика и метрики
│   ├── LOG_ANALYTICS_DESIGN.md     # Дизайн системы анализа логов
│   ├── METRICS_CATALOG.md          # Каталог метрик
│   └── DATA_PIPELINE_DESIGN.md     # Архитектура пайплайна данных
├── api/                             # Node.js API для сбора событий
│   ├── package.json
│   └── tsconfig.json
├── configs/                         # Конфигурации
│   └── adcm-service-config.md      # Конфигурация ADCM
└── docs/                            # Документация
    ├── CLICKFLOW_ANALYTICS_IMPLEMENTATION_PLAN.md  # План имплементации clickflow
    ├── servers-infrastructure-software-implementation-plan.md  # План настройки серверов
    ├── DIPLOMA_DOCUMENTATION_STRUCTURE.md          # Структура дипломной работы
    ├── PHYSICAL_SAFETY_AND_COOLING.md              # Физическая безопасность
    ├── network-architecture.txt                    # Сетевая архитектура
    ├── NODEJS_FLUME_INTEGRATION.md                 # Интеграция Node.js и Flume
    └── code_according_to_servers_infrastructure_soft.txt  # Код инфраструктуры
```

## Технологический стек

### Инфраструктура
- **Операционная система**: Ubuntu Server 22.04 LTS
- **Кластер**: 4 узла (1 Master: 192.168.1.5, 3 Workers: 192.168.1.10-12)
- **Управление кластером**: ArenaData Cluster Manager (ADCM) + ArenaData Hyperwave (ADH)

### Hadoop Ecosystem
- **HDFS**: Распределённое хранилище
- **YARN**: Управление ресурсами кластера
- **ZooKeeper**: Координация распределённых процессов
- **Flume**: Сбор и агрегация логов в реальном времени
- **Kafka**: Потоковая обработка событий
- **Impala**: MPP SQL-движок для аналитики
- **Hive**: SQL-on-Hadoop
- **Spark**: Обработка больших данных in-memory

### Приложения
- **Node.js API**: Express-сервер для сбора событий от фронтенда
- **ClickHouse/TimescaleDB**: Аналитическая БД для clickflow метрик
- **Prometheus/Grafana**: Мониторинг кластера

## Архитектура системы

### Узлы кластера

**Master Node (zeer-master, 192.168.1.5)**:
- NameNode (управление метаданными HDFS)
- ResourceManager (управление ресурсами YARN)
- Hive Metastore
- Spark History Server
- Node.js API Collector
- ADCM Server
- Prometheus/Grafana

**Worker Nodes (zeer-worker1/2/3, 192.168.1.10-12)**:
- DataNode (хранение блоков данных)
- NodeManager (выполнение контейнеров)
- Spark Worker
- Kafka Broker
- ZooKeeper
- Flume Agent

### Типы логов для обработки
1. **Action Logs**: Действия пользователей в маркетплейсе
2. **Crash Logs**: Логи сбоев системы
3. **Inject Logs**: Логи работы загрузчика

## Работа с проектом

### Использование специализированного агента

Для задач, связанных с Big Data инфраструктурой, используй агента `big-data-engineer`:

```bash
# Вызов через интерфейс Claude Code
/agent big-data-engineer "Настроить Flume для сбора логов Nginx"
```

Этот агент специализируется на:
- Конфигурации Hadoop экосистемы
- Настройке HDFS, YARN, ZooKeeper
- Работе с Flume, Kafka, Spark
- Архитектуре распределённых систем
- Проектировании пайплайнов данных

### Ключевые документы

#### 1. План реализации clickflow аналитики
**Файл**: `docs/CLICKFLOW_ANALYTICS_IMPLEMENTATION_PLAN.md`

Содержит архитектуру системы отслеживания пользовательского пути:
- Схема базы данных событий (ClickHouse)
- API Gateway для валидации и обогащения событий
- Frontend трекер с батчингом
- Воронки конверсии: landing → просмотр товара → покупка → активация

#### 2. План инфраструктуры серверов
**Файл**: `docs/servers-infrastructure-software-implementation-plan.md`

Полный план настройки 4-узлового кластера:
- **Phase 1**: Установка ОС, настройка сети, SSH, Java
- **Phase 2**: Установка ADCM
- **Phase 3**: Развёртывание ADH (Hadoop + ecosystem)
- **Phase 4**: Установка Flume, Kafka, Impala
- **Phase 5**: Настройка мониторинга
- **Phase 6**: Развёртывание Node.js API
- **Phase 7**: Тестирование и валидация

#### 3. Интеграция Node.js и Flume
**Файл**: `docs/NODEJS_FLUME_INTEGRATION.md`

Детали интеграции API-сервера сбора событий с Flume:
- HTTP Source для приёма событий
- Memory Channel для буферизации
- HDFS Sink для записи в распределённое хранилище
- Avro сериализация

### Соглашения о коде

#### Node.js API
- **Язык**: JavaScript (Node.js 14.x+)
- **Фреймворк**: Express.js
- **Стиль**: CommonJS модули
- **Валидация**: JSON Schema для входящих событий
- **Логирование**: Winston или аналогичный

#### Hadoop конфигурации
- **Формат**: XML для core-site.xml, hdfs-site.xml, yarn-site.xml
- **Комментарии**: Обязательны для нестандартных параметров
- **Версионирование**: Храни бэкапы в `configs/`

#### Shell скрипты
- **Shebang**: `#!/bin/bash`
- **Стиль**: Следуй Google Shell Style Guide
- **Проверка**: ShellCheck перед коммитом

### Работа с документацией

#### Структура дипломной работы
**Файл**: `docs/DIPLOMA_DOCUMENTATION_STRUCTURE.md`

Содержит подробную структуру дипломной документации. При написании разделов:
1. Сначала создай черновик самостоятельно
2. Затем сверься с этим файлом для дополнений
3. Не копируй текст напрямую — используй как справочник

### Git workflow

#### Ветки
- `main` — стабильная версия документации и конфигураций
- `dev` — разработка новых фич
- `feature/*` — конкретные фичи

#### Коммиты
- Пиши на русском или английском последовательно
- Формат: `<тип>: <краткое описание>`
- Типы: `docs`, `config`, `feat`, `fix`, `refactor`

Примеры:
```
docs: добавить план интеграции Flume с Kafka
config: обновить настройки HDFS replication factor
feat: реализовать Node.js API для сбора событий
```

### Важные замечания

#### Физическая безопасность
**Файл**: `docs/PHYSICAL_SAFETY_AND_COOLING.md`

Система из 3 ПК требует:
- Охлаждение: 3 кулера на вытяжку, контроль температуры
- Пожарная безопасность: датчики дыма, огнетушитель
- Электрическая безопасность: ИБП, заземление, защита от перегрузки
- Физическая защита: крепление корпусов, защита кабелей

#### Сетевая архитектура
**Файл**: `docs/network-architecture.txt`

Все узлы в одной подсети 192.168.1.0/24:
- Master — координатор и точка входа
- Workers — равноправные узлы хранения и обработки
- Heartbeat между NameNode и DataNodes: 3 сек
- Блок репликация: фактор 2 (минимум для 3 DataNodes)

### Команды для быстрого старта

#### Проверка статуса кластера
```bash
# На Master ноде
hdfs dfsadmin -report        # Статус HDFS
yarn node -list              # Статус YARN узлов
hdfs fsck / -files -blocks   # Проверка целостности данных
```

#### Запуск Flume агента
```bash
flume-ng agent \
  --conf /etc/flume/conf \
  --conf-file /etc/flume/conf/flume-agent.conf \
  --name agent1 \
  -Dflume.root.logger=INFO,console
```

#### Мониторинг логов
```bash
# HDFS NameNode logs
tail -f /var/log/hadoop-hdfs/hadoop-hdfs-namenode-zeer-master.log

# Flume logs
tail -f /var/log/flume/flume.log
```

## Помощь и поддержка

### Порядок работы с Claude Code

1. **Изучение контекста**: Прежде чем предлагать решения, прочитай соответствующие документы из `docs/` и `analytics/`

2. **Использование агента**: Для задач Big Data используй специализированного агента `big-data-engineer`

3. **Верификация**: Перед внесением изменений в конфигурации — проверь совместимость с текущей архитектурой

4. **Документирование**: Все изменения в инфраструктуре должны быть задокументированы

### Типичные задачи

**Настройка нового компонента Hadoop**:
```
1. Читай servers-infrastructure-software-implementation-plan.md
2. Используй big-data-engineer агента для генерации конфигурации
3. Создай файл конфигурации в configs/
4. Обнови документацию
```

**Разработка пайплайна данных**:
```
1. Изучи DATA_PIPELINE_DESIGN.md
2. Определи источник → трансформации → целевое хранилище
3. Спроектируй схему данных
4. Реализуй код обработки (Spark/Hive)
5. Задокументируй в docs/
```

**Добавление новых метрик**:
```
1. Открой METRICS_CATALOG.md
2. Определи метрику: имя, описание, формула, источник
3. Добавь в каталог
4. Реализуй в аналитическом слое
```

---

## Принципы работы

### Будь проактивным
- Предлагай оптимизации архитектуры
- Указывай на потенциальные проблемы масштабирования
- Рекомендуй best practices Hadoop ecosystem

### Будь конкретным
- Предоставляй готовые конфигурации, а не общие советы
- Включай примеры команд с реальными путями и параметрами
- Ссылайся на конкретные разделы документов

### Будь последовательным
- Следуй существующим соглашениям проекта
- Используй терминологию из документации
- Сохраняй структуру каталогов и стиль кода

---

**Версия**: 1.0  
**Дата создания**: 2026-09-25  
**Автор**: Максим Салий  
**Email**: akmaksa65@gmail.com
