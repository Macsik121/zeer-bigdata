# Node.js + Apache Flume Integration для Zeer Marketplace

## Обзор архитектуры

Данный документ описывает интеграцию Node.js сервера с Apache Flume для сбора и обработки логов в системе Zeer marketplace.

### Типы логов

1. **Action Logs** - действия пользователей в веб-интерфейсе (GraphQL API)
2. **Crash Logs** - ошибки при запуске/работе игрового клиента (REST API)
3. **Inject Logs** - логи инжектирования во время игры (REST API)

### Архитектура потока данных

```
┌───────────────────────────────────────────────────────────────────┐
│                        MasterNode (zeer-master)                   │
│  ┌───────────────────┐    ┌────────────────┐    ┌──────────────┐  │
│  │   Node.js Server  │    │ Flume Agent    │    │   NameNode   │  │
│  │  (Express/GraphQL)│───▶│  (Avro Source)│──▶ │    (HDFS)    │  │
│  │  Port: 3000       │    │  Port: 41414   │    │  Port: 8020  │  │
│  └───────────────────┘    └────────────────┘    └──────────────┘  │
│         │                       │                                 │
│         │ HTTP/Avro             │ HDFS Write                      │
│         │                       │                                 │
└─────────┼───────────────────────┼─────────────────────────────────┘
          │                       │
          │                       ▼
          │              ┌─────────────────────┐
          │              │   DataNodes (×3)    │
          │              │  zeer-worker1-3     │
          │              │  /zeer/logs/        │
          │              │  - action_logs/     │
          │              │  - crash_logs/      │
          │              │  - inject_logs/     │
          │              └─────────────────────┘
          │
          ▼
  ┌──────────────────────┐
  │  Web Client/         │
  │  Game Loader         │
  │  (GraphQL/REST API)  │
  └──────────────────────┘
```

---

## Ответы на ваши вопросы

### 1. Правильно ли размещать Node.js на MasterNode?

**Ответ: ДА, но с оговорками для продакшена**

**Для домашнего кластера/дипломной работы (ваш случай):**
- ✅ **Допустимо** - MasterNode имеет достаточно ресурсов (ноутбук)
- ✅ Упрощает архитектуру - все в одном месте
- ✅ Низкая сетевая задержка между Node.js → Flume → NameNode
- ✅ Легче отладка и мониторинг

**Для production:**
- ❌ **Не рекомендуется** - MasterNode должен быть изолирован
- ✅ Лучше использовать отдельный Edge Node или Gateway Node
- ✅ Load Balancer перед несколькими API серверами

**Рекомендация для вас:** 
Размещайте Node.js на MasterNode, но **ограничьте ресурсы** через systemd:

```ini
[Service]
MemoryMax=2G
CPUQuota=25%
```

---

### 2. Как настроить Apache Flume для получения логов от Node.js?

**Рекомендуемый подход: Avro Source + Memory Channel + HDFS Sink**

#### Почему Avro, а не HTTP или netcat?

| Метод | Плюсы | Минусы | Рекомендация |
|-------|-------|--------|--------------|
| **Avro RPC** | Binary, компактный, быстрый, схема данных | Нужен Avro клиент в Node.js | ✅ **Рекомендовано** |
| HTTP Source | Простой, RESTful | Больше overhead, текстовый | ⚠️ Для прототипов |
| Netcat (TCP) | Очень простой | Нет гарантий доставки, нет схемы | ❌ Не рекомендуется |
| Kafka | Гарантии доставки, буферизация | Избыточно для вашего случая | ⚠️ Если нужна очередь |

**Выбор: Avro Source** - оптимальный баланс производительности и надежности.

---

### 3. Как обрабатывать логи на Node.js перед отправкой?

**Обработка на стороне Node.js:**

1. **Валидация структуры** - проверка обязательных полей
2. **Enrichment (обогащение):**
   - Timestamp (ISO 8601)
   - Server hostname
   - Log type (action/crash/inject)
   - Session ID (если есть)
3. **Нормализация** - приведение к единому формату JSON
4. **Буферизация** - батчинг для эффективной отправки
5. **Retry механизм** - повторная отправка при ошибках

**НЕ делайте на Node.js:**
- ❌ Агрегацию (это для Spark)
- ❌ Сложные трансформации (это для ETL pipeline)
- ❌ Долгое хранение в памяти (риск потери данных)

---

## Конфигурация Apache Flume

### Сценарий 1: Один Flume Agent на MasterNode (Рекомендовано для вас)

**Файл:** `/opt/flume/conf/zeer-logs-collector.conf`

```properties
# Agent name
zeer-collector.sources = avro-source
zeer-collector.channels = memory-channel
zeer-collector.sinks = hdfs-sink

# ==================================================
# Source: Avro RPC (принимает от Node.js)
# ==================================================
zeer-collector.sources.avro-source.type = avro
zeer-collector.sources.avro-source.bind = 0.0.0.0
zeer-collector.sources.avro-source.port = 41414
zeer-collector.sources.avro-source.threads = 8
zeer-collector.sources.avro-source.compression-type = deflate

# Interceptors для enrichment
zeer-collector.sources.avro-source.interceptors = timestamp hostname logtype
zeer-collector.sources.avro-source.interceptors.timestamp.type = timestamp
zeer-collector.sources.avro-source.interceptors.hostname.type = host
zeer-collector.sources.avro-source.interceptors.hostname.useIP = false

# Custom interceptor для определения типа лога
zeer-collector.sources.avro-source.interceptors.logtype.type = regex_extractor
zeer-collector.sources.avro-source.interceptors.logtype.regex = "log_type":"(\\w+)"
zeer-collector.sources.avro-source.interceptors.logtype.serializers = s1
zeer-collector.sources.avro-source.interceptors.logtype.serializers.s1.name = logType

# ==================================================
# Channel: Memory (быстрый, но не персистентный)
# ==================================================
zeer-collector.channels.memory-channel.type = memory
zeer-collector.channels.memory-channel.capacity = 10000
zeer-collector.channels.memory-channel.transactionCapacity = 1000

# Для production рекомендуется File Channel:
# zeer-collector.channels.file-channel.type = file
# zeer-collector.channels.file-channel.checkpointDir = /var/flume/checkpoint
# zeer-collector.channels.file-channel.dataDirs = /var/flume/data

# ==================================================
# Sink: HDFS (запись на DataNodes)
# ==================================================
zeer-collector.sinks.hdfs-sink.type = hdfs
zeer-collector.sinks.hdfs-sink.hdfs.path = hdfs://zeer-master:8020/zeer/logs/%{logType}/%Y/%m/%d
zeer-collector.sinks.hdfs-sink.hdfs.filePrefix = events
zeer-collector.sinks.hdfs-sink.hdfs.fileSuffix = .log
zeer-collector.sinks.hdfs-sink.hdfs.inUsePrefix = _tmp_
zeer-collector.sinks.hdfs-sink.hdfs.inUseSuffix = .tmp

# Rotation policy
zeer-collector.sinks.hdfs-sink.hdfs.rollInterval = 600        # 10 минут
zeer-collector.sinks.hdfs-sink.hdfs.rollSize = 134217728      # 128MB
zeer-collector.sinks.hdfs-sink.hdfs.rollCount = 0             # Unlimited events
zeer-collector.sinks.hdfs-sink.hdfs.idleTimeout = 300         # 5 минут

# File format
zeer-collector.sinks.hdfs-sink.hdfs.fileType = DataStream
zeer-collector.sinks.hdfs-sink.hdfs.writeFormat = Text
zeer-collector.sinks.hdfs-sink.hdfs.batchSize = 1000
zeer-collector.sinks.hdfs-sink.hdfs.threadsPoolSize = 10

# Compression (опционально)
# zeer-collector.sinks.hdfs-sink.hdfs.codeC = snappy

# HDFS replication
zeer-collector.sinks.hdfs-sink.hdfs.minBlockReplicas = 2

# ==================================================
# Bind components
# ==================================================
zeer-collector.sources.avro-source.channels = memory-channel
zeer-collector.sinks.hdfs-sink.channel = memory-channel
```

### Запуск Flume Agent

```bash
# Systemd service: /etc/systemd/system/flume-zeer-collector.service
[Unit]
Description=Apache Flume Zeer Logs Collector
After=network.target hdfs.service

[Service]
Type=simple
User=hadoop
WorkingDirectory=/opt/flume
ExecStart=/opt/flume/bin/flume-ng agent \
    --conf /opt/flume/conf \
    --conf-file /opt/flume/conf/zeer-logs-collector.conf \
    --name zeer-collector \
    -Dflume.root.logger=INFO,console
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable flume-zeer-collector
sudo systemctl start flume-zeer-collector
sudo systemctl status flume-zeer-collector
```

---

## Node.js реализация

### Структура проекта

```
zeer-logs-api/
├── package.json
├── tsconfig.json
├── .env
├── src/
│   ├── app.ts                    # Express server
│   ├── config/
│   │   └── flume.config.ts       # Flume connection config
│   ├── services/
│   │   ├── flumeService.ts       # Avro RPC client
│   │   └── logProcessor.ts       # Log validation & enrichment
│   ├── middleware/
│   │   ├── validation.ts         # Request validation
│   │   └── errorHandler.ts       # Error handling
│   ├── routes/
│   │   ├── actionLogs.ts         # GraphQL action logs
│   │   ├── crashLogs.ts          # REST crash logs
│   │   └── injectLogs.ts         # REST inject logs
│   ├── schemas/
│   │   ├── actionLog.schema.ts
│   │   ├── crashLog.schema.ts
│   │   └── injectLog.schema.ts
│   └── types/
│       └── logs.types.ts
├── tests/
│   └── integration/
│       └── flume.test.ts
└── scripts/
    └── test-flume-connection.ts
```

### package.json

```json
{
  "name": "zeer-logs-api",
  "version": "1.0.0",
  "description": "Zeer Marketplace Logs Collection API",
  "main": "dist/app.js",
  "scripts": {
    "dev": "nodemon src/app.ts",
    "build": "tsc",
    "start": "node dist/app.js",
    "test": "jest",
    "test:flume": "ts-node scripts/test-flume-connection.ts"
  },
  "dependencies": {
    "express": "^4.18.2",
    "express-graphql": "^0.12.0",
    "graphql": "^16.8.1",
    "avro-js": "^1.11.3",
    "winston": "^3.11.0",
    "dotenv": "^16.3.1",
    "helmet": "^7.1.0",
    "cors": "^2.8.5",
    "express-validator": "^7.0.1",
    "compression": "^1.7.4",
    "uuid": "^9.0.1"
  },
  "devDependencies": {
    "@types/node": "^20.10.6",
    "@types/express": "^4.17.21",
    "typescript": "^5.3.3",
    "ts-node": "^10.9.2",
    "nodemon": "^3.0.2",
    "jest": "^29.7.0"
  }
}
```

### src/config/flume.config.ts

```typescript
export const flumeConfig = {
  host: process.env.FLUME_HOST || 'localhost',
  port: parseInt(process.env.FLUME_PORT || '41414'),
  timeout: parseInt(process.env.FLUME_TIMEOUT || '5000'),
  retries: parseInt(process.env.FLUME_RETRIES || '3'),
  retryDelay: parseInt(process.env.FLUME_RETRY_DELAY || '1000'),
  batchSize: parseInt(process.env.FLUME_BATCH_SIZE || '100'),
  flushInterval: parseInt(process.env.FLUME_FLUSH_INTERVAL || '5000')
};

export const logTypes = {
  ACTION: 'action_logs',
  CRASH: 'crash_logs',
  INJECT: 'inject_logs'
} as const;
```

### src/services/flumeService.ts

```typescript
import avro from 'avro-js';
import net from 'net';
import { EventEmitter } from 'events';
import { flumeConfig } from '../config/flume.config';
import { createLogger } from '../utils/logger';

const logger = createLogger('FlumeService');

// Avro schema для событий
const eventSchema = avro.Type.forSchema({
  type: 'record',
  name: 'LogEvent',
  fields: [
    { name: 'headers', type: { type: 'map', values: 'string' } },
    { name: 'body', type: 'bytes' }
  ]
});

interface LogEvent {
  log_type: string;
  timestamp: string;
  data: any;
}

class FlumeService extends EventEmitter {
  private socket: net.Socket | null = null;
  private connected: boolean = false;
  private reconnectTimer: NodeJS.Timeout | null = null;
  private buffer: LogEvent[] = [];
  private flushTimer: NodeJS.Timeout | null = null;

  constructor() {
    super();
    this.initFlushTimer();
  }

  // Подключение к Flume Avro Source
  async connect(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.socket = net.createConnection(
        flumeConfig.port,
        flumeConfig.host,
        () => {
          this.connected = true;
          logger.info(`Connected to Flume at ${flumeConfig.host}:${flumeConfig.port}`);
          resolve();
        }
      );

      this.socket.on('error', (err) => {
        logger.error('Flume connection error', err);
        this.connected = false;
        this.scheduleReconnect();
        reject(err);
      });

      this.socket.on('close', () => {
        logger.warn('Flume connection closed');
        this.connected = false;
        this.scheduleReconnect();
      });

      this.socket.setTimeout(flumeConfig.timeout);
      this.socket.on('timeout', () => {
        logger.warn('Flume connection timeout');
        this.socket?.destroy();
      });
    });
  }

  // Переподключение
  private scheduleReconnect(): void {
    if (this.reconnectTimer) return;

    this.reconnectTimer = setTimeout(async () => {
      this.reconnectTimer = null;
      try {
        await this.connect();
      } catch (err) {
        logger.error('Reconnect failed', err);
      }
    }, flumeConfig.retryDelay);
  }

  // Отправка одного события
  async sendEvent(event: LogEvent): Promise<void> {
    this.buffer.push(event);

    if (this.buffer.length >= flumeConfig.batchSize) {
      await this.flush();
    }
  }

  // Batch отправка
  async flush(): Promise<void> {
    if (this.buffer.length === 0) return;

    const events = this.buffer.splice(0, this.buffer.length);

    if (!this.connected) {
      logger.warn(`Flume not connected, buffering ${events.length} events`);
      this.buffer.unshift(...events); // Возвращаем в буфер
      return;
    }

    try {
      const avroEvents = events.map(event => ({
        headers: {
          logType: event.log_type,
          timestamp: event.timestamp
        },
        body: Buffer.from(JSON.stringify(event.data))
      }));

      // Avro RPC call
      const encoded = eventSchema.toBuffer(avroEvents);
      this.socket?.write(encoded);

      logger.info(`Sent ${events.length} events to Flume`);
    } catch (err) {
      logger.error('Failed to send events to Flume', err);
      this.buffer.unshift(...events); // Возвращаем в буфер для retry
      throw err;
    }
  }

  // Периодический flush
  private initFlushTimer(): void {
    this.flushTimer = setInterval(() => {
      this.flush().catch(err => logger.error('Auto-flush failed', err));
    }, flumeConfig.flushInterval);
  }

  // Graceful shutdown
  async close(): Promise<void> {
    if (this.flushTimer) {
      clearInterval(this.flushTimer);
    }
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
    }
    await this.flush(); // Отправляем оставшиеся события
    this.socket?.destroy();
    this.connected = false;
  }
}

export const flumeService = new FlumeService();
```

### src/services/logProcessor.ts

```typescript
import { v4 as uuidv4 } from 'uuid';
import { createLogger } from '../utils/logger';

const logger = createLogger('LogProcessor');

interface BaseLog {
  timestamp?: string;
  session_id?: string;
  user_id?: string;
}

export class LogProcessor {
  // Enrichment - добавление метаданных
  static enrich(log: BaseLog, logType: string): any {
    return {
      ...log,
      log_id: uuidv4(),
      log_type: logType,
      timestamp: log.timestamp || new Date().toISOString(),
      server_hostname: process.env.HOSTNAME || 'zeer-master',
      server_timestamp: new Date().toISOString(),
      environment: process.env.NODE_ENV || 'production'
    };
  }

  // Валидация структуры Action Log
  static validateActionLog(log: any): boolean {
    const required = ['user_id', 'action', 'page'];
    return required.every(field => log[field] !== undefined);
  }

  // Валидация структуры Crash Log
  static validateCrashLog(log: any): boolean {
    const required = ['client_version', 'error_message', 'stack_trace'];
    return required.every(field => log[field] !== undefined);
  }

  // Валидация структуры Inject Log
  static validateInjectLog(log: any): boolean {
    const required = ['user_id', 'product_id', 'injection_status'];
    return required.every(field => log[field] !== undefined);
  }

  // Sanitization - очистка PII (если нужно)
  static sanitize(log: any): any {
    const sanitized = { ...log };
    
    // Хешируем IP если есть
    if (sanitized.ip_address) {
      sanitized.ip_hash = this.hashString(sanitized.ip_address);
      delete sanitized.ip_address;
    }

    // Удаляем пароли и токены
    delete sanitized.password;
    delete sanitized.access_token;
    
    return sanitized;
  }

  private static hashString(str: string): string {
    const crypto = require('crypto');
    return crypto.createHash('sha256').update(str).digest('hex');
  }
}
```

### src/routes/actionLogs.ts

```typescript
import { Router, Request, Response } from 'express';
import { body, validationResult } from 'express-validator';
import { flumeService } from '../services/flumeService';
import { LogProcessor } from '../services/logProcessor';
import { logTypes } from '../config/flume.config';
import { createLogger } from '../utils/logger';

const logger = createLogger('ActionLogsRoute');
const router = Router();

// Validation middleware
const validateActionLog = [
  body('user_id').isString().notEmpty(),
  body('action').isString().notEmpty(),
  body('page').isString().notEmpty(),
  body('session_id').optional().isString(),
  body('metadata').optional().isObject()
];

router.post('/', validateActionLog, async (req: Request, res: Response) => {
  try {
    // Валидация
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({ success: false, errors: errors.array() });
    }

    // Enrichment
    const enrichedLog = LogProcessor.enrich(req.body, logTypes.ACTION);
    
    // Sanitization
    const sanitizedLog = LogProcessor.sanitize(enrichedLog);

    // Отправка в Flume
    await flumeService.sendEvent({
      log_type: logTypes.ACTION,
      timestamp: enrichedLog.timestamp,
      data: sanitizedLog
    });

    logger.info(`Action log received: ${enrichedLog.action} by ${enrichedLog.user_id}`);

    res.json({ success: true, log_id: enrichedLog.log_id });
  } catch (error: any) {
    logger.error('Failed to process action log', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// Batch endpoint для массовой загрузки
router.post('/batch', async (req: Request, res: Response) => {
  try {
    const logs = req.body.logs;
    
    if (!Array.isArray(logs)) {
      return res.status(400).json({ success: false, error: 'logs must be an array' });
    }

    const processed = logs.map(log => {
      const enriched = LogProcessor.enrich(log, logTypes.ACTION);
      return LogProcessor.sanitize(enriched);
    });

    for (const log of processed) {
      await flumeService.sendEvent({
        log_type: logTypes.ACTION,
        timestamp: log.timestamp,
        data: log
      });
    }

    res.json({ success: true, count: processed.length });
  } catch (error: any) {
    logger.error('Failed to process batch action logs', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

export default router;
```

### src/routes/crashLogs.ts

```typescript
import { Router, Request, Response } from 'express';
import { body, validationResult } from 'express-validator';
import { flumeService } from '../services/flumeService';
import { LogProcessor } from '../services/logProcessor';
import { logTypes } from '../config/flume.config';
import { createLogger } from '../utils/logger';

const logger = createLogger('CrashLogsRoute');
const router = Router();

const validateCrashLog = [
  body('client_version').isString().notEmpty(),
  body('error_message').isString().notEmpty(),
  body('stack_trace').isString().notEmpty(),
  body('user_id').optional().isString(),
  body('os_info').optional().isObject(),
  body('hardware_info').optional().isObject()
];

router.post('/', validateCrashLog, async (req: Request, res: Response) => {
  try {
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({ success: false, errors: errors.array() });
    }

    const enrichedLog = LogProcessor.enrich(req.body, logTypes.CRASH);
    const sanitizedLog = LogProcessor.sanitize(enrichedLog);

    await flumeService.sendEvent({
      log_type: logTypes.CRASH,
      timestamp: enrichedLog.timestamp,
      data: sanitizedLog
    });

    logger.warn(`Crash log received: ${enrichedLog.error_message}`);

    res.json({ success: true, log_id: enrichedLog.log_id });
  } catch (error: any) {
    logger.error('Failed to process crash log', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

export default router;
```

### src/routes/injectLogs.ts

```typescript
import { Router, Request, Response } from 'express';
import { body, validationResult } from 'express-validator';
import { flumeService } from '../services/flumeService';
import { LogProcessor } from '../services/logProcessor';
import { logTypes } from '../config/flume.config';
import { createLogger } from '../utils/logger';

const logger = createLogger('InjectLogsRoute');
const router = Router();

const validateInjectLog = [
  body('user_id').isString().notEmpty(),
  body('product_id').isString().notEmpty(),
  body('injection_status').isIn(['success', 'failure']),
  body('game_process').optional().isString(),
  body('error_code').optional().isString()
];

router.post('/', validateInjectLog, async (req: Request, res: Response) => {
  try {
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({ success: false, errors: errors.array() });
    }

    const enrichedLog = LogProcessor.enrich(req.body, logTypes.INJECT);
    const sanitizedLog = LogProcessor.sanitize(enrichedLog);

    await flumeService.sendEvent({
      log_type: logTypes.INJECT,
      timestamp: enrichedLog.timestamp,
      data: sanitizedLog
    });

    logger.info(`Inject log received: ${enrichedLog.injection_status} for product ${enrichedLog.product_id}`);

    res.json({ success: true, log_id: enrichedLog.log_id });
  } catch (error: any) {
    logger.error('Failed to process inject log', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

export default router;
```

### src/app.ts

```typescript
import express from 'express';
import helmet from 'helmet';
import cors from 'cors';
import compression from 'compression';
import dotenv from 'dotenv';
import { flumeService } from './services/flumeService';
import actionLogsRouter from './routes/actionLogs';
import crashLogsRouter from './routes/crashLogs';
import injectLogsRouter from './routes/injectLogs';
import { createLogger } from './utils/logger';

dotenv.config();

const logger = createLogger('App');
const app = express();
const PORT = process.env.PORT || 3000;

// Middleware
app.use(helmet());
app.use(cors());
app.use(compression());
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true }));

// Health check
app.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    timestamp: new Date().toISOString(),
    flume_connected: flumeService['connected']
  });
});

// Routes
app.use('/api/logs/action', actionLogsRouter);
app.use('/api/logs/crash', crashLogsRouter);
app.use('/api/logs/inject', injectLogsRouter);

// Error handler
app.use((err: any, req: express.Request, res: express.Response, next: express.NextFunction) => {
  logger.error('Unhandled error', err);
  res.status(500).json({
    success: false,
    error: 'Internal server error',
    message: process.env.NODE_ENV === 'development' ? err.message : undefined
  });
});

// Startup
async function start() {
  try {
    // Connect to Flume
    await flumeService.connect();
    
    // Start server
    app.listen(PORT, () => {
      logger.info(`Zeer Logs API running on port ${PORT}`);
      logger.info(`Environment: ${process.env.NODE_ENV || 'production'}`);
    });
  } catch (error) {
    logger.error('Failed to start server', error);
    process.exit(1);
  }
}

// Graceful shutdown
process.on('SIGTERM', async () => {
  logger.info('SIGTERM received, shutting down gracefully...');
  await flumeService.close();
  process.exit(0);
});

process.on('SIGINT', async () => {
  logger.info('SIGINT received, shutting down gracefully...');
  await flumeService.close();
  process.exit(0);
});

start();

export default app;
```

### .env

```env
# Node.js
NODE_ENV=production
PORT=3000

# Flume
FLUME_HOST=localhost
FLUME_PORT=41414
FLUME_TIMEOUT=5000
FLUME_RETRIES=3
FLUME_RETRY_DELAY=1000
FLUME_BATCH_SIZE=100
FLUME_FLUSH_INTERVAL=5000

# Logging
LOG_LEVEL=info
```

---

## Тестирование интеграции

### scripts/test-flume-connection.ts

```typescript
import { flumeService } from '../src/services/flumeService';
import { LogProcessor } from '../src/services/logProcessor';
import { logTypes } from '../src/config/flume.config';

async function testFlumeConnection() {
  console.log('Testing Flume connection...');

  try {
    // Подключение
    await flumeService.connect();
    console.log('✓ Connected to Flume');

    // Тестовые данные
    const testLogs = [
      {
        user_id: 'test_user_1',
        action: 'page_view',
        page: '/dashboard',
        session_id: 'test_session_1'
      },
      {
        user_id: 'test_user_2',
        action: 'product_click',
        page: '/products',
        product_id: 'prod_123'
      }
    ];

    // Отправка
    for (const log of testLogs) {
      const enriched = LogProcessor.enrich(log, logTypes.ACTION);
      await flumeService.sendEvent({
        log_type: logTypes.ACTION,
        timestamp: enriched.timestamp,
        data: enriched
      });
      console.log(`✓ Sent test log: ${log.action}`);
    }

    // Flush
    await flumeService.flush();
    console.log('✓ Flushed buffer');

    // Закрытие
    await flumeService.close();
    console.log('✓ Connection closed');

    console.log('\n✓ All tests passed!');
  } catch (error) {
    console.error('✗ Test failed:', error);
    process.exit(1);
  }
}

testFlumeConnection();
```

### Ручное тестирование через curl

```bash
# Test Action Log
curl -X POST http://localhost:3000/api/logs/action \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "user_123",
    "action": "product_view",
    "page": "/dashboard/products",
    "session_id": "session_abc",
    "metadata": {
      "product_id": "zeer-csgo",
      "product_title": "Zeer CS:GO Cheat"
    }
  }'

# Test Crash Log
curl -X POST http://localhost:3000/api/logs/crash \
  -H "Content-Type: application/json" \
  -d '{
    "client_version": "1.0.5",
    "error_message": "Failed to initialize DirectX",
    "stack_trace": "at main.cpp:145\nat WinMain+0x42",
    "user_id": "user_456",
    "os_info": {
      "name": "Windows 11",
      "version": "22H2"
    }
  }'

# Test Inject Log
curl -X POST http://localhost:3000/api/logs/inject \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "user_789",
    "product_id": "zeer-csgo",
    "injection_status": "success",
    "game_process": "csgo.exe"
  }'

# Health check
curl http://localhost:3000/health
```

---

## Проверка данных в HDFS

```bash
# Список логов
hdfs dfs -ls /zeer/logs/action_logs/
hdfs dfs -ls /zeer/logs/crash_logs/
hdfs dfs -ls /zeer/logs/inject_logs/

# Просмотр содержимого (последние 10 строк)
hdfs dfs -cat /zeer/logs/action_logs/2024/09/24/events.1727182800000.log | tail -10

# Подсчет количества логов
hdfs dfs -cat /zeer/logs/action_logs/2024/09/24/*.log | wc -l
```

---

## Мониторинг и отладка

### Проверка статуса Flume

```bash
# Логи Flume
sudo journalctl -u flume-zeer-collector -f

# Или если через ADCM
tail -f /var/log/flume/flume.log

# Метрики Flume (если JMX включен)
curl http://localhost:34545/metrics
```

### Проверка Node.js API

```bash
# Логи API
sudo journalctl -u zeer-api -f

# Или если запущен через pm2
pm2 logs zeer-api

# Метрики Node.js
curl http://localhost:3000/health
```

---

## Best Practices

### 1. Форматирование логов

**Рекомендуемый формат: JSON**

```json
{
  "log_id": "uuid-v4",
  "log_type": "action_logs",
  "timestamp": "2024-09-24T12:34:56.789Z",
  "server_timestamp": "2024-09-24T12:34:56.800Z",
  "server_hostname": "zeer-master",
  "user_id": "user_123",
  "session_id": "session_abc",
  "action": "product_view",
  "page": "/dashboard/products",
  "metadata": {
    "product_id": "zeer-csgo",
    "referrer": "/dashboard"
  },
  "environment": "production"
}
```

**Почему JSON?**
- ✅ Легко парсится в Spark/Hive
- ✅ Структурированные данные
- ✅ Поддержка вложенных объектов
- ✅ Human-readable

### 2. Batch vs Real-time

| Параметр | Batch (рекомендовано) | Real-time |
|----------|----------------------|-----------|
| **Размер batch** | 100-1000 событий | 1 событие |
| **Flush interval** | 5-10 секунд | Немедленно |
| **Производительность** | Высокая | Низкая |
| **Сетевой overhead** | Низкий | Высокий |
| **Latency** | 5-10 сек | < 1 сек |
| **Use case** | Analytics, reporting | Fraud detection, alerts |

**Для вашего случая:** Batch (100 событий или 5 секунд)

### 3. Retry механизм

```typescript
async function sendWithRetry(event: LogEvent, maxRetries = 3): Promise<void> {
  for (let i = 0; i < maxRetries; i++) {
    try {
      await flumeService.sendEvent(event);
      return;
    } catch (error) {
      if (i === maxRetries - 1) throw error;
      await sleep(Math.pow(2, i) * 1000); // Exponential backoff
    }
  }
}
```

### 4. Обработка ошибок

```typescript
// Dead Letter Queue для failed events
class FailedEventStore {
  private failedEvents: LogEvent[] = [];
  
  async save(event: LogEvent): Promise<void> {
    this.failedEvents.push(event);
    // Сохранить в локальный файл или Redis
    await fs.appendFile('/var/log/zeer/failed-events.jsonl', JSON.stringify(event) + '\n');
  }
  
  async retry(): Promise<void> {
    // Периодическая повторная отправка
    for (const event of this.failedEvents) {
      try {
        await flumeService.sendEvent(event);
        this.failedEvents = this.failedEvents.filter(e => e !== event);
      } catch (err) {
        // Skip
      }
    }
  }
}
```

### 5. Производительность

**Оптимизация Node.js:**
- Используйте `cluster` модуль для multi-core
- Установите `NODE_ENV=production`
- Включите compression для API responses
- Используйте connection pooling

**Оптимизация Flume:**
- Увеличьте `channel.capacity` если много логов
- Используйте File Channel для durability
- Настройте `hdfs.batchSize` = 1000-5000
- Включите compression в HDFS Sink

### 6. Безопасность

```typescript
// Rate limiting
import rateLimit from 'express-rate-limit';

const limiter = rateLimit({
  windowMs: 60 * 1000, // 1 minute
  max: 1000 // 1000 requests per minute
});

app.use('/api/logs/', limiter);

// Authentication (если нужно)
import { authenticateAPIKey } from './middleware/auth';
app.use('/api/logs/', authenticateAPIKey);
```

---

## Альтернативные архитектуры

### Вариант 2: Node.js → Kafka → Flume → HDFS

**Когда использовать:**
- Высокая нагрузка (> 10k events/sec)
- Нужна гарантия доставки (at-least-once)
- Несколько источников логов

```
Node.js → Kafka → Flume (Kafka Source) → HDFS
```

### Вариант 3: Node.js → REST → Flume HTTP Source → HDFS

**Когда использовать:**
- Прототипирование
- Низкая нагрузка (< 100 events/sec)
- Не нужна высокая производительность

```
Node.js (HTTP POST) → Flume HTTP Source → HDFS
```

---

## Итоговые рекомендации

### Для вашей дипломной работы:

1. ✅ **Размещайте Node.js на MasterNode** - это нормально для домашнего кластера
2. ✅ **Используйте Avro Source** - баланс производительности и надежности
3. ✅ **Memory Channel** для начала, File Channel для production
4. ✅ **Batch отправка** (100 events / 5 sec) - оптимально
5. ✅ **JSON формат** - легко обрабатывается в Spark
6. ✅ **Enrichment на Node.js** - добавляйте timestamp, log_type, server info
7. ✅ **Validation на Node.js** - проверяйте структуру до отправки
8. ✅ **Retry механизм** - обрабатывайте временные сбои

### Ресурсы на MasterNode:

```
Node.js API:     ~500MB RAM, 10% CPU
Flume Agent:     ~1GB RAM, 15% CPU
NameNode:        ~2GB RAM, 20% CPU
------------------------
Total overhead:  ~3.5GB RAM, 45% CPU
```

Это допустимо для ноутбука с 16GB RAM.

---

## Следующие шаги

1. Установите Flume на MasterNode через ADCM
2. Создайте конфигурацию `zeer-logs-collector.conf`
3. Запустите Flume Agent
4. Разверните Node.js API на MasterNode
5. Протестируйте отправку логов через curl
6. Проверьте данные в HDFS
7. Настройте Spark jobs для анализа логов
8. Добавьте мониторинг (Prometheus/Grafana)

---

## Полезные ссылки

- [Apache Flume User Guide](https://flume.apache.org/FlumeUserGuide.html)
- [Avro Specification](https://avro.apache.org/docs/current/spec.html)
- [HDFS Architecture](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html)
- [Node.js Best Practices](https://github.com/goldbergyoni/nodebestpractices)
