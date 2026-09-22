# План создания Big Data инфраструктуры для Zeer Marketplace

## Обзор проекта

**Цель**: Создать домашнюю распределенную систему обработки больших данных на базе Apache Hadoop для обработки логов маркетплейса Zeer.

**Архитектура**:
- 3 физических сервера (4 ядра/8 потоков, 8GB RAM, 160GB HDD каждый)
- Главный ноутбук с Node.js сервером как точка входа
- Hadoop кластер для распределенной обработки
- Ethernet сеть для межсерверного взаимодействия

---

## 1. ИНФРАСТРУКТУРА И ТОПОЛОГИЯ КЛАСТЕРА

### 1.1 Распределение ролей в кластере

С учетом ограниченных ресурсов (8GB RAM, 160GB HDD), рекомендую следующую конфигурацию:

**Server 1 - Master Node**
- NameNode (HDFS metadata management)
- ResourceManager (YARN)
- ZooKeeper (координация)
- History Server (для просмотра завершенных job'ов)
- Spark Master
- Kafka Broker 1

**Server 2 - Worker Node 1**
- DataNode (HDFS storage)
- NodeManager (YARN)
- ZooKeeper
- Spark Worker
- Kafka Broker 2
- Flume Agent (для ingestion логов)

**Server 3 - Worker Node 2**
- DataNode (HDFS storage)
- SecondaryNameNode (checkpointing)
- NodeManager (YARN)
- ZooKeeper
- Spark Worker
- Kafka Broker 3
- Flume Agent

**Ноутбук - Edge Node**
- Node.js API сервер
- Hadoop/Spark клиент
- Kafka Producer (отправка логов в Kafka)
- Web UI для мониторинга

### 1.2 Сетевая конфигурация

**Требования**:
- Все 4 машины в одной подсети (например, 192.168.1.0/24)
- Статические IP адреса для серверов
- Открытые порты между серверами

**Рекомендуемая схема IP**:
```
Server 1 (Master):    192.168.1.10
Server 2 (Worker 1):  192.168.1.11
Server 3 (Worker 2):  192.168.1.12
Laptop (Edge):        192.168.1.5
```

**Ключевые порты**:
```
HDFS NameNode:          8020 (IPC), 9870 (Web UI)
HDFS DataNode:          9864, 9866, 9867
YARN ResourceManager:   8032 (IPC), 8088 (Web UI)
YARN NodeManager:       8042
Spark Master:           7077, 8080 (Web UI)
Spark Worker:           8081
ZooKeeper:              2181, 2888, 3888
Kafka:                  9092
Flume:                  44444
```

**Настройка /etc/hosts на всех серверах**:
```
192.168.1.10    hadoop-master server1
192.168.1.11    hadoop-worker1 server2
192.168.1.12    hadoop-worker2 server3
192.168.1.5     edge-node laptop
```

---

## 2. ВЫБОР ДИСТРИБУТИВА И УСТАНОВКА

### 2.1 Рекомендуемый дистрибутив

Для домашней инфраструктуры с ограниченными ресурсами рекомендую:

**Вариант 1: Apache Hadoop + ручная установка компонентов (Рекомендуется)**
- **Apache Hadoop 3.3.6** (последняя стабильная версия)
- **Apache Spark 3.5.0** с поддержкой Scala 2.12
- **Apache Kafka 3.6.0**
- **Apache Flume 1.11.0**
- **Apache ZooKeeper 3.8.3**

**Преимущества**:
- Полный контроль над конфигурацией
- Минимальное потребление ресурсов
- Понимание работы каждого компонента
- Легко обновлять отдельные компоненты

**Вариант 2: Cloudera CDP (Community Edition)**
- Unified дистрибутив
- Встроенный Cloudera Manager для управления
- Требует минимум 16GB RAM для нормальной работы - **НЕ ПОДХОДИТ**

**Вариант 3: Hortonworks HDP (устарел, не рекомендуется)**

**РЕКОМЕНДАЦИЯ**: Используйте Вариант 1 - Apache компоненты вручную.

### 2.2 Выбор операционной системы

**Рекомендуемые ОС**:
1. **Ubuntu Server 22.04 LTS** (рекомендуется)
   - Отличная поддержка Java и Hadoop
   - Большое комьюнити
   - Легкая настройка
   
2. **CentOS Stream 9 / Rocky Linux 9**
   - Традиционно используется в enterprise Hadoop кластерах
   - Стабильная и надежная

3. **Debian 12**
   - Легковесная
   - Стабильная

**РЕКОМЕНДАЦИЯ**: Ubuntu Server 22.04 LTS для простоты настройки.

### 2.3 Подготовка серверов

**Шаг 1: Установка базовых пакетов на всех серверах**

```bash
# Обновление системы
sudo apt update && sudo apt upgrade -y

# Установка необходимых пакетов
sudo apt install -y openssh-server vim net-tools curl wget git \
    build-essential python3 python3-pip rsync

# Установка Java (Hadoop требует Java 8 или 11)
sudo apt install -y openjdk-11-jdk

# Проверка версии Java
java -version
```

**Шаг 2: Создание Hadoop пользователя**

```bash
# На всех серверах
sudo adduser hadoop
sudo usermod -aG sudo hadoop

# Переключиться на пользователя hadoop
su - hadoop
```

**Шаг 3: Настройка SSH без пароля**

```bash
# На всех серверах
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa

# С Master ноды скопировать ключи на все ноды
ssh-copy-id hadoop@hadoop-master
ssh-copy-id hadoop@hadoop-worker1
ssh-copy-id hadoop@hadoop-worker2
ssh-copy-id hadoop@edge-node

# Проверить подключение
ssh hadoop-worker1
ssh hadoop-worker2
```

**Шаг 4: Отключение firewall (для домашней сети)**

```bash
# Ubuntu
sudo ufw disable

# Или настроить правила для нужных портов
sudo ufw allow 8020
sudo ufw allow 9870
# и т.д.
```

---

## 3. УСТАНОВКА И НАСТРОЙКА HADOOP КЛАСТЕРА

### 3.1 Установка Hadoop на всех серверах

```bash
# Скачать Hadoop 3.3.6
cd /opt
sudo wget https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
sudo tar -xzf hadoop-3.3.6.tar.gz
sudo mv hadoop-3.3.6 hadoop
sudo chown -R hadoop:hadoop /opt/hadoop

# Настроить переменные окружения
echo 'export HADOOP_HOME=/opt/hadoop' >> ~/.bashrc
echo 'export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop' >> ~/.bashrc
echo 'export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin' >> ~/.bashrc
echo 'export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64' >> ~/.bashrc
source ~/.bashrc
```

### 3.2 Конфигурация Hadoop (на Master ноде, затем скопировать на Worker'ы)

**Файл: $HADOOP_HOME/etc/hadoop/hadoop-env.sh**
```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop
export HADOOP_LOG_DIR=${HADOOP_HOME}/logs
```

**Файл: $HADOOP_HOME/etc/hadoop/core-site.xml**
```xml
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://hadoop-master:8020</value>
    </property>
    <property>
        <name>hadoop.tmp.dir</name>
        <value>/opt/hadoop/tmp</value>
    </property>
    <property>
        <name>io.file.buffer.size</name>
        <value>131072</value>
    </property>
</configuration>
```

**Файл: $HADOOP_HOME/etc/hadoop/hdfs-site.xml**
```xml
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>2</value> <!-- 2 реплики для 2 DataNode -->
    </property>
    <property>
        <name>dfs.namenode.name.dir</name>
        <value>file:///opt/hadoop/hdfs/namenode</value>
    </property>
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>file:///opt/hadoop/hdfs/datanode</value>
    </property>
    <property>
        <name>dfs.namenode.checkpoint.dir</name>
        <value>file:///opt/hadoop/hdfs/namesecondary</value>
    </property>
    <property>
        <name>dfs.blocksize</name>
        <value>134217728</value> <!-- 128MB блоки для маленького диска -->
    </property>
    <property>
        <name>dfs.namenode.handler.count</name>
        <value>10</value>
    </property>
    <property>
        <name>dfs.datanode.handler.count</name>
        <value>10</value>
    </property>
</configuration>
```

**Файл: $HADOOP_HOME/etc/hadoop/yarn-site.xml**
```xml
<configuration>
    <property>
        <name>yarn.resourcemanager.hostname</name>
        <value>hadoop-master</value>
    </property>
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
    </property>
    <property>
        <name>yarn.nodemanager.resource.memory-mb</name>
        <value>6144</value> <!-- 6GB для NodeManager (оставляем 2GB для ОС) -->
    </property>
    <property>
        <name>yarn.scheduler.minimum-allocation-mb</name>
        <value>512</value>
    </property>
    <property>
        <name>yarn.scheduler.maximum-allocation-mb</name>
        <value>6144</value>
    </property>
    <property>
        <name>yarn.nodemanager.resource.cpu-vcores</name>
        <value>6</value> <!-- Оставляем 2 ядра для ОС -->
    </property>
    <property>
        <name>yarn.nodemanager.vmem-check-enabled</name>
        <value>false</value> <!-- Отключаем проверку виртуальной памяти -->
    </property>
</configuration>
```

**Файл: $HADOOP_HOME/etc/hadoop/mapred-site.xml**
```xml
<configuration>
    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>
    <property>
        <name>mapreduce.map.memory.mb</name>
        <value>1024</value>
    </property>
    <property>
        <name>mapreduce.reduce.memory.mb</name>
        <value>2048</value>
    </property>
    <property>
        <name>mapreduce.map.java.opts</name>
        <value>-Xmx819m</value>
    </property>
    <property>
        <name>mapreduce.reduce.java.opts</name>
        <value>-Xmx1638m</value>
    </property>
    <property>
        <name>yarn.app.mapreduce.am.resource.mb</name>
        <value>1024</value>
    </property>
</configuration>
```

**Файл: $HADOOP_HOME/etc/hadoop/workers**
```
hadoop-worker1
hadoop-worker2
```

### 3.3 Распространение конфигурации на все ноды

```bash
# С Master ноды
cd /opt/hadoop/etc/hadoop
scp core-site.xml hdfs-site.xml yarn-site.xml mapred-site.xml hadoop-env.sh workers \
    hadoop@hadoop-worker1:/opt/hadoop/etc/hadoop/
scp core-site.xml hdfs-site.xml yarn-site.xml mapred-site.xml hadoop-env.sh workers \
    hadoop@hadoop-worker2:/opt/hadoop/etc/hadoop/
```

### 3.4 Создание директорий для HDFS

```bash
# На Master (Server 1)
mkdir -p /opt/hadoop/hdfs/namenode
mkdir -p /opt/hadoop/tmp

# На Worker 1 (Server 2)
mkdir -p /opt/hadoop/hdfs/datanode
mkdir -p /opt/hadoop/tmp

# На Worker 2 (Server 3)
mkdir -p /opt/hadoop/hdfs/datanode
mkdir -p /opt/hadoop/hdfs/namesecondary
mkdir -p /opt/hadoop/tmp
```

### 3.5 Форматирование HDFS NameNode

```bash
# ТОЛЬКО на Master ноде, ТОЛЬКО ОДИН РАЗ
hdfs namenode -format
```

### 3.6 Запуск Hadoop кластера

```bash
# С Master ноды
# Запуск HDFS
start-dfs.sh

# Запуск YARN
start-yarn.sh

# Проверка запущенных процессов
jps

# На Master должно быть:
# - NameNode
# - ResourceManager
# - Jps

# На Worker нодах должно быть:
# - DataNode
# - NodeManager
# - Jps
# (+ SecondaryNameNode на Worker 2)
```

### 3.7 Проверка работоспособности

```bash
# Проверка HDFS
hdfs dfsadmin -report

# Проверка YARN
yarn node -list

# Web UI
# HDFS NameNode: http://192.168.1.10:9870
# YARN ResourceManager: http://192.168.1.10:8088
```

---

## 4. УСТАНОВКА ДОПОЛНИТЕЛЬНЫХ КОМПОНЕНТОВ

### 4.1 Apache Spark

**Установка на всех нодах**:

```bash
cd /opt
sudo wget https://dlcdn.apache.org/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz
sudo tar -xzf spark-3.5.0-bin-hadoop3.tgz
sudo mv spark-3.5.0-bin-hadoop3 spark
sudo chown -R hadoop:hadoop /opt/spark

# Переменные окружения
echo 'export SPARK_HOME=/opt/spark' >> ~/.bashrc
echo 'export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin' >> ~/.bashrc
source ~/.bashrc
```

**Конфигурация Spark (на Master)**:

```bash
cd /opt/spark/conf
cp spark-env.sh.template spark-env.sh
vim spark-env.sh
```

Добавить:
```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export SPARK_MASTER_HOST=hadoop-master
export SPARK_MASTER_PORT=7077
export SPARK_MASTER_WEBUI_PORT=8080
export SPARK_WORKER_CORES=6
export SPARK_WORKER_MEMORY=5g
export SPARK_WORKER_INSTANCES=1
```

**Файл: spark-defaults.conf**
```properties
spark.master                     spark://hadoop-master:7077
spark.eventLog.enabled           true
spark.eventLog.dir               hdfs://hadoop-master:8020/spark-logs
spark.history.fs.logDirectory    hdfs://hadoop-master:8020/spark-logs
spark.executor.memory            4g
spark.driver.memory              2g
spark.executor.cores             3
```

**Создать директорию для логов в HDFS**:
```bash
hdfs dfs -mkdir -p /spark-logs
```

**Запуск Spark кластера**:
```bash
# С Master ноды
start-master.sh

# С каждой Worker ноды
start-worker.sh spark://hadoop-master:7077

# Или с Master (если настроен workers файл)
start-workers.sh spark://hadoop-master:7077
```

### 4.2 Apache ZooKeeper

**Установка на всех трех серверах**:

```bash
cd /opt
sudo wget https://dlcdn.apache.org/zookeeper/zookeeper-3.8.3/apache-zookeeper-3.8.3-bin.tar.gz
sudo tar -xzf apache-zookeeper-3.8.3-bin.tar.gz
sudo mv apache-zookeeper-3.8.3-bin zookeeper
sudo chown -R hadoop:hadoop /opt/zookeeper

# Переменные окружения
echo 'export ZOOKEEPER_HOME=/opt/zookeeper' >> ~/.bashrc
echo 'export PATH=$PATH:$ZOOKEEPER_HOME/bin' >> ~/.bashrc
source ~/.bashrc
```

**Конфигурация ZooKeeper**:

```bash
cd /opt/zookeeper/conf
cp zoo_sample.cfg zoo.cfg
vim zoo.cfg
```

Содержимое:
```properties
tickTime=2000
initLimit=10
syncLimit=5
dataDir=/opt/zookeeper/data
clientPort=2181

# Ensemble configuration
server.1=hadoop-master:2888:3888
server.2=hadoop-worker1:2888:3888
server.3=hadoop-worker2:2888:3888
```

**Создать директории и ID**:

```bash
# На всех серверах
sudo mkdir -p /opt/zookeeper/data
sudo chown hadoop:hadoop /opt/zookeeper/data

# На Server 1 (Master)
echo "1" > /opt/zookeeper/data/myid

# На Server 2 (Worker 1)
echo "2" > /opt/zookeeper/data/myid

# На Server 3 (Worker 2)
echo "3" > /opt/zookeeper/data/myid
```

**Запуск ZooKeeper на всех трех серверах**:
```bash
zkServer.sh start

# Проверка статуса
zkServer.sh status
# Один должен быть leader, два - followers
```

### 4.3 Apache Kafka

**Установка на всех трех серверах**:

```bash
cd /opt
sudo wget https://downloads.apache.org/kafka/3.6.0/kafka_2.13-3.6.0.tgz
sudo tar -xzf kafka_2.13-3.6.0.tgz
sudo mv kafka_2.13-3.6.0 kafka
sudo chown -R hadoop:hadoop /opt/kafka

# Переменные окружения
echo 'export KAFKA_HOME=/opt/kafka' >> ~/.bashrc
echo 'export PATH=$PATH:$KAFKA_HOME/bin' >> ~/.bashrc
source ~/.bashrc
```

**Конфигурация Kafka**:

```bash
cd /opt/kafka/config
vim server.properties
```

**На Server 1 (Master)**:
```properties
broker.id=1
listeners=PLAINTEXT://192.168.1.10:9092
advertised.listeners=PLAINTEXT://192.168.1.10:9092
log.dirs=/opt/kafka/kafka-logs
num.partitions=3
default.replication.factor=2
min.insync.replicas=2
zookeeper.connect=hadoop-master:2181,hadoop-worker1:2181,hadoop-worker2:2181
log.retention.hours=168
log.segment.bytes=1073741824
```

**На Server 2 (Worker 1)** - изменить:
```properties
broker.id=2
listeners=PLAINTEXT://192.168.1.11:9092
advertised.listeners=PLAINTEXT://192.168.1.11:9092
```

**На Server 3 (Worker 2)** - изменить:
```properties
broker.id=3
listeners=PLAINTEXT://192.168.1.12:9092
advertised.listeners=PLAINTEXT://192.168.1.12:9092
```

**Создать директории**:
```bash
sudo mkdir -p /opt/kafka/kafka-logs
sudo chown hadoop:hadoop /opt/kafka/kafka-logs
```

**Запуск Kafka на всех трех серверах**:
```bash
kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties

# Проверка
jps | grep Kafka
```

**Создание топиков для логов Zeer**:
```bash
# С любой ноды
kafka-topics.sh --create --topic zeer-user-actions \
    --bootstrap-server hadoop-master:9092 \
    --partitions 3 --replication-factor 2

kafka-topics.sh --create --topic zeer-crash-logs \
    --bootstrap-server hadoop-master:9092 \
    --partitions 3 --replication-factor 2

kafka-topics.sh --create --topic zeer-injection-logs \
    --bootstrap-server hadoop-master:9092 \
    --partitions 3 --replication-factor 2

# Проверка
kafka-topics.sh --list --bootstrap-server hadoop-master:9092
```

### 4.4 Apache Flume

**Установка на Worker нодах (Server 2 и 3)**:

```bash
cd /opt
sudo wget https://dlcdn.apache.org/flume/1.11.0/apache-flume-1.11.0-bin.tar.gz
sudo tar -xzf apache-flume-1.11.0-bin.tar.gz
sudo mv apache-flume-1.11.0-bin flume
sudo chown -R hadoop:hadoop /opt/flume

# Переменные окружения
echo 'export FLUME_HOME=/opt/flume' >> ~/.bashrc
echo 'export PATH=$PATH:$FLUME_HOME/bin' >> ~/.bashrc
source ~/.bashrc
```

**Конфигурация Flume для Kafka to HDFS**:

```bash
cd /opt/flume/conf
vim flume-kafka-hdfs.conf
```

Содержимое:
```properties
# Agent name
agent1.sources = kafka-source
agent1.channels = memory-channel
agent1.sinks = hdfs-sink

# Source - Kafka
agent1.sources.kafka-source.type = org.apache.flume.source.kafka.KafkaSource
agent1.sources.kafka-source.kafka.bootstrap.servers = hadoop-master:9092,hadoop-worker1:9092,hadoop-worker2:9092
agent1.sources.kafka-source.kafka.topics = zeer-user-actions,zeer-crash-logs,zeer-injection-logs
agent1.sources.kafka-source.kafka.consumer.group.id = flume-consumer
agent1.sources.kafka-source.channels = memory-channel
agent1.sources.kafka-source.interceptors = i1
agent1.sources.kafka-source.interceptors.i1.type = timestamp

# Channel - Memory
agent1.channels.memory-channel.type = memory
agent1.channels.memory-channel.capacity = 10000
agent1.channels.memory-channel.transactionCapacity = 1000

# Sink - HDFS
agent1.sinks.hdfs-sink.type = hdfs
agent1.sinks.hdfs-sink.hdfs.path = hdfs://hadoop-master:8020/zeer/logs/%{topic}/%Y/%m/%d
agent1.sinks.hdfs-sink.hdfs.filePrefix = events
agent1.sinks.hdfs-sink.hdfs.fileSuffix = .log
agent1.sinks.hdfs-sink.hdfs.rollInterval = 600
agent1.sinks.hdfs-sink.hdfs.rollSize = 134217728
agent1.sinks.hdfs-sink.hdfs.rollCount = 0
agent1.sinks.hdfs-sink.hdfs.fileType = DataStream
agent1.sinks.hdfs-sink.hdfs.writeFormat = Text
agent1.sinks.hdfs-sink.channel = memory-channel
```

**Создать директории в HDFS**:
```bash
hdfs dfs -mkdir -p /zeer/logs
hdfs dfs -chmod -R 777 /zeer
```

**Запуск Flume**:
```bash
flume-ng agent --conf /opt/flume/conf \
    --conf-file /opt/flume/conf/flume-kafka-hdfs.conf \
    --name agent1 -Dflume.root.logger=INFO,console &
```

---

## 5. ИНТЕГРАЦИЯ С NODE.JS

### 5.1 Архитектура взаимодействия

```
[Zeer Marketplace] 
       |
       v
[Node.js API Server (Laptop)]
       |
       +---> [Kafka] ---> [Flume] ---> [HDFS]
       |
       +---> [HDFS Client API] ---> [HDFS]
       |
       +---> [Spark Submit] ---> [Spark Cluster]
       |
       +---> [YARN REST API] ---> [YARN Cluster]
```

### 5.2 Установка необходимых пакетов на ноутбуке

**Установка Java и Hadoop клиента**:

```bash
# На Windows (через WSL2 или Git Bash)
# Или просто установить отдельно Hadoop клиент

# Скачать winutils для Windows
# https://github.com/cdarlint/winutils

# Или использовать Docker контейнер с Hadoop клиентом
```

**Node.js пакеты**:

```bash
npm init -y

npm install express
npm install kafkajs          # Kafka producer/consumer
npm install @azure/storage-blob  # Если нужен S3-like storage
npm install axios            # Для REST API вызовов к YARN/Spark
npm install winston          # Логирование
npm install dotenv           # Конфигурация
```

### 5.3 Конфигурация подключения

**Файл: .env**
```env
# Kafka Configuration
KAFKA_BROKERS=192.168.1.10:9092,192.168.1.11:9092,192.168.1.12:9092
KAFKA_CLIENT_ID=zeer-nodejs-producer

# HDFS Configuration
HDFS_NAMENODE=hadoop-master
HDFS_PORT=8020
HDFS_USER=hadoop

# Spark Configuration
SPARK_MASTER=spark://hadoop-master:7077
SPARK_SUBMIT_PATH=/opt/spark/bin/spark-submit

# YARN Configuration
YARN_RM_HOST=hadoop-master
YARN_RM_PORT=8088
```

### 5.4 Kafka Producer для отправки логов

**Файл: src/kafka/producer.js**

```javascript
const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: process.env.KAFKA_CLIENT_ID,
  brokers: process.env.KAFKA_BROKERS.split(',')
});

const producer = kafka.producer();

async function sendLog(topic, logData) {
  await producer.connect();
  
  try {
    await producer.send({
      topic: topic,
      messages: [
        {
          key: logData.userId || 'anonymous',
          value: JSON.stringify(logData),
          timestamp: Date.now()
        }
      ]
    });
    console.log(`Log sent to topic ${topic}`);
  } catch (error) {
    console.error('Error sending log:', error);
    throw error;
  }
}

async function sendUserAction(actionData) {
  return sendLog('zeer-user-actions', actionData);
}

async function sendCrashLog(crashData) {
  return sendLog('zeer-crash-logs', crashData);
}

async function sendInjectionLog(injectionData) {
  return sendLog('zeer-injection-logs', injectionData);
}

module.exports = {
  sendUserAction,
  sendCrashLog,
  sendInjectionLog,
  disconnectProducer: async () => await producer.disconnect()
};
```

### 5.5 Spark Job запуск через Node.js

**Файл: src/spark/jobRunner.js**

```javascript
const { exec } = require('child_process');
const util = require('util');
const execPromise = util.promisify(exec);

async function submitSparkJob(jobConfig) {
  const {
    className,
    jarPath,
    args = [],
    executorMemory = '4g',
    driverMemory = '2g',
    executorCores = 3
  } = jobConfig;

  const sparkSubmitCmd = `
    ${process.env.SPARK_SUBMIT_PATH} \\
    --master ${process.env.SPARK_MASTER} \\
    --deploy-mode cluster \\
    --driver-memory ${driverMemory} \\
    --executor-memory ${executorMemory} \\
    --executor-cores ${executorCores} \\
    --class ${className} \\
    ${jarPath} \\
    ${args.join(' ')}
  `;

  try {
    const { stdout, stderr } = await execPromise(sparkSubmitCmd);
    console.log('Spark job submitted successfully');
    console.log('stdout:', stdout);
    
    // Извлечь application ID из вывода
    const appIdMatch = stdout.match(/application_\d+_\d+/);
    const applicationId = appIdMatch ? appIdMatch[0] : null;
    
    return {
      success: true,
      applicationId,
      output: stdout
    };
  } catch (error) {
    console.error('Error submitting Spark job:', error);
    throw error;
  }
}

// Пример: Запуск анализа логов действий пользователей
async function analyzeUserActions(date) {
  return submitSparkJob({
    className: 'com.zeer.analytics.UserActionsAnalyzer',
    jarPath: '/opt/zeer/analytics-jobs.jar',
    args: [`--date=${date}`, '--output=/zeer/analytics/user-actions'],
    executorMemory: '4g',
    driverMemory: '2g',
    executorCores: 3
  });
}

module.exports = {
  submitSparkJob,
  analyzeUserActions
};
```

### 5.6 YARN REST API интеграция

**Файл: src/yarn/yarnClient.js**

```javascript
const axios = require('axios');

const YARN_RM_URL = `http://${process.env.YARN_RM_HOST}:${process.env.YARN_RM_PORT}`;

async function getClusterInfo() {
  const response = await axios.get(`${YARN_RM_URL}/ws/v1/cluster/info`);
  return response.data;
}

async function getClusterMetrics() {
  const response = await axios.get(`${YARN_RM_URL}/ws/v1/cluster/metrics`);
  return response.data.clusterMetrics;
}

async function listApplications(state = 'RUNNING') {
  const response = await axios.get(`${YARN_RM_URL}/ws/v1/cluster/apps`, {
    params: { state }
  });
  return response.data.apps?.app || [];
}

async function getApplicationStatus(appId) {
  const response = await axios.get(`${YARN_RM_URL}/ws/v1/cluster/apps/${appId}`);
  return response.data.app;
}

async function killApplication(appId) {
  const response = await axios.put(`${YARN_RM_URL}/ws/v1/cluster/apps/${appId}/state`, {
    state: 'KILLED'
  });
  return response.data;
}

module.exports = {
  getClusterInfo,
  getClusterMetrics,
  listApplications,
  getApplicationStatus,
  killApplication
};
```

### 5.7 Express API endpoints

**Файл: src/app.js**

```javascript
const express = require('express');
const { sendUserAction, sendCrashLog, sendInjectionLog } = require('./kafka/producer');
const { analyzeUserActions } = require('./spark/jobRunner');
const { getClusterMetrics, listApplications } = require('./yarn/yarnClient');

const app = express();
app.use(express.json());

// Endpoint для приема логов от Zeer Marketplace
app.post('/api/logs/user-action', async (req, res) => {
  try {
    await sendUserAction(req.body);
    res.json({ success: true, message: 'User action logged' });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

app.post('/api/logs/crash', async (req, res) => {
  try {
    await sendCrashLog(req.body);
    res.json({ success: true, message: 'Crash log recorded' });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

app.post('/api/logs/injection', async (req, res) => {
  try {
    await sendInjectionLog(req.body);
    res.json({ success: true, message: 'Injection log recorded' });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Endpoint для запуска аналитических задач
app.post('/api/analytics/user-actions', async (req, res) => {
  try {
    const { date } = req.body;
    const result = await analyzeUserActions(date);
    res.json({ success: true, applicationId: result.applicationId });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Endpoint для мониторинга кластера
app.get('/api/cluster/metrics', async (req, res) => {
  try {
    const metrics = await getClusterMetrics();
    res.json(metrics);
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

app.get('/api/cluster/applications', async (req, res) => {
  try {
    const apps = await listApplications();
    res.json(apps);
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Zeer Big Data API running on port ${PORT}`);
});
```

---

## 6. РАЗРАБОТКА BIG DATA PIPELINE

### 6.1 Архитектура Data Pipeline

```
[Zeer Marketplace]
       |
       v
[Node.js API] --> [Kafka Topics]
                       |
                       v
                  [Flume Agents]
                       |
                       v
                  [HDFS Raw Data]
                       |
                       v
              [Spark Streaming / Batch Jobs]
                       |
                       +---> [Real-time Analytics]
                       |
                       +---> [Aggregated Data in HDFS]
                       |
                       v
              [Query Layer - Hive/Impala]
                       |
                       v
              [Visualization / BI Tools]
```

### 6.2 Обработка логов действий пользователей

**Структура данных user-action log**:
```json
{
  "timestamp": "2026-09-21T15:30:00Z",
  "userId": "user_12345",
  "sessionId": "session_abc123",
  "action": "product_view",
  "productId": "prod_98765",
  "category": "electronics",
  "metadata": {
    "platform": "web",
    "browser": "Chrome",
    "ip": "192.168.1.100"
  }
}
```

**Spark Job для агрегации (Scala)**:

**Файл: UserActionsAnalyzer.scala**

```scala
package com.zeer.analytics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object UserActionsAnalyzer {
  
  val schema = StructType(Array(
    StructField("timestamp", TimestampType, nullable = false),
    StructField("userId", StringType, nullable = false),
    StructField("sessionId", StringType, nullable = false),
    StructField("action", StringType, nullable = false),
    StructField("productId", StringType, nullable = true),
    StructField("category", StringType, nullable = true),
    StructField("metadata", MapType(StringType, StringType), nullable = true)
  ))
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Zeer User Actions Analyzer")
      .getOrCreate()
    
    val inputPath = args(0) // hdfs://hadoop-master:8020/zeer/logs/zeer-user-actions/2026/09/21
    val outputPath = args(1) // hdfs://hadoop-master:8020/zeer/analytics/user-actions/2026/09/21
    
    // Чтение логов
    val logs = spark.read
      .schema(schema)
      .json(inputPath)
    
    // Анализ 1: Топ действий по категориям
    val actionsByCategory = logs
      .groupBy("category", "action")
      .agg(count("*").as("count"))
      .orderBy(desc("count"))
    
    actionsByCategory.write
      .mode("overwrite")
      .parquet(s"$outputPath/actions_by_category")
    
    // Анализ 2: Активность пользователей
    val userActivity = logs
      .groupBy("userId")
      .agg(
        count("*").as("total_actions"),
        countDistinct("sessionId").as("sessions"),
        countDistinct("productId").as("products_viewed")
      )
      .orderBy(desc("total_actions"))
    
    userActivity.write
      .mode("overwrite")
      .parquet(s"$outputPath/user_activity")
    
    // Анализ 3: Почасовая активность
    val hourlyActivity = logs
      .withColumn("hour", hour(col("timestamp")))
      .groupBy("hour", "action")
      .agg(count("*").as("count"))
      .orderBy("hour", desc("count"))
    
    hourlyActivity.write
      .mode("overwrite")
      .parquet(s"$outputPath/hourly_activity")
    
    spark.stop()
  }
}
```

### 6.3 Обработка crash логов

**Структура crash log**:
```json
{
  "timestamp": "2026-09-21T15:32:15Z",
  "userId": "user_12345",
  "sessionId": "session_abc123",
  "crashType": "NullPointerException",
  "stackTrace": "...",
  "appVersion": "2.1.5",
  "platform": "android",
  "deviceInfo": {
    "model": "Samsung Galaxy S21",
    "os": "Android 13"
  }
}
```

**Spark Streaming для real-time crash detection**:

```scala
package com.zeer.analytics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._

object CrashStreamingAnalyzer {
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Zeer Crash Streaming Analyzer")
      .getOrCreate()
    
    import spark.implicits._
    
    // Чтение из Kafka в режиме streaming
    val crashStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "hadoop-master:9092,hadoop-worker1:9092,hadoop-worker2:9092")
      .option("subscribe", "zeer-crash-logs")
      .option("startingOffsets", "latest")
      .load()
      .selectExpr("CAST(value AS STRING) as json")
      .select(from_json($"json", crashSchema).as("data"))
      .select("data.*")
    
    // Агрегация по окнам времени (5 минут)
    val crashAggregates = crashStream
      .withWatermark("timestamp", "10 minutes")
      .groupBy(
        window($"timestamp", "5 minutes", "1 minute"),
        $"crashType",
        $"appVersion"
      )
      .agg(
        count("*").as("crash_count"),
        countDistinct("userId").as("affected_users")
      )
    
    // Запись результатов в HDFS
    val query = crashAggregates.writeStream
      .outputMode("append")
      .format("parquet")
      .option("path", "hdfs://hadoop-master:8020/zeer/analytics/crashes/realtime")
      .option("checkpointLocation", "hdfs://hadoop-master:8020/zeer/checkpoints/crashes")
      .trigger(Trigger.ProcessingTime("1 minute"))
      .start()
    
    query.awaitTermination()
  }
}
```

### 6.4 Локальная разработка и тестирование

**Подход 1: Spark Local Mode**

```scala
// В коде для разработки
val spark = SparkSession.builder()
  .appName("Local Test")
  .master("local[*]") // Локальный режим
  .getOrCreate()

// Чтение из локального файла
val logs = spark.read.json("file:///C:/data/test-logs.json")
```

**Подход 2: Docker Compose для локальной инфраструктуры**

**Файл: docker-compose.yml**

```yaml
version: '3.8'

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
    ports:
      - "2181:2181"

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    depends_on:
      - zookeeper
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    ports:
      - "9092:9092"

  spark-master:
    image: bitnami/spark:3.5.0
    environment:
      - SPARK_MODE=master
    ports:
      - "8080:8080"
      - "7077:7077"

  spark-worker:
    image: bitnami/spark:3.5.0
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark-master:7077
    depends_on:
      - spark-master
```

**Запуск локальной инфраструктуры**:
```bash
docker-compose up -d
```

**Подход 3: Unit тесты для Spark jobs**

```scala
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession

class UserActionsAnalyzerTest extends AnyFunSuite {
  
  val spark = SparkSession.builder()
    .master("local[2]")
    .appName("Test")
    .getOrCreate()
  
  import spark.implicits._
  
  test("should count actions by category") {
    val testData = Seq(
      ("user1", "view", "electronics"),
      ("user2", "view", "electronics"),
      ("user3", "click", "books")
    ).toDF("userId", "action", "category")
    
    val result = testData
      .groupBy("category", "action")
      .count()
      .collect()
    
    assert(result.length == 2)
  }
}
```

### 6.5 CI/CD Pipeline для Spark Jobs

**Структура проекта**:
```
zeer-analytics/
├── src/
│   ├── main/
│   │   └── scala/
│   │       └── com/zeer/analytics/
│   │           ├── UserActionsAnalyzer.scala
│   │           ├── CrashStreamingAnalyzer.scala
│   │           └── InjectionDetector.scala
│   └── test/
│       └── scala/
│           └── com/zeer/analytics/
│               └── AnalyzerTests.scala
├── build.sbt
├── Dockerfile
└── deploy.sh
```

**Файл: build.sbt**

```scala
name := "zeer-analytics"
version := "1.0"
scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.5.0" % "provided",
  "org.apache.spark" %% "spark-streaming" % "3.5.0" % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.0",
  "org.scalatest" %% "scalatest" % "3.2.15" % "test"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
```

**Скрипт деплоя: deploy.sh**

```bash
#!/bin/bash

# Сборка JAR
sbt clean assembly

# Копирование на Master ноду
scp target/scala-2.12/zeer-analytics-assembly-1.0.jar \
    hadoop@hadoop-master:/opt/zeer/jars/

# Запуск Spark job через SSH
ssh hadoop@hadoop-master "spark-submit \
    --master spark://hadoop-master:7077 \
    --deploy-mode cluster \
    --class com.zeer.analytics.UserActionsAnalyzer \
    /opt/zeer/jars/zeer-analytics-assembly-1.0.jar \
    hdfs://hadoop-master:8020/zeer/logs/zeer-user-actions/2026/09/21 \
    hdfs://hadoop-master:8020/zeer/analytics/user-actions/2026/09/21"
```

---

## 7. ОБРАБОТКА ЛОГОВ ИНЖЕКТОВ

### 7.1 Структура injection log

```json
{
  "timestamp": "2026-09-21T15:35:00Z",
  "sourceIp": "192.168.1.150",
  "endpoint": "/api/products/search",
  "payload": "SELECT * FROM users WHERE id='1' OR '1'='1'",
  "injectionType": "sql_injection",
  "blocked": true,
  "userId": "user_12345",
  "severity": "high"
}
```

### 7.2 Spark ML для детекции аномалий

```scala
package com.zeer.analytics

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer}
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.Pipeline

object InjectionDetector {
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Zeer Injection Detector")
      .getOrCreate()
    
    val inputPath = "hdfs://hadoop-master:8020/zeer/logs/zeer-injection-logs"
    
    val logs = spark.read.json(inputPath)
    
    // Feature engineering
    val indexer = new StringIndexer()
      .setInputCol("injectionType")
      .setOutputCol("injectionTypeIndex")
    
    val assembler = new VectorAssembler()
      .setInputCols(Array("injectionTypeIndex", "severity"))
      .setOutputCol("features")
    
    val rf = new RandomForestClassifier()
      .setLabelCol("blocked")
      .setFeaturesCol("features")
      .setNumTrees(100)
    
    val pipeline = new Pipeline()
      .setStages(Array(indexer, assembler, rf))
    
    // Тренировка модели
    val model = pipeline.fit(logs)
    
    // Сохранение модели
    model.write.overwrite().save("hdfs://hadoop-master:8020/zeer/models/injection-detector")
    
    spark.stop()
  }
}
```

---

## 8. МОНИТОРИНГ И ОБСЛУЖИВАНИЕ

### 8.1 Мониторинг инструменты

**Ganglia для метрик кластера** (опционально):

```bash
# Установка на Master
sudo apt install -y ganglia-monitor gmetad ganglia-webfrontend

# Конфигурация
sudo vim /etc/ganglia/gmond.conf
```

**Prometheus + Grafana** (рекомендуется):

```yaml
# docker-compose.yml на ноутбуке
version: '3.8'

services:
  prometheus:
    image: prom/prometheus:latest
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml
    ports:
      - "9090:9090"

  grafana:
    image: grafana/grafana:latest
    ports:
      - "3001:3000"
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin
```

**prometheus.yml**:
```yaml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: 'hadoop-namenode'
    static_configs:
      - targets: ['192.168.1.10:9870']
  
  - job_name: 'yarn-resourcemanager'
    static_configs:
      - targets: ['192.168.1.10:8088']
  
  - job_name: 'spark-master'
    static_configs:
      - targets: ['192.168.1.10:8080']
```

### 8.2 Регулярные задачи обслуживания

**Cron jobs для очистки логов**:

```bash
# На всех серверах
crontab -e

# Очистка старых логов Hadoop (старше 30 дней)
0 2 * * * find /opt/hadoop/logs -name "*.log*" -mtime +30 -delete

# Очистка старых логов Spark
0 3 * * * find /opt/spark/work -mtime +7 -delete

# Балансировка HDFS (каждую неделю)
0 1 * * 0 hdfs balancer -threshold 10
```

**Backup HDFS metadata**:

```bash
#!/bin/bash
# backup-hdfs-metadata.sh

BACKUP_DIR="/backup/hdfs"
DATE=$(date +%Y%m%d)

mkdir -p $BACKUP_DIR

# Backup NameNode metadata
hdfs dfsadmin -fetchImage $BACKUP_DIR/fsimage_$DATE

# Backup edits logs
cp /opt/hadoop/hdfs/namenode/current/edits_* $BACKUP_DIR/

echo "HDFS metadata backed up to $BACKUP_DIR"
```

### 8.3 Health checks

**Скрипт проверки здоровья кластера**:

```bash
#!/bin/bash
# cluster-health-check.sh

echo "=== HDFS Health ==="
hdfs dfsadmin -report | grep -E "Live datanodes|Dead datanodes|Under replicated blocks"

echo -e "\n=== YARN Health ==="
yarn node -list -all | grep -E "Total Nodes|RUNNING|UNHEALTHY"

echo -e "\n=== Disk Usage ==="
df -h | grep -E "Filesystem|/opt/hadoop"

echo -e "\n=== ZooKeeper Status ==="
ssh hadoop-master "zkServer.sh status"
ssh hadoop-worker1 "zkServer.sh status"
ssh hadoop-worker2 "zkServer.sh status"

echo -e "\n=== Kafka Brokers ==="
kafka-broker-api-versions.sh --bootstrap-server hadoop-master:9092
```

---

## 9. ЧЕКЛИСТ ЗАПУСКА

### 9.1 Предварительные проверки

- [ ] Все 3 сервера физически подключены по Ethernet
- [ ] Все серверы имеют статические IP адреса
- [ ] /etc/hosts настроен на всех машинах
- [ ] SSH без пароля работает между всеми нодами
- [ ] Java 11 установлена на всех серверах
- [ ] Пользователь hadoop создан на всех серверах
- [ ] Firewall настроен или отключен

### 9.2 Порядок запуска компонентов

**1. ZooKeeper (на всех трех серверах)**
```bash
zkServer.sh start
zkServer.sh status  # Проверить leader/follower
```

**2. HDFS (с Master ноды)**
```bash
start-dfs.sh
hdfs dfsadmin -report  # Проверить DataNodes
```

**3. YARN (с Master ноды)**
```bash
start-yarn.sh
yarn node -list  # Проверить NodeManagers
```

**4. Kafka (на всех трех серверах)**
```bash
kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties
jps | grep Kafka  # Проверить запуск
```

**5. Spark (с Master ноды)**
```bash
start-master.sh
start-workers.sh spark://hadoop-master:7077
# Проверить http://192.168.1.10:8080
```

**6. Flume (на Worker нодах)**
```bash
flume-ng agent --conf /opt/flume/conf \
    --conf-file /opt/flume/conf/flume-kafka-hdfs.conf \
    --name agent1 -Dflume.root.logger=INFO,console &
```

**7. Node.js API (на ноутбуке)**
```bash
cd zeer-bigdata-api
npm install
npm start
```

### 9.3 Тестирование pipeline end-to-end

```bash
# 1. Отправить тестовый лог через Node.js API
curl -X POST http://localhost:3000/api/logs/user-action \
  -H "Content-Type: application/json" \
  -d '{
    "timestamp": "2026-09-21T15:30:00Z",
    "userId": "test_user",
    "action": "product_view",
    "productId": "prod_123"
  }'

# 2. Проверить, что сообщение попало в Kafka
kafka-console-consumer.sh --bootstrap-server hadoop-master:9092 \
    --topic zeer-user-actions --from-beginning --max-messages 1

# 3. Подождать 10 минут и проверить, что Flume записал в HDFS
hdfs dfs -ls /zeer/logs/zeer-user-actions/

# 4. Прочитать файл из HDFS
hdfs dfs -cat /zeer/logs/zeer-user-actions/*/events*.log | head -n 5

# 5. Запустить Spark job для анализа
spark-submit --master spark://hadoop-master:7077 \
    --class com.zeer.analytics.UserActionsAnalyzer \
    /opt/zeer/jars/zeer-analytics.jar \
    hdfs://hadoop-master:8020/zeer/logs/zeer-user-actions \
    hdfs://hadoop-master:8020/zeer/analytics/user-actions

# 6. Проверить результаты
hdfs dfs -ls /zeer/analytics/user-actions/
```

---

## 10. TROUBLESHOOTING

### 10.1 Частые проблемы

**Проблема: DataNode не подключается к NameNode**

Решение:
```bash
# Проверить логи DataNode
tail -f /opt/hadoop/logs/hadoop-hadoop-datanode-*.log

# Проверить сетевое подключение
telnet hadoop-master 8020

# Пересоздать DataNode ID (если конфликт)
rm -rf /opt/hadoop/hdfs/datanode/current/BP-*/current/VERSION
hdfs datanode -format
```

**Проблема: Недостаточно памяти для YARN контейнеров**

Решение:
```xml
<!-- Уменьшить размер контейнеров в yarn-site.xml -->
<property>
    <name>yarn.nodemanager.resource.memory-mb</name>
    <value>4096</value> <!-- Было 6144 -->
</property>
```

**Проблема: Kafka не может записывать из-за нехватки места**

Решение:
```bash
# Уменьшить retention время
kafka-configs.sh --bootstrap-server hadoop-master:9092 \
    --entity-type topics --entity-name zeer-user-actions \
    --alter --add-config retention.ms=86400000  # 1 день вместо 7

# Или очистить старые сегменты
kafka-delete-records.sh --bootstrap-server hadoop-master:9092 \
    --offset-json-file delete.json
```

### 10.2 Полезные команды диагностики

```bash
# HDFS
hdfs fsck /                                    # Проверка файловой системы
hdfs dfsadmin -safemode get                    # Проверка safe mode
hdfs dfs -du -h /zeer                          # Использование места

# YARN
yarn application -list                         # Список приложений
yarn application -status application_id        # Статус приложения
yarn logs -applicationId application_id        # Логи приложения

# Kafka
kafka-topics.sh --list --bootstrap-server hadoop-master:9092
kafka-consumer-groups.sh --bootstrap-server hadoop-master:9092 --list
kafka-consumer-groups.sh --bootstrap-server hadoop-master:9092 \
    --group flume-consumer --describe

# Spark
spark-submit --status application_id
```

---

## 11. ДАЛЬНЕЙШЕЕ РАЗВИТИЕ

### 11.1 Оптимизации

1. **Компрессия данных в HDFS**
   - Использовать Snappy или LZ4 для логов
   - Конвертировать JSON в Parquet/ORC для аналитики

2. **Партиционирование данных**
   - Партиционировать по дате и типу лога
   - Использовать bucketing для частых join операций

3. **Кеширование в Spark**
   - Кешировать часто используемые DataFrame
   - Использовать Alluxio для in-memory кеширования HDFS

4. **Kafka Streams для real-time обработки**
   - Агрегация метрик в реальном времени
   - Сложные event processing паттерны

### 11.2 Добавление новых компонентов

1. **Apache Hive**
   - SQL-доступ к данным в HDFS
   - Интеграция с BI инструментами

2. **Apache HBase**
   - Быстрый случайный доступ к логам
   - Time-series данные для метрик

3. **Elasticsearch + Kibana**
   - Full-text поиск по логам
   - Визуализация в реальном времени

4. **Apache Airflow**
   - Оркестрация batch jobs
   - Управление зависимостями между задачами

### 11.3 Масштабирование

Когда понадобится больше ресурсов:

1. Добавить 4-й и 5-й сервер как Worker nodes
2. Настроить HDFS Federation для масштабирования NameNode
3. Перейти на External ZooKeeper ensemble (5+ нод)
4. Использовать Kafka MirrorMaker для репликации между дата-центрами

---

## ЗАКЛЮЧЕНИЕ

Этот план покрывает полный цикл создания домашней Big Data инфраструктуры:

1. ✅ Топология и роли серверов
2. ✅ Установка и конфигурация всех компонентов
3. ✅ Интеграция с Node.js
4. ✅ Разработка data pipeline
5. ✅ Обработка логов Zeer marketplace
6. ✅ Локальная разработка и тестирование
7. ✅ Мониторинг и обслуживание
8. ✅ Troubleshooting

**Рекомендуемый порядок выполнения**:

**Неделя 1**: Настройка базовой инфраструктуры (серверы, сеть, Hadoop, YARN)
**Неделя 2**: ZooKeeper, Kafka, Spark
**Неделя 3**: Flume, Node.js интеграция, первый test pipeline
**Неделя 4**: Разработка Spark jobs, мониторинг, оптимизация

**Ключевые моменты**:

- Начните с минимальной конфигурации и постепенно добавляйте компоненты
- Тестируйте каждый компонент отдельно перед интеграцией
- Используйте Docker для локальной разработки
- Документируйте все изменения конфигурации
- Регулярно делайте backup HDFS metadata

Удачи с проектом Zeer! 🚀
