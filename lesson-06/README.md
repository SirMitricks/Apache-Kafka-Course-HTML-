# Урок 6. Репликация и кластер из трёх брокеров

**Цель:** поднять локальный кластер из трёх узлов, создать реплицируемый топик и пронаблюдать, как Kafka переживает отказ брокера.

### Теория

Чтобы данные не пропали при поломке сервера, каждая партиция хранится в нескольких копиях — **репликах**. Число копий задаёт **фактор репликации** (replication factor); в промышленной эксплуатации типичное значение равно 3. Одна из реплик партиции — **лидер**: через неё идут все записи и чтения клиентов. Остальные — **фолловеры**, они копируют данные с лидера.

Множество реплик, которые успевают за лидером, называется **ISR** (in-sync replicas). Если лидер недоступен, новым лидером становится одна из реплик из ISR, и работа продолжается без потери подтверждённых записей.

Два параметра надёжности, которые мы используем дальше:

- `replication.factor` — сколько копий партиции хранится в кластере.
- `min.insync.replicas` — сколько реплик из ISR должно быть в наличии, чтобы запись с `acks=all` считалась успешной (подробности в уроке 9).

Ещё одно изменение в Kafka 4.x: управлением метаданными занимается **кворум (минимальное большинство участников, чьё согласие нужно для решения) контроллеров** KRaft. По [документации](https://kafka.apache.org/43/operations/kraft/), для надёжной работы нужно 2N + 1 контроллеров, чтобы пережить N отказов, и большинство контроллеров должно быть живо. Для учебного кластера мы запустим три узла, каждый из которых одновременно и брокер, и контроллер; документация допускает такую «совмещённую» схему для разработки, но не советует её для критичных систем.

### Практика

Все команды выполняются из каталога дистрибутива. Сначала остановите одиночный брокер из урока 2 (`Ctrl-C` в его терминале), чтобы освободить порт 9092.

**Шаг 1. Подготовьте конфигурации трёх узлов**

Порты брокеров: 9092, 9192, 9292; порты контроллеров: 9093, 9193, 9293.

```bash
mkdir -p config/cluster
for i in 1 2 3; do
  B=$((9092 + (i-1)*100)); C=$((B+1))
  cat > config/cluster/node$i.properties <<EOF
process.roles=broker,controller
node.id=$i
controller.quorum.voters=1@localhost:9093,2@localhost:9193,3@localhost:9293
listeners=PLAINTEXT://localhost:$B,CONTROLLER://localhost:$C
advertised.listeners=PLAINTEXT://localhost:$B
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
log.dirs=/tmp/kraft-cluster/node$i
offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2
EOF
done
```

Здесь используется статический кворум (`controller.quorum.voters`): он проще для обучения. В документации Kafka он назван «старым способом», а для рабочих кластеров рекомендован динамический кворум; с ним познакомьтесь в самостоятельном задании.

**Шаг 2. Отформатируйте узлы одним идентификатором кластера**

```bash
CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
for i in 1 2 3; do
  bin/kafka-storage.sh format -t $CLUSTER_ID -c config/cluster/node$i.properties
done
```

**Шаг 3. Запустите три брокера**

Удобнее всего открыть по терминалу на каждый узел:

```bash
bin/kafka-server-start.sh config/cluster/node1.properties
bin/kafka-server-start.sh config/cluster/node2.properties
bin/kafka-server-start.sh config/cluster/node3.properties
```

Проверьте состояние кворума контроллеров:

```bash
bin/kafka-metadata-quorum.sh --bootstrap-server localhost:9092 describe --status
```

В выводе должны быть `LeaderId` и три записи в `CurrentVoters`.

**Шаг 4. Создайте реплицируемый топик**

```bash
bin/kafka-topics.sh --create --topic resilient --partitions 3 --replication-factor 3 \
  --bootstrap-server localhost:9092,localhost:9192,localhost:9292
bin/kafka-topics.sh --describe --topic resilient --bootstrap-server localhost:9092
```

Для каждой партиции запишите `Leader`, `Replicas` и `Isr`. Обычно лидеры распределены по разным брокерам, а в `Isr` перечислены все три узла.

**Шаг 5. Запишите данные и отключите брокер**

1. Запишите 10 событий через `kafka-console-producer.sh` с `--bootstrap-server localhost:9092,localhost:9192,localhost:9292`.
2. Остановите узел 2 (`Ctrl-C` в его терминале).
3. Повторите `--describe`: у партиций, лидером которых был узел 2, появился новый лидер, а в `Isr` осталось два узла.
4. Прочитайте топик консьюмером с `--from-beginning`: все 10 событий должны быть на месте.
5. Запишите ещё несколько событий. Запись продолжает работать.

**Шаг 6. Верните брокер и дождитесь синхронизации**

Запустите узел 2 заново и подождите несколько секунд. Снова выполните `--describe`: в `Isr` должны вернуться все три узла.

**Шаг 7. Потеряйте кворум**

Остановите два узла из трёх и попробуйте создать топик или записать событие через оставшийся. Запишите, что произошло, и объясните это правилом «нужно большинство контроллеров». Затем верните узлы.

### Практика Б. Тот же кластер в Docker Compose

Если конфигурации из практики А не получаются или нет времени на ручную сборку, поднимите такой же кластер из трёх брокеров одной командой. Docker Compose (инструмент, который запускает несколько контейнеров по одному YAML-файлу) создаст три контейнера, а каждый узел будет и брокером, и контроллером KRaft, как в практике А. Нужны Docker с плагином Compose (`docker compose version`) и свободные порты 29092, 39092, 49092.

Создайте каталог `kafka-cluster` и в нём файл `docker-compose.yml`:

```yaml
# Кластер Kafka 4.3.1 из трёх узлов (брокер + контроллер KRaft на каждом).
# Подключение с вашего компьютера:  localhost:29092,localhost:39092,localhost:49092
# Подключение из других контейнеров: kafka-1:19092,kafka-2:19092,kafka-3:19092

x-kafka-common: &kafka-common
  image: apache/kafka:4.3.1
  environment: &kafka-env
    KAFKA_PROCESS_ROLES: "broker,controller"
    KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT"
    KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093"
    KAFKA_LISTENERS: "PLAINTEXT://:19092,CONTROLLER://:9093,PLAINTEXT_HOST://:9092"
    KAFKA_INTER_BROKER_LISTENER_NAME: "PLAINTEXT"
    KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
    CLUSTER_ID: "4L6g3nShT-eMCtK--X86sw"
    KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
    KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3
    KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 2
    KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR: 3
    KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR: 2
    KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
    KAFKA_LOG_DIRS: "/tmp/kraft-combined-logs"

services:
  kafka-1:
    <<: *kafka-common
    hostname: kafka-1
    container_name: kafka-1
    ports:
      - "29092:9092"
    environment:
      <<: *kafka-env
      KAFKA_NODE_ID: 1
      KAFKA_ADVERTISED_LISTENERS: "PLAINTEXT://kafka-1:19092,PLAINTEXT_HOST://localhost:29092"

  kafka-2:
    <<: *kafka-common
    hostname: kafka-2
    container_name: kafka-2
    ports:
      - "39092:9092"
    environment:
      <<: *kafka-env
      KAFKA_NODE_ID: 2
      KAFKA_ADVERTISED_LISTENERS: "PLAINTEXT://kafka-2:19092,PLAINTEXT_HOST://localhost:39092"

  kafka-3:
    <<: *kafka-common
    hostname: kafka-3
    container_name: kafka-3
    ports:
      - "49092:9092"
    environment:
      <<: *kafka-env
      KAFKA_NODE_ID: 3
      KAFKA_ADVERTISED_LISTENERS: "PLAINTEXT://kafka-3:19092,PLAINTEXT_HOST://localhost:49092"

  # Нужен только для урока 8 (запускается лишь с --profile registry)
  schema-registry:
    image: confluentinc/cp-schema-registry:8.3.2
    profiles: ["registry"]
    hostname: schema-registry
    container_name: schema-registry
    restart: on-failure
    depends_on:
      - kafka-1
      - kafka-2
      - kafka-3
    ports:
      - "8081:8081"
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_LISTENERS: "http://0.0.0.0:8081"
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: "PLAINTEXT://kafka-1:19092,PLAINTEXT://kafka-2:19092,PLAINTEXT://kafka-3:19092"
```

Шаг Б1. Запустите кластер и проверьте, что все три контейнера работают:

```bash
docker compose up -d
docker compose ps
docker exec -it kafka-1 /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server kafka-1:19092 describe --status
```

В выводе должны быть LeaderId и три записи в CurrentVoters. Если контейнеры сразу останавливаются, посмотрите причину командой `docker compose logs kafka-1`.

Шаг Б2. Создайте реплицируемый топик. Команды Kafka здесь выполняются внутри контейнера, поэтому адрес брокера `kafka-1:19092`:

```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh --create --topic replicated \
  --partitions 3 --replication-factor 3 --bootstrap-server kafka-1:19092
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh --describe --topic replicated \
  --bootstrap-server kafka-1:19092
```

Шаг Б3. Повторите опыт с отказом брокера из шагов 5–6 практики А. Остановите узел и посмотрите, как изменились Leader и Isr:

```bash
docker stop kafka-2
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh --describe --topic replicated \
  --bootstrap-server kafka-1:19092
docker start kafka-2
```

Шаг Б4. Подключайте клиентов с вашего компьютера (Python из урока 7, Java из урока 12) по адресам `localhost:29092,localhost:39092,localhost:49092`; в примерах вместо `localhost:9092` подставляйте этот список. Адреса `kafka-1:19092` и т. д. работают только внутри сети Docker.

Шаг Б5. Остановка. `docker compose down` останавливает контейнеры, а `docker compose down -v` ещё и удаляет данные, чтобы начать с чистого листа.

**Примечание.** Параметры контейнера `apache/kafka` взяты из официального примера Compose в репозитории Kafka 4.3, синтаксис файла проверен командой `docker compose config`. В среде автора запустить Docker не удалось, поэтому сам запуск не проверялся; если у вас что-то разойдётся с описанием, начните с docker compose logs.

### Контрольные вопросы

- В чём разница между лидером и фолловером?
- Что такое ISR и почему лидер выбирается именно из него?
- Почему кластер из трёх контроллеров переживает один отказ, но не два?
- Почему нельзя создать топик с `--replication-factor 3` на кластере из двух брокеров?

### Самостоятельное задание

Прочитайте раздел KRaft документации про динамический кворум (`--standalone`, `--initial-controllers`) и опишите, чем он отличается от статического. Затем пересоберите кластер в варианте с отдельными контроллерами (по желанию, повышенная сложность).
