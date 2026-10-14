# Урок 12. Потоковая обработка: демо Kafka Streams и аналог на Node.js

Kafka Streams — библиотека только для Java/Scala, поэтому в этом уроке две части. В первой вы запускаете готовое демо «подсчёт слов» (практика А ниже) без написания Java-кода. Во второй пишете похожую задачу на Node.js и видите, что приходится делать руками без библиотеки.

### Теория

[Kafka Streams](https://kafka.apache.org/documentation/streams) — библиотека для Java и Scala, с помощью которой обычное приложение читает топики, обрабатывает данные и пишет результат в другие топики. Отдельного кластера для обработки не нужно: приложение запускается как обычная программа, а масштабируется запуском нескольких копий.

| Понятие | Что это |
| --- | --- |
| `KStream` | Бесконечный поток событий; каждое событие — отдельный факт |
| `KTable` | Таблица текущих значений по ключу; новое событие с тем же ключом обновляет строку |
| Stateless-операции | `filter`, `map`, `flatMapValues`: результат зависит только от текущего события |
| Stateful-операции | `count`, `aggregate`, `join`, окна: нужно хранить состояние |
| State store и changelog | Локальное хранилище состояния и его копия в служебном топике Kafka для восстановления |
| `application.id` | Имя приложения; одновременно служит идентификатором группы потребителей |

Главная идея — **двойственность потока и таблицы**: изменения таблицы можно выложить в виде потока (changelog), а проиграв поток с начала, можно восстановить таблицу. Поэтому вывод подсчёта слов — это не итоговая таблица, а поток обновлений, где поздняя запись по ключу заменяет раннюю.

### Практика А. Демо Kafka Streams

Нужен запущенный брокер Kafka. Сценарий взят из [официального Streams Quickstart](https://kafka.apache.org/43/streams/quickstart/); команды выполняются из каталога установки Kafka.

**Шаг 1. Создайте входной и выходной топики**

```bash
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 \
  --replication-factor 1 --partitions 1 --topic streams-plaintext-input
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 \
  --replication-factor 1 --partitions 1 --topic streams-wordcount-output \
  --config cleanup.policy=compact
```

Выходной топик создан со сжатием, потому что он хранит changelog-поток (урок 10).

**Шаг 2. Запустите приложение**

```bash
bin/kafka-run-class.sh org.apache.kafka.streams.examples.wordcount.WordCountDemo
```

В терминале будут только записи журнала: результат приложение пишет в Kafka.

**Шаг 3. Откройте консьюмер выходного топика** (новый терминал)

```bash
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic streams-wordcount-output --from-beginning \
  --formatter-property print.key=true --formatter-property print.value=true \
  --formatter-property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
  --formatter-property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
```

**Шаг 4. Отправьте текст** (ещё один терминал)

```bash
bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic streams-plaintext-input
> all streams lead to kafka
> hello kafka streams
> join kafka summit
```

После трёх строк в выводе консьюмера должны появиться пары «слово, счётчик», причём `kafka` встречается трижды с растущим значением (1, 2, 3): это и есть поток обновлений. Объясните письменно, почему в топике три записи для ключа `kafka`, а не одна.

Остановите приложение и консьюмер через `Ctrl-C`.

Запомните её результат: таблица «слово → счётчик» обновляется с каждым новым событием.

### Практика Б. Счётчик статусов на Node.js

Задача: в топике `order-status` лежат события «заказ (ключ) перешёл в статус (значение)». Нужно всегда знать, сколько заказов сейчас в каждом статусе, и публиковать эти числа в топик `status-counts` (ключ — статус, значение — число).

Шаг 1. Создайте топики. Топик `status-counts` хранит только последнее значение каждого статуса, поэтому делаем его сжимаемым (`cleanup.policy=compact`, урок 10):

```bash
kafka-topics.sh --bootstrap-server localhost:9092 --create --topic order-status --partitions 3
kafka-topics.sh --bootstrap-server localhost:9092 --create --topic status-counts --partitions 1 \
  --config cleanup.policy=compact
```

Шаг 2. Файл `status_count.js`:

```javascript
const { Kafka } = require('@confluentinc/kafka-javascript').KafkaJS;

const BOOTSTRAP = process.env.BOOTSTRAP || 'localhost:9092';

async function main() {
  const kafka = new Kafka();
  const producer = kafka.producer({ 'bootstrap.servers': BOOTSTRAP });
  await producer.connect();

  // Состояние: текущий статус каждого заказа и счётчики по статусам.
  const orderStatus = new Map();   // номер заказа -> статус
  const counts = new Map();        // статус -> сколько заказов сейчас в нём

  const consumer = kafka.consumer({
    'bootstrap.servers': BOOTSTRAP,
    // Новая группа при каждом запуске + чтение с начала: состояние восстанавливается
    // проигрыванием журнала, так же как Kafka Streams восстанавливает его из changelog.
    'group.id': 'status-count-' + Date.now(),
    'auto.offset.reset': 'earliest',
  });
  await consumer.connect();
  await consumer.subscribe({ topics: ['order-status'] });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const orderId = message.key.toString();
      const status = message.value.toString();
      if (!status.trim()) return;

      const previous = orderStatus.get(orderId);
      if (previous === status) return;
      orderStatus.set(orderId, status);

      const changed = new Set([status]);
      if (previous !== undefined) {
        counts.set(previous, counts.get(previous) - 1); // заказ ушёл из старого статуса
        changed.add(previous);
      }
      counts.set(status, (counts.get(status) || 0) + 1);

      // публикуем новые значения счётчиков (ключ — статус)
      await producer.send({
        topic: 'status-counts',
        messages: [...changed].map((s) => ({ key: s, value: String(counts.get(s)) })),
      });
    },
  });

  process.on('SIGINT', async () => {
    await consumer.disconnect();
    await producer.disconnect();
    process.exit(0);
  });
}

main().catch((err) => {
  console.error('Ошибка:', err);
  process.exit(1);
});
```

Шаг 3. Отправьте тестовые события и прочитайте результат:

```bash
printf '1:created\n2:created\n3:created\n1:paid\n' | kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic order-status \
  --property parse.key=true --property key.separator=:
node status_count.js            # в одном терминале
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic status-counts \
  --from-beginning --property print.key=true       # в другом
```

В потоке `status-counts` должны появиться записи вроде `created 1`, `created 2`, `created 3`, затем `paid 1` и `created 2`; итоговые значения — `created=2`, `paid=1`. Порядок записей между партициями `order-status` не гарантирован, но для одного заказа (один ключ) он сохраняется.

Шаг 4. Восстановление состояния. Остановите скрипт (Ctrl+C) и запустите снова: он проиграет журнал `order-status` с нуля и получит то же состояние. Отметьте: повторный запуск заново публикует те же значения в `status-counts` — благодаря сжатию топик всё равно хранит по одной актуальной записи на статус.

Чего здесь нет по сравнению с Kafka Streams и в чём разница: состояние хранится в памяти процесса и не дублируется в changelog-топик, поэтому восстанавливать его приходится проигрыванием всего исходного топика; нет окон, ребаланса состояния между экземплярами и гарантии exactly-once между чтением и записью (при падении между чтением и публикацией возможен повтор). Для учебной задачи этого достаточно; для рабочей системы с состоянием нужны окна, персистентное хранилище и транзакции, то есть Kafka Streams (Java) или другой потоковый движок.

Статус проверки: `status_count.js` запускался на имитации кластера из библиотеки клиента; итог `created=2`, `paid=1` получен на том же наборе событий. Демо Kafka Streams на настоящем кластере не запускалось.

### Контрольные вопросы

- Чем таблица (состояние) отличается от потока событий?
- Почему для `status-counts` подходит сжатие, а для `order-status` нет?
- Как скрипт восстанавливает состояние после перезапуска и что будет с временем запуска, если журнал вырастет до миллионов записей?
- Что произойдёт, если запустить два экземпляра скрипта одновременно?

### Самостоятельное задание

Добавьте в скрипт второй счётчик: число событий в минуту (простое «окно» на массиве времени событий или на `message.timestamp`) и публикацию его в топик `events-per-minute`. Опишите, какие проблемы возникнут при поздних событиях и при перезапуске.
