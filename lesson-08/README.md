# Урок 8. Форматы данных: JSON, Avro и Schema Registry (Node.js)

Цель: сравнить JSON и Avro по размеру, изменить схему события без поломки читателей и подключить реестр схем к клиентам на JavaScript.

### Теория

Kafka ничего не знает о содержимом событий: для неё ключ и значение — просто последовательности байтов. Превращение данных в байты называется сериализацией (serialization), обратное превращение — десериализацией (deserialization). В уроках 4 и 7 мы отправляли строки: это самый простой формат, но он не передаёт структуру («что такое поле status?») и ничего не проверяет.

В реальных проектах продюсер и консьюмер пишут разные команды, а структура событий меняется со временем. Поэтому нужен договор об их формате, который называют схемой (schema): списком полей, их типов и значений по умолчанию. Основные варианты:

| Формат | Размер | Схема | Когда применяют |
| --- | --- | --- | --- |
| Строка | минимальный | нет | простые сообщения, обучение |
| JSON | большой: имена полей повторяются в каждом событии | нет (по желанию — JSON Schema) | удобно читать глазами, прототипы |
| Avro | малый: хранятся только значения, без имён полей | обязательна | события в промышленных системах, эволюция схем |
| Protobuf | малый | обязательна | то же, особенно в связке с gRPC |

Avro (формат сериализации из проекта Apache, где схема записывается в JSON-файле .avsc) хранит в событии только значения, поэтому читателю нужна та же схема, что у писателя. Передавать схему в каждом событии дорого, а договариваться о ней вручную ненадёжно. Эту проблему решает Schema Registry (реестр схем): отдельный сервис, который хранит схемы и выдаёт каждой номер (schema id). Продюсер регистрирует схему и записывает в начало события этот номер, консьюмер читает номер и запрашивает схему из реестра. Важно: Schema Registry не входит в Apache Kafka, это отдельный продукт компании Confluent; для обучения мы запускаем его готовым образом Docker.

Событие в таком формате (его называют wire format, «формат на проводе») устроено так: 1 байт со значением 0 (магический байт), 4 байта с номером схемы, затем данные Avro. Схемы хранятся в реестре по именам, которые называются subject («предмет»). По умолчанию имя равно `<топик>-value` для значений и `<топик>-key` для ключей, например `orders-avro-value`. У каждого subject есть версии схемы: 1, 2, 3...

Когда схема меняется, реестр проверяет совместимость (compatibility) новой версии со старыми. Режимы:

- `BACKWARD` (по умолчанию): читатель с новой схемой читает старые данные. Можно добавить поле со значением по умолчанию или удалить поле. Обновлять сначала консьюмеров.
- `FORWARD`: читатель со старой схемой читает новые данные. Можно добавить поле или удалить поле, у которого есть значение по умолчанию. Обновлять сначала продюсеров.
- `FULL`: выполняются оба условия.
- `NONE`: проверки нет.

К названию режима можно добавить суффикс `_TRANSITIVE`: тогда новая схема проверяется не только по последней версии, но и по всем предыдущим.

Правило, которое нужно запомнить: новое поле без значения по умолчанию ломает совместимость, потому что старые события такого поля не содержат и заполнить его нечем.

Специфика JavaScript: библиотека `avsc` кодирует и декодирует Avro, а `@confluentinc/schemaregistry` на её основе общается с реестром и добавляет заголовок с номером схемы. Сериализатор возвращает `Buffer`, который мы сами кладём в `value` сообщения, а десериализатор принимает `Buffer` из `message.value`. В самой библиотеке есть ещё автоматические «сборщики» сериализаторов в настройках клиента, но их авторы помечают как экспериментальные (могут измениться), поэтому в уроке вызываем сериализатор явно.

### Практика

Практика А и Б не требуют Kafka. Практика В и Г требуют кластера из Docker Compose (урок 6, практика Б) с профилем `registry`; скрипты В и Г по умолчанию обращаются к адресам `localhost:29092,localhost:39092,localhost:49092` и реестру `http://localhost:8081`.

**Практика А. Сколько места занимает событие.** Файл `sizes.js`:

```javascript
const avro = require('avsc');

const type = avro.Type.forSchema({
  type: 'record',
  name: 'OrderEvent',
  namespace: 'shop',
  fields: [
    { name: 'order_id', type: 'int' },
    { name: 'status', type: 'string' },
    { name: 'amount', type: 'double' },
  ],
});

const event = { order_id: 123456, status: 'created', amount: 1999.9 };

const jsonBytes = Buffer.from(JSON.stringify(event), 'utf8');
const avroBytes = type.toBuffer(event);

console.log('JSON :', jsonBytes.length, 'байт ->', jsonBytes.toString());
console.log('Avro :', avroBytes.length, 'байт ->', avroBytes);
```

Ожидаемый результат: JSON — 54 байта, Avro — 19 байт (`JSON.stringify` не добавляет пробелы, поэтому число меньше, чем у Python-версии с 59 байтами). Запишите отношение и объясните, что есть в JSON и нет в Avro.

**Практика Б. Эволюция схемы без реестра.** Файл `evolve.js`:

```javascript
const avro = require('avsc');

const fields = [
  { name: 'order_id', type: 'int' },
  { name: 'status', type: 'string' },
  { name: 'amount', type: 'double' },
];
const record = (extra) => avro.Type.forSchema({
  type: 'record', name: 'OrderEvent', namespace: 'shop', fields: [...fields, ...extra],
});

const v1 = record([]);
const v2 = record([{ name: 'currency', type: 'string', default: 'RUB' }]);
const v3Bad = record([{ name: 'currency', type: 'string' }]); // нет default

const data = v1.toBuffer({ order_id: 1, status: 'created', amount: 10.5 });

// резольвер = «как прочитать данные, записанные схемой v1, схемой читателя»
console.log('читаем старые данные схемой v2:',
  v2.fromBuffer(data, v2.createResolver(v1)));
try {
  v3Bad.createResolver(v1);
} catch (e) {
  console.log('ошибка v3 (поле без default):', e.message);
}
```

Ожидаемый результат: схема v2 читает старое событие и подставляет `currency: 'RUB'`; для v3 библиотека бросает ошибку `no matching field for default-less shop.OrderEvent.currency`. Здесь `v1` — схема писателя (по ней записаны данные), а вторая схема — схема читателя.

**Практика В. Schema Registry.** Шаг В1. Из каталога `kafka-cluster` (урок 6, практика Б) запустите кластер вместе с реестром и дождитесь ответа (на пустом реестре — `[]`):

```bash
docker compose --profile registry up -d
curl http://localhost:8081/subjects
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh --create --topic orders-avro \
  --partitions 3 --replication-factor 3 --bootstrap-server kafka-1:19092
```

Шаг В2. Файлы схем. `order_v1.avsc`:

```json
{
  "type": "record",
  "name": "OrderEvent",
  "namespace": "shop",
  "fields": [
    {"name": "order_id", "type": "int"},
    {"name": "status", "type": "string"},
    {"name": "amount", "type": "double"}
  ]
}
```

`order_v2.avsc` добавляет поле `currency` со значением по умолчанию:

```json
{
  "type": "record",
  "name": "OrderEvent",
  "namespace": "shop",
  "fields": [
    {"name": "order_id", "type": "int"},
    {"name": "status", "type": "string"},
    {"name": "amount", "type": "double"},
    {"name": "currency", "type": "string", "default": "RUB"}
  ]
}
```

Шаг В3. Продюсер `avro_producer.js`. Сначала он регистрирует схему в реестре, затем сериализует каждое событие:

```javascript
const fs = require('fs');
const { Kafka } = require('@confluentinc/kafka-javascript').KafkaJS;
const { SchemaRegistryClient, AvroSerializer, SerdeType } = require('@confluentinc/schemaregistry');

const BOOTSTRAP = process.env.BOOTSTRAP || 'localhost:29092,localhost:39092,localhost:49092';
const REGISTRY = process.env.REGISTRY || 'http://localhost:8081';
const topic = 'orders-avro';
const schemaFile = process.argv[2]; // order_v1.avsc или order_v2.avsc

async function main() {
  const schema = fs.readFileSync(schemaFile, 'utf8');
  const registry = new SchemaRegistryClient({ baseURLs: [REGISTRY] });

  // Регистрируем схему под именем &lt;топик&gt;-value. Если она несовместима с прошлыми
  // версиями, реестр ответит ошибкой (HTTP 409), и событий в топике не появится.
  const schemaId = await registry.register(`${topic}-value`, { schemaType: 'AVRO', schema });
  console.log(`Схема зарегистрирована, id=${schemaId}`);

  // useLatestVersion: брать последнюю схему subject'а из реестра
  const serializer = new AvroSerializer(registry, SerdeType.VALUE, { useLatestVersion: true });

  const producer = new Kafka().producer({ 'bootstrap.servers': BOOTSTRAP });
  await producer.connect();

  for (let orderId = 1; orderId <= 3; orderId++) {
    const event = { order_id: orderId, status: 'created', amount: 100.0 * orderId };
    if (schemaFile.includes('v2')) event.currency = 'USD';
    const [report] = await producer.send({
      topic,
      messages: [{ key: String(orderId), value: await serializer.serialize(topic, event) }],
    });
    console.log(`Доставлено: партиция=${report.partition} offset=${report.baseOffset}`);
  }
  await producer.disconnect();
}

main().catch((err) => {
  console.error('Ошибка:', err.message || err);
  process.exit(1);
});
```

Запустите `node avro_producer.js order_v1.avsc`: должны появиться «Схема зарегистрирована, id=1» и три строки «Доставлено».

Шаг В4. Консьюмер `avro_consumer.js`. Он при каждом запуске создаёт новую группу, читает топик с начала и выходит, если 10 секунд нет событий; рядом с событием он печатает магический байт и номер схемы из заголовка:

```javascript
const { Kafka } = require('@confluentinc/kafka-javascript').KafkaJS;
const { SchemaRegistryClient, AvroDeserializer, SerdeType } = require('@confluentinc/schemaregistry');

const BOOTSTRAP = process.env.BOOTSTRAP || 'localhost:29092,localhost:39092,localhost:49092';
const REGISTRY = process.env.REGISTRY || 'http://localhost:8081';
const topic = 'orders-avro';

async function main() {
  const registry = new SchemaRegistryClient({ baseURLs: [REGISTRY] });
  const deserializer = new AvroDeserializer(registry, SerdeType.VALUE, {}); // схему записи десериализатор берёт из реестра по id

  const consumer = new Kafka().consumer({
    'bootstrap.servers': BOOTSTRAP,
    'group.id': 'avro-reader-' + Date.now(), // новая группа при каждом запуске
    'auto.offset.reset': 'earliest',
  });
  await consumer.connect();
  await consumer.subscribe({ topics: [topic] });

  let lastMessage = Date.now();
  await consumer.run({
    eachMessage: async ({ message }) => {
      lastMessage = Date.now();
      const raw = message.value;
      const magic = raw.readUInt8(0);        // заголовок: 1 байт...
      const schemaId = raw.readUInt32BE(1);  // ...и 4 байта с номером схемы
      const event = await deserializer.deserialize(topic, raw);
      console.log(`magic=${magic} schema_id=${schemaId} размер=${raw.length} байт -> ${JSON.stringify(event)}`);
    },
  });

  // выходим, если 10 секунд нет новых событий
  while (Date.now() - lastMessage < 10000) {
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
  await consumer.disconnect();
}

main().catch((err) => {
  console.error('Ошибка:', err.message || err);
  process.exit(1);
});
```

Запустите `node avro_consumer.js`. Ожидаемый результат: три строки с `magic=0`, `schema_id=1` и размером 22 байта (5 байт заголовка и 17 байт данных); порядок строк между партициями может отличаться.

Шаг В5. Загляните в реестр через REST (HTTP-интерфейс сервиса):

```bash
curl http://localhost:8081/subjects
curl http://localhost:8081/subjects/orders-avro-value/versions
curl http://localhost:8081/schemas/ids/1
curl http://localhost:8081/config
```

Должны быть видны subject `orders-avro-value`, версия 1, текст схемы и режим совместимости `BACKWARD`.

Шаг В6. Версия 2. Запустите `node avro_producer.js order_v2.avsc`: реестр проверит совместимость и вернёт id 2, а `curl http://localhost:8081/subjects/orders-avro-value/versions` покажет `[1,2]`. Запустите консьюмер снова: старые события (`schema_id=1`, 22 байта) приходят без поля `currency`, новые (`schema_id=2`, 26 байт) — с ним. Один консьюмер читает оба поколения событий, потому что номер схемы у каждого события свой.

**Практика Г. Несовместимое изменение.** Создайте файл `order_v3_bad.avsc`: тот же список полей, что в `order_v2.avsc`, но у поля `currency` удалено `"default": "RUB"`. Запустите `node avro_producer.js order_v3_bad.avsc`. По документации реестра в режиме `BACKWARD` он отклонит такую схему (HTTP 409): скрипт выведет «Ошибка», а новых событий в топике не появится. Те же проверки можно сделать напрямую через `curl`. Ответы реестра ниже даны по документации и не проверялись на настоящем реестре. Сначала спросите реестр, примет ли он схему с полем `currency` без значения по умолчанию:

```bash
curl -s -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"schema": "{\"type\":\"record\",\"name\":\"OrderEvent\",\"namespace\":\"shop\",\"fields\":[{\"name\":\"order_id\",\"type\":\"int\"},{\"name\":\"status\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":\"double\"},{\"name\":\"currency\",\"type\":\"string\"}]}"}' \
  http://localhost:8081/compatibility/subjects/orders-avro-value/versions/latest
```

По документации ответ должен быть `{"is_compatible":false}`; попытка зарегистрировать такую схему (`POST /subjects/orders-avro-value/versions` с тем же телом) завершается ошибкой HTTP 409. Затем посмотрите смену режима: эта команда отключает проверку для данного subject:

```bash
curl -X PUT -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"compatibility":"NONE"}' \
  http://localhost:8081/config/orders-avro-value
```

Верните `BACKWARD` тем же способом:

```bash
curl -X PUT -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"compatibility":"BACKWARD"}' \
  http://localhost:8081/config/orders-avro-value
```

Запишите, почему в рабочей системе отключать проверку опасно.

Остановите реестр и кластер: `docker compose --profile registry down` (с опцией `-v` удаляются и данные).

### Контрольные вопросы

- Почему Avro-событие короче JSON-события с теми же данными?
- Что находится в первых пяти байтах события, записанного через Schema Registry?
- Почему добавление поля со значением по умолчанию совместимо в режиме BACKWARD, а без значения нет?
- В чём разница между схемой писателя и схемой читателя в `evolve.js`?
- Почему в этом уроке сериализатор вызывается явно, а не через настройки клиента, и чем это полезно для обучения?

### Самостоятельное задание

Опишите схему `PaymentEvent` (номер платежа, номер заказа, сумма, способ оплаты) и проведите её через топик `payments-avro` по образцу практики В. Затем сделайте два изменения схемы: одно совместимое и одно несовместимое, и докажите оба ответами реестра. Для повышенной сложности отправьте то же событие как JSON (`JSON.stringify` у продюсера, `JSON.parse` у консьюмера) и сравните размеры.
