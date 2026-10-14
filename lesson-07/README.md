# Урок 7. Клиенты на Node.js

Цель: написать собственные продюсер и консьюмер на JavaScript, понять роль отчёта о доставке и ручного коммита offset.

### Теория

Консольные утилиты удобны для экспериментов, но реальные системы общаются с Kafka через клиентские библиотеки. В Node.js всё строится вокруг промисов (promise — объект, который завершится позже, а результат его ждут командой `await`), поэтому код получается короче, чем с ручным циклом опроса. Ключевые идеи, которые нужно увидеть в коде:

- `producer.send()` отправляет события и возвращает промис, который завершается, когда брокер подтвердил запись. Результат — отчёт о доставке (delivery report): топик, партиция, offset. В Python эту роль играл callback.
- Ключ и значение — строки или байты (`Buffer`, контейнер для байтов в Node.js). Консьюмер всегда получает `Buffer`, поэтому его нужно превратить в строку (`.toString()`), а затем в объект (`JSON.parse`). Offset приходит строкой.
- Консьюмер не пишет цикл `poll()`: он передаёт библиотеке функцию `eachMessage`, и библиотека сама опрашивает Kafka и вызывает её для каждого события. Если функция выбросила ошибку, событие считается необработанным и придёт снова.
- Без ручного управления коммитом библиотека сама коммитит offset обработанных событий каждые 5 секунд. Мы отключим это (`enable.auto.commit: false`) и будем коммитить сами после обработки, чтобы увидеть, как момент коммита влияет на доставку.
- В Kafka записывается «следующий offset, который нужно прочитать», поэтому после обработки события с offset N коммитится N + 1.

Если нужна большая пропускная способность, не ждите каждое событие по отдельности: передавайте в один вызов `send()` массив сообщений. В учебных примерах мы ждём каждое событие намеренно, чтобы видеть отчёт о каждом.

### Практика

Работаем с любым запущенным брокером из уроков 2 или 6 (адрес берётся из `BOOTSTRAP`, см. «Подготовка»).

**Шаг 1. Топик.** Создайте топик из трёх партиций командой из дистрибутива Kafka (для кластера в Docker — через `docker exec`, как в уроке 6):

```bash
bin/kafka-topics.sh --create --topic js-orders --partitions 3 --bootstrap-server localhost:9092
```

**Шаг 2. Продюсер (файл `producer.js`).**

```javascript
const { Kafka } = require('@confluentinc/kafka-javascript').KafkaJS;

const BOOTSTRAP = process.env.BOOTSTRAP || 'localhost:9092';

async function main() {
  const producer = new Kafka().producer({ 'bootstrap.servers': BOOTSTRAP });
  await producer.connect();

  for (let orderId = 1; orderId <= 10; orderId++) {
    const event = { order_id: orderId, status: 'created', ts: Date.now() };
    // await ждёт подтверждения доставки: в ответе — отчёт о доставке
    const [report] = await producer.send({
      topic: 'js-orders',
      messages: [{ key: String(orderId % 3), value: JSON.stringify(event) }],
    });
    console.log(`Доставлено: ${report.topicName} партиция=${report.partition} offset=${report.baseOffset}`);
  }

  await producer.disconnect(); // дожидается отправки всего, что осталось в буфере
}

main().catch((err) => {
  console.error('Ошибка:', err);
  process.exit(1);
});
```

Запустите `node producer.js`. Ожидаемый результат: десять строк «Доставлено…», причём события с одинаковым ключом (0, 1, 2) идут в одну и ту же партицию, а offset в каждой партиции растёт.

**Шаг 3. Консьюмер (файл `consumer.js`).**

```javascript
const { Kafka } = require('@confluentinc/kafka-javascript').KafkaJS;

const BOOTSTRAP = process.env.BOOTSTRAP || 'localhost:9092';

async function main() {
  const consumer = new Kafka().consumer({
    'bootstrap.servers': BOOTSTRAP,
    'group.id': 'js-shop-service',
    'auto.offset.reset': 'earliest', // новая группа читает с начала
    'enable.auto.commit': false,     // коммитим вручную после обработки
  });
  await consumer.connect();
  await consumer.subscribe({ topics: ['js-orders'] });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const event = JSON.parse(message.value.toString());
      console.log(`партиция=${partition} offset=${message.offset} ` +
                  `ключ=${message.key.toString()} значение=${JSON.stringify(event)}`);
      // коммитим ПОСЛЕ обработки; в Kafka записывается «следующий offset», поэтому +1
      await consumer.commitOffsets([
        { topic, partition, offset: String(Number(message.offset) + 1) },
      ]);
    },
  });

  process.on('SIGINT', async () => {
    await consumer.disconnect();
    process.exit(0);
  });
}

main().catch((err) => {
  console.error('Ошибка:', err);
  process.exit(1);
});
```

Запустите `node consumer.js` в отдельном терминале. Вы увидите все десять событий (первый запуск может занять несколько секунд: группа проходит ребалансировку), а затем программа продолжит ждать новые. Остановите её через Ctrl-C: обработчик `SIGINT` корректно выйдет из группы. Если процесс убить без выхода из группы (например, `kill -9`), следующий запуск той же группы может до нескольких десятков секунд ждать, пока брокер поймёт, что прежний участник исчез.

**Шаг 4. Проверьте сохранённую позицию.**

```bash
bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group js-shop-service
```

Значение LAG должно быть равно нулю. Запустите `node producer.js` ещё раз, не запуская консьюмер: лаг станет 10. После перезапуска `node consumer.js` он прочитает только новые события.

**Шаг 5. Эксперимент с потерей и повтором.** Запустите `node producer.js`, затем в `consumer.js` поставьте перед вызовом `commitOffsets` строку `process.exit(1);` и запустите консьюмер: он обработает одно событие и завершится без коммита. Запустите его снова: то же событие придёт повторно. Так проявляется доставка «хотя бы один раз». Верните код в исходный вид. (Если в этом опыте перед коммитом бросить ошибку вместо `process.exit`, библиотека сама повторит обработку этого события без перезапуска.)

**Шаг 6. Группа из двух консьюмеров.** Запустите `node consumer.js` в двух терминалах и снова `node producer.js`. Убедитесь, что партиции разделились между процессами, как в уроке 5.

### Контрольные вопросы

- Что вернёт `await producer.send()` и что будет, если не дождаться этого и сразу завершить процесс?
- Почему мы отключили `enable.auto.commit`?
- Почему коммитится `offset + 1`, а не сам offset?
- Что означает `auto.offset.reset: 'earliest'` и когда он срабатывает?
- Чем отличается потеря события от его повторной доставки и как это связано с моментом коммита?

### Самостоятельное задание

Добавьте в продюсер поле `status` со случайными значениями `created`, `paid`, `shipped`, а в консьюмер — счётчик событий по статусам, который выводится при остановке (в обработчике `SIGINT`). Проверьте, что сумма равна числу отправленных событий.
