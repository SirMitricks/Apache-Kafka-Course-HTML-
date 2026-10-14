# Урок 10. Хранение данных: retention, сегменты, compaction

**Цель:** понимать, как Kafka хранит события на диске, настраивать срок и объём хранения и использовать сжатие журнала по ключу.

### Теория

Каждая партиция на диске — это набор **сегментов**: файлов, в конец последнего из которых дописываются новые события. Когда сегмент достигает заданного размера (`segment.bytes`) или возраста (`segment.ms`), он закрывается и открывается следующий. Закрытые сегменты можно удалять или сжимать, а активный (последний) сегмент не трогают.

За жизнь события отвечает политика очистки `cleanup.policy`:

| Политика | Что происходит | Для чего |
| --- | --- | --- |
| `delete` (по умолчанию) | Сегменты старше `retention.ms` или сверх `retention.bytes` удаляются | Журналы, потоки событий |
| `compact` | Для каждого ключа сохраняется последнее значение | «Текущее состояние»: профили, настройки, остатки |
| `compact,delete` | Сжатие плюс удаление по сроку | Состояние с ограничением по времени |

По умолчанию брокер хранит события 7 дней (параметр уровня брокера `log.retention.hours`); топик может переопределить срок через `retention.ms`. Событие с ключом и пустым значением (**tombstone**, «надгробие») при сжатии означает удаление ключа.

Важно: очистка выполняется фоновым потоком (отдельной задачей внутри брокера) по расписанию, поэтому события удаляются не точно в момент истечения срока.

### Практика

Запущен любой брокер из урока 2 или 6.

**Шаг 1. Посмотрите и измените настройки топика**

```bash
bin/kafka-configs.sh --bootstrap-server localhost:9092 --entity-type topics --entity-name orders --describe
bin/kafka-configs.sh --bootstrap-server localhost:9092 --entity-type topics --entity-name orders \
  --alter --add-config retention.ms=86400000
bin/kafka-configs.sh --bootstrap-server localhost:9092 --entity-type topics --entity-name orders --describe
```

Во втором выводе должна появиться строка `retention.ms=86400000` (сутки). Чтобы вернуть значение по умолчанию:

```bash
bin/kafka-configs.sh --bootstrap-server localhost:9092 --entity-type topics --entity-name orders \
  --alter --delete-config retention.ms
```

**Шаг 2. Загляните в сегменты**

Определите каталог данных (`log.dirs` из урока 2, по умолчанию `/tmp/kraft-combined-logs`) и выведите содержимое партиции:

```bash
ls -lh /tmp/kraft-combined-logs/orders-0/
bin/kafka-dump-log.sh --files /tmp/kraft-combined-logs/orders-0/00000000000000000000.log --print-data-log | head -n 20
```

Найдите в выводе offset, ключ и значение событий. Для кластера из урока 6 путь будет `/tmp/kraft-cluster/node1/...`.

**Шаг 3. Удаление по сроку**

Для наглядности ускорим проверку: добавьте в файл конфигурации брокера строку `log.retention.check.interval.ms=10000` (по умолчанию проверка идёт раз в 5 минут) и перезапустите брокер. Затем:

```bash
bin/kafka-topics.sh --create --topic short-lived --partitions 1 \
  --config segment.ms=5000 --config retention.ms=15000 --bootstrap-server localhost:9092
# запишите несколько событий, подождите ~10 секунд, запишите ещё несколько
bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic short-lived --time earliest
bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic short-lived --time latest
```

Первая команда показывает самый ранний доступный offset, вторая — следующий за последним. Повторяйте первую команду раз в минуту: когда старые сегменты будут удалены, ранний offset вырастет. Прочитайте топик с `--from-beginning` и убедитесь, что старых событий нет.

**Шаг 4. Сжатие журнала по ключу**

```bash
bin/kafka-topics.sh --create --topic user-settings --partitions 1 \
  --config cleanup.policy=compact --config segment.ms=5000 \
  --config min.cleanable.dirty.ratio=0.01 --config delete.retention.ms=100 --bootstrap-server localhost:9092

bin/kafka-console-producer.sh --topic user-settings --bootstrap-server localhost:9092 \
  --reader-property parse.key=true --reader-property key.separator=: \
  --reader-property null.marker=NULL
> alice:theme=dark
> bob:theme=light
> alice:theme=blue
> alice:lang=ru
> bob:NULL
```

Подождите около минуты, допишите ещё несколько событий с другими ключами (сжатие затрагивает только закрытые сегменты), затем прочитайте топик:

```bash
bin/kafka-console-consumer.sh --topic user-settings --from-beginning \
  --formatter-property print.key=true --bootstrap-server localhost:9092
```

В старых сегментах для `alice` должно остаться только последнее значение, а запись `bob` с пустым значением со временем исчезнет полностью. Результат зависит от работы фонового потока: если сжатие ещё не произошло, повторите чтение через минуту.

### Контрольные вопросы

- Что такое сегмент и почему активный сегмент не удаляется?
- Чем `retention.ms` отличается от `retention.bytes`?
- Для каких задач подходит `cleanup.policy=compact`, а для каких нет?
- Что такое tombstone и как он используется?
- Почему событие может остаться на диске дольше, чем указано в `retention.ms`?

### Самостоятельное задание

Создайте топик `prices` с политикой `compact` и промоделируйте обновление цен на 5 товаров по 10 раз. Опишите, сколько записей вы ожидаете после сжатия, и сравните с результатом.
