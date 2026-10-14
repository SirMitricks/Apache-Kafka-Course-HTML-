# Урок 11. Kafka Connect

**Цель:** настроить непрерывный перенос данных из файла в Kafka и из Kafka в другой файл с помощью коннекторов и управлять ими через REST API (управление программой обычными веб-запросами по HTTP).

### Теория

[Kafka Connect](https://kafka.apache.org/documentation/#connect) — инструмент для постоянного импорта данных из внешних систем в Kafka и экспорта обратно, без написания собственного кода. Он запускает **коннекторы** — плагины (подключаемые модули) с логикой работы с конкретной системой. Готовых коннекторов сотни: для баз данных, файловых хранилищ, облачных сервисов.

| Понятие | Что это |
| --- | --- |
| Worker | Процесс Connect, в котором выполняются коннекторы |
| Source connector | Читает данные из внешней системы и пишет в топик Kafka |
| Sink connector | Читает топик Kafka и пишет данные во внешнюю систему |
| Task | Единица параллельной работы коннектора |
| Converter | Преобразует данные в формат, хранимый в Kafka (JSON, Avro и другие) |
| Standalone / distributed | Один процесс для разработки или группа процессов с общей конфигурацией для продакшена |

В этом уроке мы запустим режим standalone с двумя учебными файловыми коннекторами, которые идут в составе Kafka.

### Практика

Брокер должен быть запущен и доступен на `localhost:9092`. Работаем из каталога дистрибутива.

**Шаг 1. Подключите плагин файловых коннекторов**

```bash
echo "plugin.path=libs/connect-file-4.3.1.jar" >> config/connect-standalone.properties
```

**Шаг 2. Подготовьте исходный файл**

```bash
echo -e "foo\nbar" > test.txt
```

На Windows: `echo foo > test.txt` и `echo bar >> test.txt`.

**Шаг 3. Запустите Connect с двумя коннекторами**

```bash
bin/connect-standalone.sh config/connect-standalone.properties \
  config/connect-file-source.properties config/connect-file-sink.properties
```

Первый файл конфигурации описывает сам процесс Connect (адрес брокера, формат данных), остальные — по коннектору. Source читает строки из `test.txt` и пишет в топик `connect-test`, sink читает этот топик и пишет в `test.sink.txt`.

**Шаг 4. Проверьте результат**

```bash
more test.sink.txt
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic connect-test --from-beginning
```

В файле должны быть строки `foo` и `bar`. В топике они лежат в виде JSON (текстового формата данных с полями «имя: значение») с полями `schema` и `payload`: это работа конвертера.

**Шаг 5. Данные идут непрерывно**

```bash
echo "Another line" >> test.txt
```

Новая строка должна появиться и в консьюмере, и в `test.sink.txt`.

**Шаг 6. Управление через REST API**

У Connect есть HTTP-интерфейс, по умолчанию на порту 8083. В новом терминале:

```bash
curl -s localhost:8083/connectors
curl -s localhost:8083/connectors/local-file-source/status
curl -s localhost:8083/connector-plugins
```

Первый запрос вернёт список из двух коннекторов; второй покажет состояние `RUNNING`; третий — доступные плагины. Затем приостановите источник и убедитесь, что новые строки перестали попадать в топик:

```bash
curl -s -X PUT localhost:8083/connectors/local-file-source/pause
echo "while paused" >> test.txt
curl -s -X PUT localhost:8083/connectors/local-file-source/resume
```

После `resume` строка `while paused` должна дойти до топика и файла-приёмника.

**Шаг 7.** **Добавьте второй приёмник**

Остановите Connect (`Ctrl-C`), скопируйте `config/connect-file-sink.properties` в `my-sink.properties` и измените в копии `name` (например, `second-file-sink`) и `file` (например, `test.sink2.txt`). Затем запустите Connect с тремя файлами конфигурации:

```bash
bin/connect-standalone.sh config/connect-standalone.properties \
  config/connect-file-source.properties config/connect-file-sink.properties my-sink.properties
```

Добавьте строку в `test.txt` и убедитесь, что оба файла-приёмника получают одинаковые данные. Так проявляется свойство Kafka: один топик могут независимо читать многие потребители.

### Контрольные вопросы

- Чем source-коннектор отличается от sink-коннектора?
- Зачем нужен конвертер и откуда в сообщениях в топике взялись `schema` и `payload`?
- В чём разница между режимами standalone и distributed?
- Почему файловые коннекторы подходят только для обучения, а не для продакшена?

### Самостоятельное задание

Найдите на странице [Kafka Ecosystem](https://cwiki.apache.org/confluence/display/KAFKA/Ecosystem) или в документации Connect по одному коннектору для базы данных, объектного хранилища и очереди сообщений. Для каждого запишите: название, направление (source или sink) и один сценарий использования.
