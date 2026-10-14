# Источники и статус проверки

### Источники

- Пакет `@confluentinc/kafka-javascript` (версия 1.11.0, на основе librdkafka 2.16.0, интерфейс `.KafkaJS`, нужен Node.js 18+): https://www.npmjs.com/package/@confluentinc/kafka-javascript
- Пакет `@confluentinc/schemaregistry` (1.11.0): https://www.npmjs.com/package/@confluentinc/schemaregistry
- Библиотека `avsc` (5.7.9) для Avro: https://www.npmjs.com/package/avsc
- Документация Apache Kafka: https://kafka.apache.org/documentation/
- Документация и список настроек librdkafka (параметры вроде `enable.idempotence`, `transactional.id` передаются клиенту как есть): https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
- Остальные источники и глоссарий — в конце документа (раздел «Глоссарий и источники»).

Почему не KafkaJS: его последний релиз (2.2.4) вышел в феврале 2023 года. Интерфейс `.KafkaJS` в пакете Confluent создан похожим, поэтому старый код часто легко перенести.

### Статус проверки

Проверено: код уроков 7, 8 (практики А, Б — `sizes.js` и `evolve.js` работают без Kafka; В — на имитации), 9 (шаг 5) и 12 был запущен на тестовом кластере, встроенном в librdkafka (три брокера в одном процессе), и на минимальной имитации реестра схем; ожидаемые выводы в тексте взяты из этих прогонов.

Не проверялось:

- работа на настоящем брокере Kafka и в Docker Compose;
- настоящий Schema Registry (ответы на несовместимую схему, HTTP 409 и эндпойнты REST — по документации);
- поведение `read_committed` и транзакции на реальном кластере;
- Kafka Connect и демо Kafka Streams;
- порядок строк в выводе и точные тексты ошибок на ваших версиях пакетов могут отличаться.

Если что-то расходится с описанным — сверяйтесь с документацией вашей версии и сообщайте преподавателю.
