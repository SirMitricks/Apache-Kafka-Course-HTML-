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
