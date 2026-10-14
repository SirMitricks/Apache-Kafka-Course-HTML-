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

  // Регистрируем схему под именем <топик>-value. Если она несовместима с прошлыми
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
