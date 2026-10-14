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
