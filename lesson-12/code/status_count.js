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
