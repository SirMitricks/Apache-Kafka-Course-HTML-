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
