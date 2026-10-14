const { Kafka } = require('@confluentinc/kafka-javascript').KafkaJS;

const BOOTSTRAP = process.env.BOOTSTRAP || 'localhost:9092';
const mode = process.argv[2]; // commit или abort

async function main() {
  const producer = new Kafka().producer({
    'bootstrap.servers': BOOTSTRAP,
    'transactional.id': 'demo-tx',
  });
  await producer.connect();

  const tx = await producer.transaction();
  for (let i = 0; i < 3; i++) {
    await tx.send({ topic: 'tx-demo', messages: [{ key: 'k', value: `${mode}-${i}` }] });
  }
  if (mode === 'commit') {
    await tx.commit();
  } else {
    await tx.abort();
  }
  await producer.disconnect();
}

main().catch((err) => {
  console.error('Ошибка:', err);
  process.exit(1);
});
