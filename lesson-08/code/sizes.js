const avro = require('avsc');

const type = avro.Type.forSchema({
  type: 'record',
  name: 'OrderEvent',
  namespace: 'shop',
  fields: [
    { name: 'order_id', type: 'int' },
    { name: 'status', type: 'string' },
    { name: 'amount', type: 'double' },
  ],
});

const event = { order_id: 123456, status: 'created', amount: 1999.9 };

const jsonBytes = Buffer.from(JSON.stringify(event), 'utf8');
const avroBytes = type.toBuffer(event);

console.log('JSON :', jsonBytes.length, 'байт ->', jsonBytes.toString());
console.log('Avro :', avroBytes.length, 'байт ->', avroBytes);
