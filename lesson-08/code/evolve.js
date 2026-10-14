const avro = require('avsc');

const fields = [
  { name: 'order_id', type: 'int' },
  { name: 'status', type: 'string' },
  { name: 'amount', type: 'double' },
];
const record = (extra) => avro.Type.forSchema({
  type: 'record', name: 'OrderEvent', namespace: 'shop', fields: [...fields, ...extra],
});

const v1 = record([]);
const v2 = record([{ name: 'currency', type: 'string', default: 'RUB' }]);
const v3Bad = record([{ name: 'currency', type: 'string' }]); // нет default

const data = v1.toBuffer({ order_id: 1, status: 'created', amount: 10.5 });

// резолвер = «как прочитать данные, записанные схемой v1, схемой читателя»
console.log('читаем старые данные схемой v2:',
  v2.fromBuffer(data, v2.createResolver(v1)));
try {
  v3Bad.createResolver(v1);
} catch (e) {
  console.log('ошибка v3 (поле без default):', e.message);
}
