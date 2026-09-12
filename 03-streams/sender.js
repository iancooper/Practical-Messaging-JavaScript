/**
 * Puts things on channels, including things the other end will not like.
 *
 *     node sender.js                a good order, on the queue
 *     node sender.js burst 20       twenty good orders
 *     node sender.js poison         a SKU that is not in the catalogue (queue-side failure)
 *     node sender.js unmappable     a body that is not a PlaceOrder (queue-side failure)
 *     node sender.js flaky          a lookup that fails twice then works
 *     node sender.js bad-event      A RECORD THE STREAM CONSUMER CANNOT READ -- straight onto Kafka
 */
import { OrderPlaced } from './model/order-placed.js';
import { PlaceOrder } from './model/place-order.js';
import { EventStreamProducer } from './simple-eventing/event-stream-producer.js';
import { DataTypeChannelProducer } from './simple-messaging/data-type-channel-producer.js';

const UNMAPPABLE_BODY = '{"Id":"c0ffee","ProductCode":"WIDGET-1","Qty":1}';
const BAD_EVENT = '{"Id":"c0ffee","OrderRef":"not-a-field","Sku":"WIDGET-1"}';

const [command = 'good', ...rest] = process.argv.slice(2);

if (command.toLowerCase() === 'bad-event') {
  // Appended to the stream directly, because we need a poison *record* rather than a poison
  // message. There is no such thing as putting it on an invalid record topic for us.
  const stream = await EventStreamProducer.create(
    OrderPlaced, OrderPlaced.serialize, (event) => event.orderId);
  console.log('Appending a record the consumer cannot map:');
  console.log(`  ${BAD_EVENT}`);
  await stream.sendRaw('poison', BAD_EVENT);
  await stream.close();
  process.exit(0);
}

const producer = await DataTypeChannelProducer.create(PlaceOrder, PlaceOrder.serialize);

async function publish(order) {
  await producer.send(order);
  console.log(`Sent order ${order.id}: ${order.quantity} x ${order.sku}`);
}

let status = 0;

switch (command.toLowerCase()) {
  case 'good':
    await publish(PlaceOrder.for('WIDGET-1'));
    break;

  case 'flaky':
    await publish(PlaceOrder.for('FLAKY-1'));
    break;

  case 'poison':
    await publish(PlaceOrder.for('NOPE-404'));
    break;

  case 'unmappable':
    await producer.sendRaw(UNMAPPABLE_BODY);
    console.log(`Sent an unmappable body: ${UNMAPPABLE_BODY}`);
    break;

  case 'slow':
    await publish(PlaceOrder.for('GIZMO-SLOW'));
    break;

  case 'burst': {
    const count = Number.parseInt(rest[0], 10) || 20;
    for (let i = 0; i < count; i++) {
      await publish(PlaceOrder.for('WIDGET-1', i + 1));
    }
    console.log(`Sent ${count} orders`);
    break;
  }

  default:
    console.error(
      `Unknown command '${command}'. Try: good, flaky, poison, unmappable, slow, burst, bad-event`);
    status = 1;
}

await producer.close();
process.exit(status);
