/**
 * The producer, and the source of every failure you are asked to survive.
 *
 *     node sender.js                a good order
 *     node sender.js flaky          TRANSIENT: the lookup fails twice, then works
 *     node sender.js poison         PERMANENT: a SKU that is not in the catalogue, ever
 *     node sender.js unmappable     INVALID:   a body that is not a PlaceOrder at all
 *     node sender.js slow           an order whose lookup takes 30 seconds
 *     node sender.js burst 20       twenty good orders
 *
 * Three of those are three different failures. They should not all end up in the same place.
 */
import { PlaceOrder } from './model/place-order.js';
import { DataTypeChannelProducer } from './simple-messaging/data-type-channel-producer.js';

// Valid JSON, wrong shape. No number of retries will make this a PlaceOrder.
const UNMAPPABLE_BODY = '{"Id":"c0ffee","ProductCode":"WIDGET-1","Qty":1}';

const [command = 'good', ...rest] = process.argv.slice(2);

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
    // The order is fine. The catalogue is having a bad minute and will recover.
    await publish(PlaceOrder.for('FLAKY-1'));
    break;

  case 'poison':
    // Well-formed, maps perfectly, and the handler will throw on it every single time.
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
    console.error(`Unknown command '${command}'. Try: good, flaky, poison, unmappable, slow, burst`);
    status = 1;
}

await producer.close();
process.exit(status);
