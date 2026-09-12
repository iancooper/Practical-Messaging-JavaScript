/**
 * The producer. It puts things on the channel for you, including things the receiver will
 * not like. Every probe in PROBE.md starts with one of these.
 *
 *     node sender.js                one good order
 *     node sender.js slow           an order whose lookup takes 30 seconds
 *     node sender.js poison         an order for a SKU that is not in the catalogue
 *     node sender.js unmappable     a body that is not a PlaceOrder at all
 *     node sender.js burst 20       twenty good orders, as fast as we can publish them
 */
import { PlaceOrder } from './model/place-order.js';
import { DataTypeChannelProducer } from './simple-messaging/data-type-channel-producer.js';

// Valid JSON, wrong shape. The mapper cannot turn this into a PlaceOrder, and no amount
// of retrying will change that.
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

  case 'slow':
    await publish(PlaceOrder.for('GIZMO-SLOW'));
    break;

  case 'poison':
    // Well-formed. Maps perfectly. The handler will throw on it every single time.
    await publish(PlaceOrder.for('NOPE-404'));
    break;

  case 'unmappable':
    await producer.sendRaw(UNMAPPABLE_BODY);
    console.log(`Sent an unmappable body: ${UNMAPPABLE_BODY}`);
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
    console.error(`Unknown command '${command}'. Try: good, slow, poison, unmappable, burst`);
    status = 1;
}

await producer.close();
process.exit(status);
