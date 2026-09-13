/**
 * Stands in for the catalogue service: it owns prices, and it tells the world when one changes.
 *
 *     node price-seeder.js seed                 a starting price for every SKU
 *     node price-seeder.js set WIDGET-1 11.99   change one, on demand
 *
 * It publishes and exits. It keeps no state, because the stream is the state -- which is the
 * whole argument for ECST, and the reason a new price consumer with an empty copy can catch up
 * by reading the log from the beginning.
 */
import { PriceChanged } from './model/price-changed.js';
import { EventStreamProducer } from './simple-eventing/event-stream-producer.js';

// The two SKUs that survived exercise 4. GIZMO-SLOW and FLAKY-1 are gone: they were failures of
// an on-demand lookup, and there is no longer a lookup to fail. See model/catalogue.js.
const STARTING_PRICES = [
  ['WIDGET-1', 9.99],
  ['GIZMO-2', 24.50],
];

const [command = 'seed', ...rest] = process.argv.slice(2);

const producer = await EventStreamProducer.create(
  PriceChanged,
  PriceChanged.serialize,
  // Keyed by SKU, so two changes to one price stay in order. Key it by the event's own id
  // instead and they land on different partitions, and yesterday's price can be applied on
  // top of today's -- which is a bug you will not see until the day it costs money.
  (event) => event.sku);

async function publish(event) {
  await producer.send(event);
  console.log(
    `Published ${event.sku} = ${event.price.toFixed(2)} at ${event.changedAt.slice(11, 23)}`);
}

let status = 0;

switch (command.toLowerCase()) {
  case 'seed':
    for (const [sku, price] of STARTING_PRICES) await publish(PriceChanged.for(sku, price));
    console.log(`Seeded ${STARTING_PRICES.length} prices.`);
    break;

  case 'set': {
    const [sku, amount] = rest;
    const price = Number(amount);
    if (!sku || !amount || !Number.isFinite(price)) {
      console.error('Usage: set <SKU> <PRICE>   e.g. set WIDGET-1 11.99');
      status = 1;
      break;
    }
    await publish(PriceChanged.for(sku, price));
    break;
  }

  default:
    console.error(`Unknown command '${command}'. Try: seed, set`);
    status = 1;
}

await producer.close();
process.exit(status);
