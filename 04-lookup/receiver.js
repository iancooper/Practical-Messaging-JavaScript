/**
 * A command in on a queue, a fact out on a stream -- and now the price comes from a local copy
 * of somebody else's data rather than from a Map.
 *
 *     node receiver.js
 *     DUAL_WRITE_WINDOW=10 node receiver.js     # exercise 3's Probe A, still here
 *
 * **This is the only file that knows all three things at once**: that prices live in SQLite,
 * that the domain wants a PriceStore, and that the two fit together. Composition is the
 * application's job. model/ declares the contract and names none of the rest of it.
 */
import { DEFAULT_PATH, SqlitePriceStore } from './local-copy/sqlite-price-store.js';
import { Catalogue } from './model/catalogue.js';
import { OrderPlaced } from './model/order-placed.js';
import { PlaceOrder } from './model/place-order.js';
import { PlaceOrderHandler } from './model/place-order-handler.js';
import { PlaceOrderMapper } from './model/place-order-mapper.js';
import { EventStreamProducer } from './simple-eventing/event-stream-producer.js';
import { KafkaEventPublisher } from './simple-eventing/kafka-event-publisher.js';
import { MessagePump } from './simple-messaging/message-pump.js';

process.stdout._handle?.setBlocking?.(true);

const pid = process.pid;
console.log(`Receiver starting. PID ${pid}`);

const stopping = new AbortController();

process.on('SIGINT', () => {
  console.log('\nStopping after the current message...');
  stopping.abort();
});

// An order's events share a partition, so they stay in order relative to each other.
const stream = await EventStreamProducer.create(
  OrderPlaced, OrderPlaced.serialize, (event) => event.orderId);

const prices = SqlitePriceStore.open();

// Say how old the copy is, once, at startup. It is the only line in the system that knows --
// and watch what Probe C makes of that. Knowing at startup is not the same as noticing, and a
// receiver that prices ten thousand orders from a four-day-old copy will say this once.
const newest = await prices.newestChangedAt();
const copyAge = newest === null ? 'empty' : `newest change ${age(newest)} old`;
console.log(`Local copy is ${DEFAULT_PATH}, holding ${await prices.count()} prices -- ${copyAge}.`);

const pump = new MessagePump(
  PlaceOrder,
  new PlaceOrderMapper(),
  // PlaceOrderHandler has not changed since exercise 3, and nothing in this line asks it to.
  new PlaceOrderHandler(new Catalogue(prices), new KafkaEventPublisher(stream)));

try {
  await pump.run(stopping.signal);
} catch (e) {
  // Ctrl-C. The expected ending.
  if (e.name !== 'AbortError') throw e;
}

await stream.close();
prices.close();
console.log('Receiver stopped.');

/**
 * How old, in words, without saying "1 minutes". A copy's age is the one number that tells a
 * current local copy from a stale one, so it is worth printing in a shape a human reads.
 */
function age(isoTimestamp) {
  const seconds = (Date.now() - Date.parse(isoTimestamp)) / 1000;
  if (seconds < 60) return `${Math.round(seconds)}s`;
  if (seconds < 3600) return `${Math.round(seconds / 60)}m`;
  if (seconds < 86400) return `${Math.round(seconds / 3600)}h`;
  return `${Math.round(seconds / 86400)}d`;
}
