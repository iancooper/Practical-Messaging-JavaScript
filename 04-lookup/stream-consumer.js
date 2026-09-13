/**
 * Follows the OrderPlaced stream and counts what it sees.
 *
 *     node stream-consumer.js
 *
 * The count is the point. An order placed once should appear once.
 */
import { OrderPlaced } from './model/order-placed.js';
import { EventStreamConsumer } from './simple-eventing/event-stream-consumer.js';

process.stdout._handle?.setBlocking?.(true);

console.log(`StreamConsumer starting. PID ${process.pid}`);

const stopping = new AbortController();
process.on('SIGINT', () => stopping.abort());

// How many times have we seen an event for each order? Anything above one is a duplicate,
// and duplicates are what exercise 3 is about.
const seen = new Map();

const consumer = await EventStreamConsumer.create(
  OrderPlaced,
  OrderPlaced.deserialize,
  (event) => {
    const count = (seen.get(event.orderId) ?? 0) + 1;
    seen.set(event.orderId, count);
    const flag = count > 1 ? `  <-- DUPLICATE, seen ${count} times` : '';

    console.log(
      `  order ${event.orderId}: ${event.quantity} x ${event.sku} ` +
      `for ${Number(event.total).toFixed(2)}${flag}`);
  });

await consumer.run(stopping.signal);
await consumer.close();

const total = [...seen.values()].reduce((a, b) => a + b, 0);
console.log();
console.log(`Distinct orders seen: ${seen.size}. Events read: ${total}.`);
for (const [orderId, count] of seen) {
  if (count > 1) console.log(`  ${orderId} arrived ${count} times`);
}
