/**
 * Follows streams.PriceChanged and maintains the local copy of the catalogue's prices.
 *
 *     node price-consumer.js
 *     PRICE_WRITE_WINDOW=15 node price-consumer.js     # for Probe D
 *
 * **It is a process of its own, and that is deliberate.** A thread inside the receiver would
 * have been less code; it would also have made Probe B "set a flag" instead of "kill -9 a real
 * process", and the whole of exercise 4 is about what happens to the people downstream of a
 * thing that stops.
 */
import { setTimeout as delay } from 'node:timers/promises';

import { DEFAULT_PATH, SqlitePriceStore } from './local-copy/sqlite-price-store.js';
import { PriceChanged } from './model/price-changed.js';
import { EventStreamReader } from './simple-eventing/event-stream-reader.js';
import { PRICE_CONSUMER_GROUP } from './simple-eventing/stream.js';

process.stdout._handle?.setBlocking?.(true);

console.log(`PriceConsumer starting. PID ${process.pid}`);

// How long to pause *between* the two writes below, so that you can aim a kill at the gap.
// In a real service the gap is microseconds wide. It is still a gap. This is the same
// instrument as DUAL_WRITE_WINDOW in exercise 3, one layer down and in your own code.
const WINDOW_SECONDS = Number.parseInt(process.env.PRICE_WRITE_WINDOW ?? '', 10) || 0;

const stopping = new AbortController();

process.on('SIGINT', () => {
  console.log('\nStopping...');
  stopping.abort();
});

const store = SqlitePriceStore.open();
const reader = await EventStreamReader.create(
  PriceChanged, PriceChanged.deserialize, PRICE_CONSUMER_GROUP);

console.log(`Following ${reader.topic} as group '${reader.consumerGroup}'`);

const newest = await store.newestChangedAt();
console.log(
  `Local copy is ${DEFAULT_PATH}, holding ${await store.count()} prices` +
  (newest === null ? ' -- empty.' : ` -- newest change ${age(newest)} old.`));

if (WINDOW_SECONDS > 0) {
  console.log(
    `PRICE_WRITE_WINDOW is ${WINDOW_SECONDS}s -- there is a gap between the two writes.`);
}

try {
  while (!stopping.signal.aborted) {
    const record = await reader.read(stopping.signal);
    if (record === null) continue;

    const event = record.message;

    // ---------------------------------------------------------------------------------
    //  TWO WRITES, TWO STORES, NO TRANSACTION. **PROBE D IS THE ORDER OF THESE LINES.**
    //
    //  The price goes into SQLite. The offset goes into Kafka. Nothing on this machine
    //  can make those two happen together, which is exactly what exercise 3 showed you
    //  in the receiver -- except that this time it is a loop you wrote, and it looks
    //  like one step.
    //
    //  As written: apply, then commit. Die in between and the record is read again on
    //  restart and applied twice, which is harmless *because PriceChanged is a snapshot*.
    //  Swap the two lines and die in between and the price is lost for ever, because
    //  Kafka will never offer it again.
    // ---------------------------------------------------------------------------------
    const appliedAt = await store.apply(event);

    const staleness = Date.parse(appliedAt) - Date.parse(event.changedAt);
    console.log(
      `  ${event.sku} = ${Number(event.price).toFixed(2)}  (${record.where})  ` +
      `published-to-applied ${staleness} ms`);

    await pauseInTheWindow();

    reader.commit(record);
  }
} catch (e) {
  // Ctrl-C during one of the delays. The expected ending.
  if (e.name !== 'AbortError') throw e;
}

await reader.close();
store.close();
console.log('PriceConsumer stopped.');

async function pauseInTheWindow() {
  if (WINDOW_SECONDS <= 0) return;

  console.log('  [one of the two writes has happened and the other has not.]');
  console.log(`  [you have ${WINDOW_SECONDS} seconds. kill -9 ${process.pid}]`);
  await delay(WINDOW_SECONDS * 1000, undefined, { signal: stopping.signal });
}

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
