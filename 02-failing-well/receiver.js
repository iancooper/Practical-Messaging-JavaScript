/**
 * The consumer. Runs a Message Pump until you stop it.
 *
 *     node receiver.js
 *
 * Ctrl-C stops it between messages. 'kill -9 <pid>' stops it mid-message.
 */
import { Catalogue } from './model/catalogue.js';
import { PlaceOrder } from './model/place-order.js';
import { PlaceOrderHandler } from './model/place-order-handler.js';
import { PlaceOrderMapper } from './model/place-order-mapper.js';
import { MessagePump } from './simple-messaging/message-pump.js';

// Node's stdout is asynchronous when it is a pipe on macOS, and this exercise can print
// thousands of lines in a few seconds. Write them out as we go, or a probe that ends in a
// 'kill -9' loses the tail of its own evidence.
process.stdout._handle?.setBlocking?.(true);

const pid = process.pid;
console.log(`Receiver starting. PID ${pid}`);
console.log();

const stopping = new AbortController();

// SIGINT, not the 'exit' event: an exit handler runs on every ending, including a crash.
process.on('SIGINT', () => {
  console.log('\nStopping after the current message...');
  stopping.abort();
});

const pump = new MessagePump(
  PlaceOrder,
  new PlaceOrderMapper(),
  new PlaceOrderHandler(new Catalogue()));

try {
  await pump.run(stopping.signal);
} catch (e) {
  // Ctrl-C. The expected ending.
  if (e.name !== 'AbortError') throw e;
}

console.log('Receiver stopped.');
