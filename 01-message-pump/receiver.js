/**
 * The consumer. Runs a Message Pump until you stop it.
 *
 *     node receiver.js
 *
 * Ctrl-C stops it *between* messages, which is the polite ending.
 * Several probes want the rude ending instead -- a process that dies with a message in its
 * hands. Use the PID printed below, from another terminal:
 *
 *     kill -9 <pid>
 */
import { Catalogue } from './model/catalogue.js';
import { PlaceOrder } from './model/place-order.js';
import { PlaceOrderHandler } from './model/place-order-handler.js';
import { MessagePump } from './simple-messaging/message-pump.js';

// Write every line out before carrying on. Node's stdout is asynchronous when it is a pipe
// on macOS, so several thousand lines can still be in a buffer when a probe ends in a
// 'kill -9' -- and a line you never see is a probe you cannot read.
process.stdout._handle?.setBlocking?.(true);

const pid = process.pid;
console.log(`Receiver starting. PID ${pid}`);
console.log(`Ctrl-C to stop between messages; 'kill -9 ${pid}' to stop mid-message.`);
console.log();

const stopping = new AbortController();

// SIGINT, not the 'exit' event: an exit handler runs on every ending, including a crash,
// and would print a polite goodbye underneath the stack trace that had just killed us.
process.on('SIGINT', () => {
  // Do not tear the process down; let the pump notice.
  console.log('\nStopping after the current message...');
  stopping.abort();
});

const pump = new MessagePump(PlaceOrder, new PlaceOrderHandler(new Catalogue()));

try {
  await pump.run(stopping.signal);
} catch (e) {
  // Ctrl-C during the poll delay. The expected ending.
  if (e.name !== 'AbortError') throw e;
}

console.log('Receiver stopped.');
