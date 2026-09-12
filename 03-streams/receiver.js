/**
 * A command in on a queue, a fact out on a stream. This process is both a RabbitMQ consumer
 * and a Kafka producer, which is an extremely common shape and the reason exercise 3 exists.
 *
 *     node receiver.js
 *     DUAL_WRITE_WINDOW=10 node receiver.js     # for Probe A
 */
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

const pump = new MessagePump(
  PlaceOrder,
  new PlaceOrderMapper(),
  new PlaceOrderHandler(new Catalogue(), new KafkaEventPublisher(stream)));

try {
  await pump.run(stopping.signal);
} catch (e) {
  // Ctrl-C. The expected ending.
  if (e.name !== 'AbortError') throw e;
}

await stream.close();
console.log('Receiver stopped.');
