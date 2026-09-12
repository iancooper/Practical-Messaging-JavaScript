/**
 * Appends records to a Kafka topic. The producer half of the eventing gateway.
 *
 * Note what is *not* here, compared with the RabbitMQ producer: no exchange, no binding, no
 * routing key. You append to a log, and the key decides which partition the record lands in.
 * Same key, same partition, so records that must stay in order must share a key.
 */
import Confluent from '@confluentinc/kafka-javascript';

import { BOOTSTRAP_SERVERS, ensureTopicExists, topicFor } from './stream.js';

const { Producer } = Confluent;

export class EventStreamProducer {
  #topic;
  #serializer;
  #partitionKey;
  #producer;

  // One entry per record we have produced and not yet heard back about, keyed by the token
  // we handed librdkafka. The delivery report brings the token back, which is how an
  // event-based client is turned into something you can await.
  #pending = new Map();
  #nextToken = 0;

  constructor(topic, serializer, partitionKey, producer) {
    this.#topic = topic;
    this.#serializer = serializer;
    this.#partitionKey = partitionKey;
    this.#producer = producer;
  }

  /**
   * @param {Function} messageClass the event type; the topic is named after it
   * @param {(event: object) => string} serializer turns an event into the record's value
   * @param {(event: object) => string} partitionKey
   *   Which partition this record belongs in -- and therefore what it is ordered with respect
   *   to. Records sharing a key share a partition and stay in order; records with different
   *   keys have no order between them at all.
   *
   *   **This is a design decision and there is no safe default**, which is why you have to
   *   pass it. Key an order's events by the order and they arrive in sequence. Key them by
   *   the event's own id and every event is independent -- which is fine right up until two
   *   events about the same thing are processed out of order by different consumers.
   */
  static async create(messageClass, serializer, partitionKey, bootstrapServers = BOOTSTRAP_SERVERS) {
    const topic = topicFor(messageClass);
    await ensureTopicExists(topic, bootstrapServers);

    const producer = new Producer({
      'bootstrap.servers': bootstrapServers,
      // Wait for the leader and all in-sync replicas before calling a write done.
      // This is the producer-side half of guaranteed delivery, and it is the cheap half --
      // exactly as it was on RabbitMQ, where it was one 'persistent' flag.
      'acks': -1,
      'enable.idempotence': true,
      // Ask for a delivery report per record. Without this there is nothing to wait for.
      'dr_cb': true,
    });

    await new Promise((resolve, reject) => {
      producer.once('ready', resolve);
      producer.once('event.error', reject);
      producer.connect();
    });

    // How often the client checks for delivery reports. It is a background poll, so this is
    // the granularity of "the broker has answered", not a delay we add to each send.
    producer.setPollInterval(100);

    const created = new EventStreamProducer(topic, serializer, partitionKey, producer);
    producer.on('delivery-report', (err, report) => created.#settle(err, report));
    return created;
  }

  #settle(err, report) {
    const waiting = this.#pending.get(report.opaque);
    if (!waiting) return;
    this.#pending.delete(report.opaque);
    if (err) waiting.reject(err);
    else waiting.resolve(report);
  }

  /**
   * Append a record, and wait until the broker has acknowledged it.
   *
   * We wait for the delivery report rather than fire-and-forget: produce() alone returns
   * before the record is durable, and then "I produced the event" would be a claim about a
   * buffer in this process rather than about anything the broker has.
   */
  async send(event) {
    await this.sendRaw(this.#partitionKey(event), this.#serializer(event));
  }

  /** Append a record we did not serialize -- used to put something unreadable on the stream. */
  async sendRaw(key, body) {
    const token = ++this.#nextToken;
    const acknowledged = new Promise((resolve, reject) => {
      this.#pending.set(token, { resolve, reject });
    });

    this.#producer.produce(
      this.#topic, null, Buffer.from(body, 'utf8'), Buffer.from(key, 'utf8'), Date.now(), token);

    const report = await acknowledged;
    console.log(`  -> ${report.topic} partition ${report.partition} offset ${report.offset}`);
  }

  async close() {
    await new Promise((resolve) => this.#producer.flush(5000, resolve));
    await new Promise((resolve) => this.#producer.disconnect(resolve));
  }
}
