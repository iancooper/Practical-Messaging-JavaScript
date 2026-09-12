/**
 * The producer half of the Messaging Gateway: the only class here that knows this is RabbitMQ.
 *
 * Under RMQ, to send, we:
 *     1. open a socket connection to the broker
 *     2. create a channel (a lightweight logical connection) on that socket
 *     3. declare a direct exchange to publish to
 *
 * We do not declare the queue. The consumer does that, and binds it to our routing key.
 * That is the asymmetry AMQP has and a queue API does not: we publish to an exchange and
 * have no idea who, if anyone, is listening.
 *
 * This class is given to you and it is correct. Read it for the AMQP vocabulary; the
 * exercise is not here.
 */
import amqp from 'amqplib';

import { BROKER_URL, EXCHANGE_NAME, routingKeyFor } from './channel.js';

export class DataTypeChannelProducer {
  #serializer;
  #connection;
  #channel;
  #routingKey;

  constructor(serializer, connection, channel, routingKey) {
    this.#serializer = serializer;
    this.#connection = connection;
    this.#channel = channel;
    this.#routingKey = routingKey;
  }

  /**
   * Connecting is I/O, and a constructor cannot await, so construction is a static method.
   *
   * @param {Function} messageClass the type on this channel; the routing key is derived from it
   * @param {(message: object) => string} serializer turns a message into the string we put in the body
   * @param {string} brokerUrl where the broker is
   */
  static async create(messageClass, serializer, brokerUrl = BROKER_URL) {
    // The URL carries the defaults: user guest, password guest, port 5672, virtual host /
    const connection = await amqp.connect(brokerUrl);

    // A *confirm* channel, so that publishing can be awaited. On a plain channel, publish()
    // returns once the bytes are in this process's socket buffer, which tells you nothing
    // about whether the broker has them -- and the sender in these exercises publishes and
    // exits immediately.
    const channel = await connection.createConfirmChannel();

    // Durable, so the exchange survives a broker restart.
    await channel.assertExchange(EXCHANGE_NAME, 'direct', { durable: true });

    return new DataTypeChannelProducer(
      serializer, connection, channel, routingKeyFor(messageClass));
  }

  /**
   * Send a message. The routing key is derived from the type, so sender and receiver
   * match up without either knowing about the other.
   */
  async send(message) {
    await this.sendRaw(this.#serializer(message));
  }

  /** Send a body we did not serialize -- used to put something unmappable on the channel. */
  async sendRaw(body) {
    // Persistent: the broker writes it to its message store, so it survives a broker restart.
    // This is the producer-side half of guaranteed delivery, and it is the cheap half.
    this.#channel.publish(
      EXCHANGE_NAME, this.#routingKey, Buffer.from(body, 'utf8'), { persistent: true });

    // Wait for the broker to acknowledge it, so that returning from send() means the broker
    // has the message rather than that we have written it to a socket.
    await this.#channel.waitForConfirms();
  }

  async close() {
    await this.#channel.close();
    await this.#connection.close();
  }
}
