/**
 * The consumer half of the Messaging Gateway. Again, the only class that knows this is RabbitMQ.
 *
 * Under RMQ, to receive, we:
 *     1. open a socket connection to the broker
 *     2. create a channel on that socket
 *     3. declare the same direct exchange the producer publishes to
 *     4. declare a queue to hold our messages
 *     5. bind the queue to the routing key on that exchange
 *
 * Both ends declare the exchange, so it does not matter which starts first. Only we declare
 * the queue -- which does mean that anything published before the first run of the consumer
 * went nowhere. Start the consumer once before you send anything.
 *
 * This is a Polling Consumer: receive() asks the broker whether there is anything there. It
 * costs us a process sitting in a loop while idle, and buys us not having to hold a
 * subscription open for the broker to push work down.
 *
 * This class is given to you and it is correct. Read it for the AMQP vocabulary; the
 * exercise is not here.
 */
import amqp from 'amqplib';

import { BROKER_URL, EXCHANGE_NAME, queueNameFor, routingKeyFor } from './channel.js';

export class DataTypeChannelConsumer {
  #connection;
  #channel;
  #queueName;

  constructor(connection, channel, queueName) {
    this.#connection = connection;
    this.#channel = channel;
    this.#queueName = queueName;
  }

  /** @param {Function} messageClass the type on this channel; the queue is named after it */
  static async create(messageClass, brokerUrl = BROKER_URL) {
    const connection = await amqp.connect(brokerUrl);
    const channel = await connection.createChannel();

    const routingKey = routingKeyFor(messageClass);
    const queueName = queueNameFor(messageClass);

    await channel.assertExchange(EXCHANGE_NAME, 'direct', { durable: true });

    // Durable queue to go with the persistent messages: no point writing a message to disk
    // and then keeping it in a queue that evaporates on restart.
    await channel.assertQueue(queueName, { durable: true, exclusive: false, autoDelete: false });

    await channel.bindQueue(queueName, EXCHANGE_NAME, routingKey);

    return new DataTypeChannelConsumer(connection, channel, queueName);
  }

  /**
   * Ask the broker for one message. `false` means the queue was empty.
   *
   * noAck is false, so what comes back is *locked to us* and not yet removed from the
   * queue. The broker is now waiting to be told what happened. Until we tell it, this
   * message shows in the management console as "unacked".
   */
  receive() {
    return this.#channel.get(this.#queueName, { noAck: false });
  }

  /** I am done with this message. The broker may forget it. */
  acknowledge(delivery) {
    this.#channel.ack(delivery, false);
  }

  /**
   * I am not done with this message.
   * requeue = true  -- put it back, someone will try again (possibly us, immediately).
   * requeue = false -- reject it. On a plain queue that deletes it.
   */
  reject(delivery, requeue) {
    this.#channel.nack(delivery, false, requeue);
  }

  async close() {
    await this.#channel.close();
    await this.#connection.close();
  }
}
