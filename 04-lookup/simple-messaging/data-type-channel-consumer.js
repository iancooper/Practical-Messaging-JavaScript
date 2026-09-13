/**
 * The consumer half of the Messaging Gateway. It declares the topology described in
 * channel.js and hands the pump five things it can do with a message.
 *
 * **The plumbing is given to you and it is correct.** Declaring exchanges and binding queues
 * is AMQP vocabulary, not judgement, and you can read it here at your leisure. The exercise is
 * deciding *which of these five to call, and when* -- and that lives in the pump.
 */
import amqp from 'amqplib';

import {
  BROKER_URL,
  DEAD_LETTER_EXCHANGE_NAME,
  EXCHANGE_NAME,
  RETRY_DELAY_MS,
  deadLetterQueueNameFor,
  invalidQueueNameFor,
  queueNameFor,
  retryQueueNameFor,
  routingKeyFor,
} from './channel.js';

export class DataTypeChannelConsumer {
  #connection;
  #channel;
  #queueName;
  #invalidQueueName;
  #deadLetterQueueName;
  #retryQueueName;

  constructor(connection, channel, names) {
    this.#connection = connection;
    this.#channel = channel;
    this.#queueName = names.queue;
    this.#invalidQueueName = names.invalid;
    this.#deadLetterQueueName = names.dead;
    this.#retryQueueName = names.retry;
  }

  static async create(messageClass, brokerUrl = BROKER_URL) {
    const connection = await amqp.connect(brokerUrl);
    const channel = await connection.createConfirmChannel();

    const routingKey = routingKeyFor(messageClass);
    const queueName = queueNameFor(messageClass);
    const invalidKey = invalidQueueNameFor(messageClass);
    const deadKey = deadLetterQueueNameFor(messageClass);
    const retryKey = retryQueueNameFor(messageClass);

    await channel.assertExchange(EXCHANGE_NAME, 'direct', { durable: true });
    await channel.assertExchange(DEAD_LETTER_EXCHANGE_NAME, 'direct', { durable: true });

    // The work queue. Rejecting a message from here (nack, requeue: false) sends it to the
    // dead-letter exchange with the *retry* routing key -- so a rejection lands in the
    // retry queue without us publishing anything.
    //
    // Note what this means: the queue's dead-letter routing key is fixed at declare time.
    // One reject, one destination. Anything else you want to do with a message, you do by
    // publishing it somewhere yourself.
    await channel.assertQueue(queueName, {
      durable: true, exclusive: false, autoDelete: false,
      arguments: {
        'x-dead-letter-exchange': DEAD_LETTER_EXCHANGE_NAME,
        'x-dead-letter-routing-key': retryKey,
      },
    });
    await channel.bindQueue(queueName, EXCHANGE_NAME, routingKey);

    // Bodies we could not read. A terminal destination: no TTL, no dead-letter exchange.
    await channel.assertQueue(invalidKey, { durable: true, exclusive: false, autoDelete: false });
    await channel.bindQueue(invalidKey, DEAD_LETTER_EXCHANGE_NAME, invalidKey);

    // Work we gave up on. Also terminal. This is the one an operator looks in.
    await channel.assertQueue(deadKey, { durable: true, exclusive: false, autoDelete: false });
    await channel.bindQueue(deadKey, DEAD_LETTER_EXCHANGE_NAME, deadKey);

    // The retry queue: a waiting room with a clock on the door.
    // Nothing consumes it. Every message in it expires after RETRY_DELAY_MS, and an expired
    // message is dead-lettered -- back to the main exchange, and so back to the work queue.
    // RabbitMQ stamps an x-death header on the way through, which is how we count attempts.
    await channel.assertQueue(retryKey, {
      durable: true, exclusive: false, autoDelete: false,
      arguments: {
        'x-message-ttl': RETRY_DELAY_MS,
        'x-dead-letter-exchange': EXCHANGE_NAME,
        'x-dead-letter-routing-key': routingKey,
      },
    });
    await channel.bindQueue(retryKey, DEAD_LETTER_EXCHANGE_NAME, retryKey);

    return new DataTypeChannelConsumer(connection, channel, {
      queue: queueName, invalid: invalidKey, dead: deadKey, retry: retryKey,
    });
  }

  /** Ask the broker for one message. `false` means the queue was empty. */
  receive() {
    return this.#channel.get(this.#queueName, { noAck: false });
  }

  /** Done. The broker may forget it. */
  acknowledge(delivery) {
    this.#channel.ack(delivery, false);
  }

  /**
   * Put it back on the queue, right now, for someone to try again immediately.
   * There is no limit on this and no delay. Think about what that means before you use it.
   */
  requeue(delivery) {
    this.#channel.nack(delivery, false, true);
  }

  /**
   * Reject it. Because of the work queue's arguments, the broker routes it to the
   * **retry queue**, where it waits and then comes back on its own. One call, and RabbitMQ
   * does the moving -- and because RabbitMQ owns both hops, RabbitMQ counts them for you.
   */
  rejectForRetry(delivery) {
    this.#channel.nack(delivery, false, false);
  }

  /**
   * Publish it to the invalid message queue. Terminal: a body nobody can read.
   *
   * This is a publish, not a reject -- so the original delivery is still outstanding and it
   * is still your problem. Headers are carried forward, because x-death is the attempt count
   * and losing it resets the clock.
   */
  sendToInvalidMessageQueue(delivery) {
    return this.#republish(delivery, this.#invalidQueueName);
  }

  /** Send it to the dead letter queue. Terminal: somebody has to come and look. */
  sendToDeadLetter(delivery) {
    return this.#republish(delivery, this.#deadLetterQueueName);
  }

  async #republish(delivery, routingKey) {
    this.#channel.publish(DEAD_LETTER_EXCHANGE_NAME, routingKey, delivery.content, {
      persistent: true,
      headers: { ...delivery.properties.headers },
    });
    await this.#channel.waitForConfirms();
  }

  /**
   * How many times has this message been round the retry loop?
   *
   * RabbitMQ records every dead-lettering in an `x-death` header: an array of entries, one
   * per (queue, reason) pair, each with a count. A message that has expired out of the retry
   * queue twice has an entry for that queue with count 2. A message arriving for the first
   * time has no x-death header at all, so it has had no attempts yet.
   *
   * Look at this header in the management console. It is the most useful thing RabbitMQ will
   * tell you about a message's history and almost nobody knows it is there.
   */
  retriesSoFar(delivery) {
    const deaths = delivery.properties.headers?.['x-death'];
    if (!Array.isArray(deaths)) return 0;

    const entry = deaths.find((death) => death?.queue === this.#retryQueueName);
    return entry ? Number(entry.count) : 0;
  }

  async close() {
    await this.#channel.close();
    await this.#connection.close();
  }
}
