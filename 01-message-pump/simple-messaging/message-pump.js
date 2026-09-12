/**
 * The Message Pump: take a message off a channel, get it to application code, repeat
 * until cancelled.
 *
 *     Get -> Translate -> Dispatch -> Handle
 *
 * Each of those four stages fails in its own way, which is why a message that is never going
 * to be handled has four different places it can end up.
 *
 * ---------------------------------------------------------------------------------------
 *  THIS PUMP IS THE EXERCISE.
 *
 *  It runs, and messages flow through it. It is also wrong, in more than one way, and every
 *  way it is wrong is something that has shipped to production somewhere.
 *
 *  Read it before you run it. Then read PROBE.md. Do not copy this file into anything.
 * ---------------------------------------------------------------------------------------
 */
import { setTimeout as delay } from 'node:timers/promises';

import { BROKER_URL, queueNameFor } from './channel.js';
import { DataTypeChannelConsumer } from './data-type-channel-consumer.js';

const POLL_INTERVAL_MS = 1000;

export class MessagePump {
  #messageClass;
  #handler;
  #brokerUrl;

  /**
   * @param {Function} messageClass the type this pump reads
   * @param {import('./handler.js').Handler} handler what to do with each message
   */
  constructor(messageClass, handler, brokerUrl = BROKER_URL) {
    this.#messageClass = messageClass;
    this.#handler = handler;
    this.#brokerUrl = brokerUrl;
  }

  /** Pump until `signal` is aborted. `signal` is an AbortSignal. */
  async run(signal) {
    const consumer = await DataTypeChannelConsumer.create(this.#messageClass, this.#brokerUrl);

    console.log(`Pump running on ${queueNameFor(this.#messageClass)}`);

    try {
      while (!signal.aborted) {
        // GET
        const delivery = await consumer.receive();

        if (!delivery) {
          // Nothing there. Yield, so a Polling Consumer does not spin the CPU.
          await delay(POLL_INTERVAL_MS, undefined, { signal });
          continue;
        }

        console.log(`Got delivery ${delivery.fields.deliveryTag}`);

        // We have the message in our hands, so the broker does not need to hold it for us
        // any more. Tell it we are done and let it free the slot.
        consumer.acknowledge(delivery);

        // TRANSLATE, DISPATCH and HANDLE
        await this.#handler.handle(delivery);
      }
    } finally {
      await consumer.close();
    }
  }
}
