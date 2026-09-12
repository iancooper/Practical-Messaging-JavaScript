/**
 * The Message Pump: Get -> Translate -> Dispatch -> Handle, in a loop, until cancelled.
 *
 * ---------------------------------------------------------------------------------------
 *  THIS IS EXERCISE 1's ANSWER, AND EXERCISE 2's PROBLEM.
 *
 *  Everything exercise 1 asked for is here and correct:
 *    - the Translate stage is back, and it goes through a Message Mapper
 *    - the handler takes a domain type; nothing in model/ knows a broker exists
 *    - the acknowledgement happens after the work, not before it
 *    - a failure no longer kills the loop
 *
 *  So it does not lose messages any more, and it does not fall over. It is still wrong, and
 *  the way it is wrong is worse than falling over, because it will not show up in your logs
 *  as a crash. Read PROBE.md.
 * ---------------------------------------------------------------------------------------
 */
import { setTimeout as delay } from 'node:timers/promises';

import { BROKER_URL, queueNameFor } from './channel.js';
import { DataTypeChannelConsumer } from './data-type-channel-consumer.js';

const POLL_INTERVAL_MS = 1000;

export class MessagePump {
  #messageClass;
  #mapper;
  #handler;
  #brokerUrl;

  /**
   * @param {Function} messageClass the type this pump reads
   * @param {import('./message-mapper.js').MessageMapper} mapper the Translate stage
   * @param {import('./handler.js').Handler} handler what to do with each message
   */
  constructor(messageClass, mapper, handler, brokerUrl = BROKER_URL) {
    this.#messageClass = messageClass;
    this.#mapper = mapper;
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
          await delay(POLL_INTERVAL_MS, undefined, { signal });
          continue;
        }

        try {
          // TRANSLATE
          const body = delivery.content.toString('utf8');
          const message = this.#mapper.mapToRequest(body);

          // DISPATCH and HANDLE
          await this.#handler.handle(message);

          // Only now are we done with it.
          consumer.acknowledge(delivery);
        } catch (e) {
          // Something went wrong, and we must not lose the message. Put it back on the
          // queue so it gets tried again.
          console.log(`  FAILED: ${e.message} -- putting it back`);
          consumer.requeue(delivery);
        }
      }
    } finally {
      await consumer.close();
    }
  }
}
