/**
 * The Message Pump: Get -> Translate -> Dispatch -> Handle, in a loop, until cancelled.
 *
 * ---------------------------------------------------------------------------------------
 *  THIS IS EXERCISE 2's ANSWER, AND IT IS CORRECT. Nothing here needs fixing.
 *
 *    - Translate goes through a Message Mapper; the handler takes a domain type
 *    - the acknowledgement happens after the work, not before it
 *    - a body we cannot read goes to the invalid message queue, and is never retried
 *    - work that failed is retried with a delay, up to a limit, then dead-lettered
 *    - the retry count comes from RabbitMQ's x-death header; we count nothing ourselves
 *
 *  Every one of those five is something the broker does for you. Exercise 3 is about what
 *  happens to this list when the channel is a stream instead of a queue.
 *
 *  There is one line in here that is now a problem, and it is not a problem with the pump.
 *  Read PROBE.md.
 * ---------------------------------------------------------------------------------------
 */
import { setTimeout as delay } from 'node:timers/promises';

import {
  BROKER_URL,
  RETRY_DELAY_MS,
  deadLetterQueueNameFor,
  invalidQueueNameFor,
  queueNameFor,
} from './channel.js';
import { DataTypeChannelConsumer } from './data-type-channel-consumer.js';
import { UnmappableMessageError } from './message-mapper.js';

const POLL_INTERVAL_MS = 1000;

/** How many times we retry before giving up. This is n. */
const MAX_RETRIES = 3;

export class MessagePump {
  #messageClass;
  #mapper;
  #handler;
  #brokerUrl;

  constructor(messageClass, mapper, handler, brokerUrl = BROKER_URL) {
    this.#messageClass = messageClass;
    this.#mapper = mapper;
    this.#handler = handler;
    this.#brokerUrl = brokerUrl;
  }

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
          if (e instanceof UnmappableMessageError) {
            // A failure to UNDERSTAND. The bytes are not going to change, so there is
            // nothing to retry -- retrying this is the definition of a poison pill.
            // Publish it to the invalid message queue, where someone can go and look at
            // it. A reject would send it to the *retry* queue, which is the one thing this
            // message must never go to.
            console.log(`  INVALID: ${e.message}`);
            console.log(`  -> ${invalidQueueNameFor(this.#messageClass)}`);
            await consumer.sendToInvalidMessageQueue(delivery);
            consumer.acknowledge(delivery);
            continue;
          }

          // A failure to PROCESS. The message was perfectly readable; the work failed.
          // That may have been bad luck, so it is worth trying again -- but not forever.
          const retries = consumer.retriesSoFar(delivery);

          if (retries < MAX_RETRIES) {
            console.log(`  FAILED on attempt ${retries + 1}: ${e.message}`);
            console.log(`  -> retrying in ${RETRY_DELAY_MS / 1000}s`);
            consumer.rejectForRetry(delivery);
          } else {
            console.log(`  GIVING UP after ${retries + 1} attempts: ${e.message}`);
            console.log(`  -> ${deadLetterQueueNameFor(this.#messageClass)}`);
            await consumer.sendToDeadLetter(delivery);
            consumer.acknowledge(delivery);
          }
        }
      }
    } finally {
      await consumer.close();
    }
  }
}
