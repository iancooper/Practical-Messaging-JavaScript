/**
 * Reads records from a Kafka topic and hands them to application code.
 *
 * It is the same four stages as the message pump -- Get, Translate, Dispatch, Handle -- and
 * then the fifth thing, the one that decides everything about failure, is different:
 *
 *   On a queue:  acknowledge this message. The broker holds the others.
 *   On a stream: commit this offset. It means "I am past everything up to here."
 *
 * **An offset is a bookmark, not a lock.** There is no per-record acknowledgement, so there is
 * nothing to withhold for one record and grant for another. You are either past a point in the
 * log or you are not.
 *
 * Which means every mechanism exercise 2 relied on is simply absent:
 *
 *   requeue                 -- nothing to hand back
 *   requeue with delay      -- nothing holding it, so nothing to hold it longer
 *   reject                  -- nothing to route it away
 *   dead letter queue       -- nothing to move it there
 *   redelivery count        -- nothing counting
 *
 * This consumer takes the default answer, which is the one most frameworks take for you:
 * **retry in place.** Read what that does to a partition, then read PROBE.md.
 */
import Confluent from '@confluentinc/kafka-javascript';
import { setTimeout as delay } from 'node:timers/promises';

import { BOOTSTRAP_SERVERS, CONSUMER_GROUP, ensureTopicExists, topicFor } from './stream.js';

const { KafkaConsumer } = Confluent;

const RETRY_DELAY_MS = 2000;
const POLL_TIMEOUT_MS = 1000;

export class EventStreamConsumer {
  #topic;
  #mapper;
  #handler;
  #consumerGroup;
  #consumer;

  constructor(topic, mapper, handler, consumerGroup, consumer) {
    this.#topic = topic;
    this.#mapper = mapper;
    this.#handler = handler;
    this.#consumerGroup = consumerGroup;
    this.#consumer = consumer;
  }

  static async create(
    messageClass, mapper, handler,
    consumerGroup = CONSUMER_GROUP, bootstrapServers = BOOTSTRAP_SERVERS,
  ) {
    const topic = topicFor(messageClass);

    // Either end may create the topic, so it does not matter which you start first.
    await ensureTopicExists(topic, bootstrapServers);

    const consumer = new KafkaConsumer(
      {
        'bootstrap.servers': bootstrapServers,
        'group.id': consumerGroup,
        // Commit when we say so. Auto-commit on a timer would move the bookmark past
        // records we have not finished with, which is the stream's version of acking early.
        'enable.auto.commit': false,
      },
      {
        // Start at the beginning of the log the first time this group ever reads it.
        // A queue has no equivalent of this setting, because a queue has no past.
        //
        // It goes in the *second* argument. librdkafka splits its settings into client-wide
        // ones and per-topic ones, and auto.offset.reset is per-topic -- put it in the first
        // object and it is quietly ignored, the group starts at the end of the log, and you
        // spend an afternoon wondering where your records went.
        'auto.offset.reset': 'earliest',
      });

    await new Promise((resolve, reject) => {
      consumer.once('ready', resolve);
      consumer.once('event.error', reject);
      consumer.connect();
    });

    consumer.subscribe([topic]);

    // How long a poll waits for a record before giving up and returning nothing.
    consumer.setDefaultConsumeTimeout(POLL_TIMEOUT_MS);

    return new EventStreamConsumer(topic, mapper, handler, consumerGroup, consumer);
  }

  async run(signal) {
    console.log(`Following ${this.#topic} as group '${this.#consumerGroup}'`);

    try {
      while (!signal.aborted) {
        // GET
        let records;
        try {
          records = await this.#poll();
        } catch (e) {
          // Broker-level, not record-level: the topic is not there yet, a rebalance
          // is in progress, the broker is restarting. None of those are this
          // record's fault, because there is no record.
          console.log(`  consume failed: ${e.message} -- retrying`);
          await delay(RETRY_DELAY_MS, undefined, { signal });
          continue;
        }

        if (records.length === 0) continue;

        const record = records[0];
        const where = `p${record.partition}@${record.offset}`;

        try {
          // TRANSLATE
          const event = this.#mapper(record.value.toString('utf8'));

          // DISPATCH and HANDLE
          await this.#handler(event);

          // Move the bookmark. Everything up to and including this offset is done.
          //
          // Hand it the record, not an offset. A committed offset is the *next* record the
          // group wants, so it is always one past the one you just finished -- and this call
          // adds that one for you. Add it yourself as well and the group ends up bookmarked
          // past the end of the log, which is not an error: the offset is out of range, so
          // auto.offset.reset quietly sends the next run back to the beginning.
          this.#consumer.commitMessageSync(record);
          console.log(`  committed ${where}`);
        } catch (e) {
          console.log(`  FAILED ${where}: ${e.message}`);
          console.log('  there is no nack, no requeue and no dead letter topic, so: retry in place');

          // Wind the bookmark back to this record and read it again. The partition
          // stops here until this record succeeds -- which, if it never can, is
          // forever. The other partitions carry on, perfectly happily.
          await this.#seekTo(record);
          await delay(RETRY_DELAY_MS, undefined, { signal });
        }
      }
    } catch (e) {
      // Ctrl-C during one of the delays. The expected ending.
      if (e.name !== 'AbortError') throw e;
    }
  }

  /** One record, or none. The callback API turned into something the loop can await. */
  #poll() {
    return new Promise((resolve, reject) => {
      this.#consumer.consume(1, (err, records) => (err ? reject(err) : resolve(records)));
    });
  }

  #seekTo(record) {
    return new Promise((resolve, reject) => {
      this.#consumer.seek(
        { topic: record.topic, partition: record.partition, offset: record.offset },
        1000,
        (err) => (err ? reject(err) : resolve()));
    });
  }

  /** Leave the group tidily so the next run does not wait for a session timeout. */
  close() {
    return new Promise((resolve) => this.#consumer.disconnect(resolve));
  }
}
