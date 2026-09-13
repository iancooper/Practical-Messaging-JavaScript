/**
 * The same gateway as {@link EventStreamConsumer}, with one difference that is the whole
 * reason it exists: **the caller commits.**
 *
 * EventStreamConsumer does Get, Translate, Dispatch, Handle and then commits for you, which is
 * the right shape when the ordering is not what you are studying. In exercise 4 the ordering
 * *is* what you are studying -- Probe D is "apply the record, then commit the offset, and die
 * in between" -- so the commit has to be a line in the application that you can move.
 *
 * Notice that this is a *gateway* decision, not an application one. The application still names
 * no Kafka type: it is handed a {@link StreamRecord}, and it says commit or seek.
 */
import Confluent from '@confluentinc/kafka-javascript';
import { setTimeout as delay } from 'node:timers/promises';

import { BOOTSTRAP_SERVERS, ensureTopicExists, topicFor } from './stream.js';

const { KafkaConsumer } = Confluent;

const RETRY_DELAY_MS = 2000;
const POLL_TIMEOUT_MS = 1000;

/** A record read off the stream, and where it was. The position is ours, not yours. */
export class StreamRecord {
  /** The mapped event. The only part of this the application is meant to look at. */
  message;

  /** "p1@42" -- the partition and offset, for printing. */
  where;

  /** Where this record is, in the client's vocabulary. Not for the application. */
  position;

  /**
   * Where the *next* record is, which is what a commit actually means.
   *
   * **A committed offset is "the next one I have not read", not "the last one I did read".**
   * Commit this record's own offset and you have told the broker to start here again, so every
   * restart replays the record you just finished -- and a drained group sits for ever at a lag
   * of one per partition. That looks exactly like the duplicate Probe D is about, and is not
   * it. Off by one, and the symptom is somebody else's bug.
   *
   * The client has two ways to commit and they do not agree about this. `commitMessageSync`
   * takes the *message* and adds the one for you; `commitSync` takes explicit offsets and adds
   * nothing. This gateway uses the explicit form -- because the caller has to be able to move
   * the line -- so the `+ 1` is ours to write, and it is written here, once.
   */
  next;

  constructor(message, where, record) {
    this.message = message;
    this.where = where;
    this.position = { topic: record.topic, partition: record.partition, offset: record.offset };
    this.next = { ...this.position, offset: record.offset + 1, leaderEpoch: record.leaderEpoch };
  }
}

export class EventStreamReader {
  #topic;
  #mapper;
  #consumerGroup;
  #consumer;

  constructor(topic, mapper, consumerGroup, consumer) {
    this.#topic = topic;
    this.#mapper = mapper;
    this.#consumerGroup = consumerGroup;
    this.#consumer = consumer;
  }

  static async create(messageClass, mapper, consumerGroup, bootstrapServers = BOOTSTRAP_SERVERS) {
    const topic = topicFor(messageClass);
    await ensureTopicExists(topic, bootstrapServers);

    const consumer = new KafkaConsumer(
      {
        'bootstrap.servers': bootstrapServers,
        'group.id': consumerGroup,
        // Commit when the *caller* says so, which is the point of this class.
        'enable.auto.commit': false,
      },
      {
        // The local copy is built by replaying the whole log, which is the thing a stream can
        // do and a queue cannot. A new consumer with an empty database reads from the start
        // and catches up; that is Archive and Replay from exercise 3, earning its keep.
        //
        // Per-topic setting, so it goes in the second object -- see event-stream-consumer.js
        // for what happens when it goes in the first.
        'auto.offset.reset': 'earliest',
      });

    await new Promise((resolve, reject) => {
      consumer.once('ready', resolve);
      consumer.once('event.error', reject);
      consumer.connect();
    });

    consumer.subscribe([topic]);
    consumer.setDefaultConsumeTimeout(POLL_TIMEOUT_MS);

    return new EventStreamReader(topic, mapper, consumerGroup, consumer);
  }

  get topic() {
    return this.#topic;
  }

  get consumerGroup() {
    return this.#consumerGroup;
  }

  /**
   * Get and Translate. Returns null when there was nothing to read, or when the broker was
   * unhappy in a way that is not this record's fault -- a rebalance, a topic that does not
   * exist yet. Both are normal and the caller should just come round again.
   *
   * Throws when the record could not be mapped. There is no invalid-record topic on a stream
   * and nothing will move it to one for you, which is exercise 3's finding and not this
   * exercise's problem to solve -- so that is fatal, loudly, rather than quietly skipped.
   */
  async read(signal) {
    let records;
    try {
      records = await this.#poll();
    } catch (e) {
      console.log(`  consume failed: ${e.message} -- retrying`);
      await delay(RETRY_DELAY_MS, undefined, { signal });
      return null;
    }

    if (records.length === 0) return null;

    const record = records[0];
    const where = `p${record.partition}@${record.offset}`;

    try {
      return new StreamRecord(this.#mapper(record.value.toString('utf8')), where, record);
    } catch (e) {
      throw new Error(
        `cannot read the record at ${where}: ${e.message}. There is no invalid-record ` +
        'topic on a stream -- see exercise 3.', { cause: e });
    }
  }

  /** Move the bookmark. Everything up to and including this record is done. */
  commit(record) {
    this.#consumer.commitSync(record.next);
  }

  /** Wind the bookmark back to this record and read it again. */
  seek(record) {
    return new Promise((resolve, reject) => {
      this.#consumer.seek(record.position, 1000, (err) => (err ? reject(err) : resolve()));
    });
  }

  /** One record, or none. The callback API turned into something the loop can await. */
  #poll() {
    return new Promise((resolve, reject) => {
      this.#consumer.consume(1, (err, records) => (err ? reject(err) : resolve(records)));
    });
  }

  /** Leave the group tidily so the next run does not wait for a session timeout. */
  close() {
    return new Promise((resolve) => this.#consumer.disconnect(resolve));
  }
}
