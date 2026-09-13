/**
 * The stream's names and shape, in one place -- and notice how much shorter this is than
 * simple-messaging/channel.js.
 *
 * There is one topic. There is no retry topic, no invalid record topic and no dead letter
 * topic, because nothing in Kafka will move a record to one for you. If you want any of those,
 * you write the producer, the consumer, the scheduler and the state yourself.
 *
 * **Three partitions**, because a partition is the unit of both ordering and parallelism:
 * one consumer in a group holds a partition at a time, records within a partition are ordered,
 * and records in different partitions are not ordered relative to each other at all.
 */
import Confluent from '@confluentinc/kafka-javascript';

const { AdminClient } = Confluent;

export const BOOTSTRAP_SERVERS = 'localhost:9092';
export const CONSUMER_GROUP = 'practical-messaging-streams';

/**
 * The price consumer reads a different topic for a different reason, so it gets a group of its
 * own. Two consumers in one group would be told to share the partitions of everything the group
 * subscribes to, which is not what either of them wants -- and the offsets of
 * streams.OrderPlaced and streams.PriceChanged have nothing to do with each other.
 *
 * **A consumer group is a unit of work-sharing, not a name for your application.**
 */
export const PRICE_CONSUMER_GROUP = 'practical-messaging-prices';

export const PARTITIONS = 3;

export function topicFor(messageClass) {
  return 'streams.' + messageClass.name;
}

/**
 * Create the topic if it is not there.
 *
 * Both the producer and the consumer call this, so it does not matter which you start
 * first. (Compare RabbitMQ, where only the consumer declares the queue -- so anything
 * published before the consumer's first ever run went nowhere. Kafka's topic is shared
 * state that either end can create, which is a small but real difference in how the two
 * feel to operate.)
 *
 * It is here rather than left to the broker's auto-create so that the partition count is
 * ours to choose, and so the exercises do not depend on a broker setting.
 */
export async function ensureTopicExists(topic, bootstrapServers = BOOTSTRAP_SERVERS) {
  const admin = AdminClient.create({ 'bootstrap.servers': bootstrapServers });

  try {
    const created = await new Promise((resolve, reject) => {
      admin.createTopic({ topic, num_partitions: PARTITIONS, replication_factor: 1 }, (err) => {
        if (!err) resolve(true);
        // A race with another process creating the same topic is fine; anything else is not.
        else if (/already exists/i.test(String(err))) resolve(false);
        else reject(err);
      });
    });

    if (created) console.log(`Created topic ${topic} with ${PARTITIONS} partitions`);
  } finally {
    admin.disconnect();
  }
}
