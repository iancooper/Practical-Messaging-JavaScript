/**
 * The names both ends of the channel have to agree on, in one place.
 *
 * A Datatype Channel carries one type of message, so we derive the routing key from the
 * type. Producer and consumer both compute it, which is how they find each other without
 * a shared config file.
 */

export const EXCHANGE_NAME = 'practical-messaging-pump';

export const BROKER_URL = 'amqp://guest:guest@localhost:5672';

/** @param {Function} messageClass the class on this channel; its name is the routing key */
export function routingKeyFor(messageClass) {
  return 'message-pump.' + messageClass.name;
}

/** @param {Function} messageClass */
export function queueNameFor(messageClass) {
  return routingKeyFor(messageClass);
}
