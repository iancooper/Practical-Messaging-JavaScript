/**
 * Implements the EventPublisher contract over a Kafka topic. This is the only place the
 * handler's "tell the world" becomes "append to a log".
 */
export class KafkaEventPublisher {
  #producer;

  constructor(producer) {
    this.#producer = producer;
  }

  publish(event) {
    return this.#producer.send(event);
  }
}
