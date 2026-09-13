/**
 * Application code: price the order, place it, and tell the world it happened.
 *
 * A command came in on a queue; a fact goes out on a stream. That is an entirely ordinary
 * shape and you have probably written it -- which is the point.
 *
 * ---------------------------------------------------------------------------------------
 *  Nothing in this class is wrong, and that is what makes exercise 3 worth doing. The
 *  handler is clean, the domain has no broker in it, the ordering is the sensible one.
 *  Read PROBE.md before you run it, and predict what a crash costs you.
 * ---------------------------------------------------------------------------------------
 */
import { randomUUID } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';

import { OrderPlaced } from './order-placed.js';

/**
 * How long to pause between publishing the event and returning to the pump -- which is
 * to say, between the Kafka write and the RabbitMQ acknowledgement.
 *
 * In a real service that gap is microseconds wide. It is still a gap, and a service that
 * handles a million orders will fall into it. Widening it to ten seconds does not create
 * the problem; it just means you can aim at it.
 *
 *     DUAL_WRITE_WINDOW=10 node receiver.js
 */
const WINDOW_SECONDS = Number.parseInt(process.env.DUAL_WRITE_WINDOW ?? '', 10) || 0;

export class PlaceOrderHandler {
  #catalogue;
  #events;

  /**
   * @param {import('./catalogue.js').Catalogue} catalogue
   * @param {import('../simple-messaging/event-publisher.js').EventPublisher} events
   */
  constructor(catalogue, events) {
    this.#catalogue = catalogue;
    this.#events = events;
  }

  async handle(order) {
    const price = await this.#catalogue.priceOf(order.sku);
    const total = price * order.quantity;

    console.log(`  placed order ${order.id}: ${order.quantity} x ${order.sku} for ${total.toFixed(2)}`);

    await this.#events.publish(new OrderPlaced(
      randomUUID(), order.id, order.sku, order.quantity, total, new Date().toISOString()));

    if (WINDOW_SECONDS > 0) {
      console.log('  [the event is on the stream. RabbitMQ has NOT been acked yet.]');
      console.log(`  [you have ${WINDOW_SECONDS} seconds. kill -9 ${process.pid}]`);
      await delay(WINDOW_SECONDS * 1000);
    }
  }
}
