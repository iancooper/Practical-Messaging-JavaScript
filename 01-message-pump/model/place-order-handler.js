/**
 * Application code. What the business actually wanted: price the order and place it.
 *
 * ---------------------------------------------------------------------------------------
 *  THIS HANDLER IS PART OF THE EXERCISE. See PROBE.md.
 *
 *  Ask yourself one question before you read any further: how much of this method is
 *  about placing an order?
 * ---------------------------------------------------------------------------------------
 */
import { PlaceOrder } from './place-order.js';

export class PlaceOrderHandler {
  #catalogue;

  constructor(catalogue) {
    this.#catalogue = catalogue;
  }

  /** @param {import('amqplib').GetMessage} delivery what RabbitMQ handed the pump */
  async handle(delivery) {
    const body = delivery.content.toString('utf8');
    const order = PlaceOrder.deserialize(body);

    const price = await this.#catalogue.priceOf(order.sku);
    const total = price * order.quantity;

    console.log(
      `  placed order ${order.id}: ${order.quantity} x ${order.sku} ` +
      `for ${total.toFixed(2)} (customer ${order.customerId})`);
  }
}
