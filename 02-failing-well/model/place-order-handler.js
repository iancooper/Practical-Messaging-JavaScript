/**
 * Application code, and this is what it should look like: a domain type in, a return or a
 * throw out. No delivery, no headers, no acknowledgement, no broker.
 *
 * A test can call this. So can an HTTP endpoint. That is a consequence of the separation
 * rather than the reason for it, but it is a good smoke alarm: if you cannot call your
 * handler from a test without a broker running, the mapper has not finished its job.
 */
export class PlaceOrderHandler {
  #catalogue;

  constructor(catalogue) {
    this.#catalogue = catalogue;
  }

  async handle(order) {
    const price = await this.#catalogue.priceOf(order.sku);
    const total = price * order.quantity;

    console.log(
      `  placed order ${order.id}: ${order.quantity} x ${order.sku} ` +
      `for ${total.toFixed(2)} (customer ${order.customerId})`);
  }
}
