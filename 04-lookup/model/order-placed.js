/**
 * An Event Message: this happened. Many readers may care, none of them may reply, and it is
 * a statement about the past rather than a request.
 *
 * Contrast {@link PlaceOrder}, which was a Command: one recipient, allowed to fail.
 * Same system, ten milliseconds apart, and the difference in intent is the whole reason one
 * goes on a queue and the other on a stream.
 */

const WIRE = {
  id: 'Id',
  orderId: 'OrderId',
  sku: 'Sku',
  quantity: 'Quantity',
  total: 'Total',
  placedAt: 'PlacedAt',
};

export class OrderPlaced {
  constructor(id, orderId, sku, quantity, total, placedAt) {
    this.id = id;
    this.orderId = orderId;
    this.sku = sku;
    this.quantity = quantity;
    this.total = total;
    this.placedAt = placedAt;
  }

  static serialize(event) {
    return JSON.stringify(
      Object.fromEntries(Object.entries(WIRE).map(([field, wire]) => [wire, event[field]])));
  }

  /** Throws {@link TypeError} if the record is not exactly an OrderPlaced. */
  static deserialize(body) {
    let raw;
    try {
      raw = JSON.parse(body);
    } catch (e) {
      throw new TypeError(`record is not JSON: ${e.message}`);
    }

    if (raw === null || typeof raw !== 'object' || Array.isArray(raw)) {
      throw new TypeError('record is not a JSON object');
    }

    const expected = new Set(Object.values(WIRE));
    const actual = new Set(Object.keys(raw));
    const missing = [...expected].filter((k) => !actual.has(k));
    const unexpected = [...actual].filter((k) => !expected.has(k));

    if (missing.length || unexpected.length) {
      throw new TypeError(
        `not an OrderPlaced: missing [${missing.sort().join(', ') || 'none'}], ` +
        `unexpected [${unexpected.sort().join(', ') || 'none'}]`);
    }

    return new OrderPlaced(
      raw.Id, raw.OrderId, raw.Sku, raw.Quantity, raw.Total, raw.PlacedAt);
  }
}
