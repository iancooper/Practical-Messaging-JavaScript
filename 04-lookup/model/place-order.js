/**
 * A Command Message: go and do this. One recipient, and it is allowed to fail.
 *
 * Every member is required and unmapped members are disallowed, so a body that is not
 * exactly this shape will not deserialize. That is deliberate -- you need a message the
 * receiver cannot understand, and "nearly the right JSON" is the realistic version of one.
 */
import { randomUUID } from 'node:crypto';

// The wire format is the same in every language repo, so a JavaScript sender and a Java
// receiver would understand each other. Hence the capitalised names.
const WIRE = { id: 'Id', sku: 'Sku', quantity: 'Quantity', customerId: 'CustomerId' };

export class PlaceOrder {
  constructor(id, sku, quantity, customerId) {
    this.id = id;
    this.sku = sku;
    this.quantity = quantity;
    this.customerId = customerId;
  }

  static serialize(order) {
    return JSON.stringify(
      Object.fromEntries(Object.entries(WIRE).map(([field, wire]) => [wire, order[field]])));
  }

  /**
   * Throws {@link TypeError} if the body is not exactly a PlaceOrder.
   *
   * JavaScript will happily build an object out of nearly-right JSON -- JSON.parse returns
   * whatever was there and every missing field is just `undefined` -- so we check the shape
   * ourselves: every field required, and nothing else permitted. Strictness here is what
   * makes an unmappable message possible at all.
   */
  static deserialize(body) {
    let raw;
    try {
      raw = JSON.parse(body);
    } catch (e) {
      throw new TypeError(`body is not JSON: ${e.message}`);
    }

    if (raw === null || typeof raw !== 'object' || Array.isArray(raw)) {
      throw new TypeError('body is not a JSON object');
    }

    const expected = new Set(Object.values(WIRE));
    const actual = new Set(Object.keys(raw));
    const missing = [...expected].filter((k) => !actual.has(k));
    const unexpected = [...actual].filter((k) => !expected.has(k));

    if (missing.length || unexpected.length) {
      throw new TypeError(
        `not a PlaceOrder: missing [${missing.sort().join(', ') || 'none'}], ` +
        `unexpected [${unexpected.sort().join(', ') || 'none'}]`);
    }

    return new PlaceOrder(raw.Id, raw.Sku, raw.Quantity, raw.CustomerId);
  }

  static for(sku, quantity = 1, customerId = 'CUST-001') {
    return new PlaceOrder(randomUUID(), sku, quantity, customerId);
  }
}
