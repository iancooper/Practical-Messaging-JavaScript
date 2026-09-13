/**
 * The ECST event: the catalogue service, which owns SKUs and prices, says what one costs now.
 *
 * **It is a snapshot, not a delta**, and that is the decision the whole exercise turns on.
 * "WIDGET-1 is now 11.99" can be applied twice with no harm; "WIDGET-1 went up by 2.00" cannot.
 * Probe D is where that stops being a matter of taste -- see README.md, step 1.
 *
 * `changedAt` is here for two reasons and both are probes. Probe A subtracts it from the moment
 * the consumer applies the record, and that difference is your staleness. Probe C asks how old
 * your local copy is, and a copy that does not carry a date cannot answer.
 */
import { randomUUID } from 'node:crypto';

// Capitalised, like every other wire format in this repo, so a JavaScript seeder and a C#
// price consumer would understand each other.
const WIRE = { id: 'Id', sku: 'Sku', price: 'Price', changedAt: 'ChangedAt' };

export class PriceChanged {
  constructor(id, sku, price, changedAt) {
    this.id = id;
    this.sku = sku;
    this.price = price;
    this.changedAt = changedAt;
  }

  static serialize(event) {
    return JSON.stringify(
      Object.fromEntries(Object.entries(WIRE).map(([field, wire]) => [wire, event[field]])));
  }

  /** Throws {@link TypeError} if the record is not exactly a PriceChanged. */
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
        `not a PriceChanged: missing [${missing.sort().join(', ') || 'none'}], ` +
        `unexpected [${unexpected.sort().join(', ') || 'none'}]`);
    }

    return new PriceChanged(raw.Id, raw.Sku, raw.Price, raw.ChangedAt);
  }

  static for(sku, price) {
    return new PriceChanged(randomUUID(), sku, price, new Date().toISOString());
  }
}
