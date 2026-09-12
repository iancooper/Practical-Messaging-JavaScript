/**
 * Reference data the handler needs in order to do its job: what does this SKU cost?
 *
 * Here it is a dictionary, because exercises 1 to 3 are not about lookups. It behaves like
 * the real thing in the two ways that matter to us: it can be slow, and it can not know.
 *
 * (Exercise 4, if you get to it, replaces this with a local copy filled from a stream.)
 */
import { setTimeout as delay } from 'node:timers/promises';

export class UnknownSkuError extends Error {
  constructor(sku) {
    super(`'${sku}' is not in the catalogue`);
    this.name = 'UnknownSkuError';
    this.sku = sku;
  }
}

const PRICES = new Map([
  ['WIDGET-1', 9.99],
  ['GIZMO-2', 24.50],
  ['GIZMO-SLOW', 24.50],   // in the catalogue, but the lookup crawls
]);

export class Catalogue {
  /** How long a lookup takes. GIZMO-SLOW is the one that hurts. */
  static SLOW_LOOKUP_SECONDS = 30;

  async priceOf(sku) {
    if (sku === 'GIZMO-SLOW') {
      console.log(`  catalogue: looking up ${sku} (this one takes ${Catalogue.SLOW_LOOKUP_SECONDS}s)`);
      await delay(Catalogue.SLOW_LOOKUP_SECONDS * 1000);
    }

    if (!PRICES.has(sku)) {
      throw new UnknownSkuError(sku);
    }

    return PRICES.get(sku);
  }
}
