/**
 * Reference data the handler needs: what does this SKU cost?
 *
 * It fails in the three ways a real lookup fails, and telling them apart is the exercise:
 *
 *   WIDGET-1, GIZMO-2   fine
 *   GIZMO-SLOW          in the catalogue, but the lookup takes 30 seconds
 *   FLAKY-1             fails twice, then works -- a service that was restarting
 *   anything else       not in the catalogue, and never will be
 */
import { setTimeout as delay } from 'node:timers/promises';

/** Permanent. This SKU does not exist and asking again will not change that. */
export class UnknownSkuError extends Error {
  constructor(sku) {
    super(`'${sku}' is not in the catalogue`);
    this.name = 'UnknownSkuError';
    this.sku = sku;
  }
}

/** Transient. The lookup is unwell; the order is fine. Try again shortly. */
export class CatalogueUnavailableError extends Error {
  constructor(sku, attempt) {
    super(`catalogue is unavailable (attempt ${attempt} for '${sku}')`);
    this.name = 'CatalogueUnavailableError';
    this.sku = sku;
    this.attempt = attempt;
  }
}

const PRICES = new Map([
  ['WIDGET-1', 9.99],
  ['GIZMO-2', 24.50],
  ['GIZMO-SLOW', 24.50],
  ['FLAKY-1', 12.00],
]);

export class Catalogue {
  static SLOW_LOOKUP_SECONDS = 30;

  /** How many times FLAKY-1 fails before it starts working. */
  static FLAKY_FAILURES = 2;

  #flakyAttempts = 0;

  async priceOf(sku) {
    if (sku === 'GIZMO-SLOW') {
      console.log(`  catalogue: looking up ${sku} (this one takes ${Catalogue.SLOW_LOOKUP_SECONDS}s)`);
      await delay(Catalogue.SLOW_LOOKUP_SECONDS * 1000);
    }

    if (sku === 'FLAKY-1') {
      this.#flakyAttempts++;
      if (this.#flakyAttempts <= Catalogue.FLAKY_FAILURES) {
        throw new CatalogueUnavailableError(sku, this.#flakyAttempts);
      }
      console.log(`  catalogue: ${sku} worked on attempt ${this.#flakyAttempts}`);
    }

    if (!PRICES.has(sku)) {
      throw new UnknownSkuError(sku);
    }

    return PRICES.get(sku);
  }
}
