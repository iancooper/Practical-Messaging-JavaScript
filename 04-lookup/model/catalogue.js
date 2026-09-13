/**
 * Reference data the handler needs: what does this SKU cost?
 *
 * **Everything about how this answers has changed, and its signature has not.** In exercises
 * 1 to 3 it was a Map that pretended to be a service call, and it failed in three ways: slow
 * (GIZMO-SLOW), briefly unwell (FLAKY-1) and unknown. Two of those three are gone, and their
 * going is the lesson -- they were *on-demand* failures, and there is no longer a call to be
 * slow or unwell. Get It In Advance did not fix them. It removed the thing that could fail,
 * and bought you Probe B instead.
 *
 * The handler did not change. It still asks the catalogue for a price, and the catalogue still
 * decides where prices come from. That is what the seam was for.
 */

/** Permanent. This SKU does not exist and asking again will not change that. */
export class UnknownSkuError extends Error {
  constructor(sku) {
    super(`'${sku}' is not in the catalogue`);
    this.name = 'UnknownSkuError';
    this.sku = sku;
  }
}

/**
 * Transient. We have no copy of the catalogue yet -- the price consumer has not started, or
 * has not caught up. The SKU may be perfectly good; we are simply not ready to price it.
 *
 * **This is a different fact from {@link UnknownSkuError} and the difference is the point of
 * Probe C.** One of them is about the order and one of them is about us.
 */
export class LocalCopyEmptyError extends Error {
  constructor(sku) {
    super(`cannot price '${sku}': the local copy has no prices in it yet`);
    this.name = 'LocalCopyEmptyError';
    this.sku = sku;
  }
}

export class Catalogue {
  #prices;

  /** @param {import('./price-store.js').PriceStore} prices the local copy, however it is kept */
  constructor(prices) {
    this.#prices = prices;
  }

  async priceOf(sku) {
    const price = await this.#prices.lookup(sku);

    if (price !== null) return price.amount;

    // Two different failures wear the same shape -- a lookup that returned nothing -- and
    // exercise 2 spent forty minutes on why that matters. "I have no copy yet" is about us
    // and will fix itself; "that SKU is not a thing" is about the order and never will.
    //
    // **The domain's job is to say which.** What to do about each is the pump's policy and
    // not ours, and Probe C is about the fact that the pump currently does the same thing
    // with both.
    if (await this.#prices.count() === 0) throw new LocalCopyEmptyError(sku);

    throw new UnknownSkuError(sku);
  }
}
