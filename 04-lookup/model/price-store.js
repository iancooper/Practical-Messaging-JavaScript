/**
 * The local copy of somebody else's reference data, as the domain sees it.
 *
 * ---------------------------------------------------------------------------------------
 *  **This contract is declared by model/, and that is the whole point of it.**
 *
 *  Nothing under model/ names SQLite, or a file, or the separate process that fills it.
 *  local-copy/ knows all three, receiver.js puts the two together, and the domain names
 *  none of it.
 *
 *  It is exercise 1's fix arriving a second time, against a storage technology instead of a
 *  broker, and the seam did not have to change to cope. Import `node:sqlite` from anywhere
 *  under model/ and you have undone it -- which is what `node check-domain.js` is for.
 * ---------------------------------------------------------------------------------------
 *
 * Note that it answers three different questions, not one. "Have you a price for this?" is the
 * obvious one. "Have you any prices at all?" separates *the SKU is unknown* from *we are not
 * ready yet*, which is Probe C. "How old is the newest thing you have?" is the only question
 * that can tell a current copy from a stale one, and it is the one nothing asks often enough.
 *
 * **A lookup that can only say yes or no cannot be operated.** That is a design decision you
 * make when you write the contract, long before anybody needs the answer.
 *
 * JavaScript has no interfaces, so this is a contract written down rather than one the runtime
 * enforces -- which is why the file exports nothing; `export {}` is only there to make it a
 * module. The discipline is the same one `simple-messaging/handler.js` relies on.
 *
 * @typedef {object} Price
 *   One row of the local copy: a price, and when it became true and when we heard.
 * @property {string} sku what it is a price for
 * @property {number} amount the price itself
 * @property {string} changedAt
 *   When the catalogue service says it changed. Comes off the event, ISO-8601.
 * @property {string} appliedAt
 *   When *we* wrote it down, ISO-8601. The gap between the two is Probe A.
 *
 * @typedef {object} PriceStore
 * @property {(sku: string) => Promise<Price|null>} lookup
 *   The price for this SKU, or null if the local copy does not have one.
 * @property {() => Promise<number>} count
 *   How many prices the copy holds. **Zero means "I have never been filled"**, which is not the
 *   same fact as "that SKU is not a thing" and must not produce the same behaviour.
 * @property {() => Promise<string|null>} newestChangedAt
 *   When the catalogue last changed something we know about, or null if the copy is empty.
 *
 *   **This is the number Probe C is really about.** A copy that cannot say how old it is
 *   cannot be monitored, and a copy that cannot be monitored is one you find out about from
 *   a customer.
 */

export {};
