/**
 * Anything we are willing to put on a channel.
 *
 * The id is the message's identity, not the entity's -- we need it to key a stream,
 * and later to answer "have I seen this before?".
 *
 * JavaScript has no interfaces, so this is a contract written down rather than one the
 * runtime enforces -- which is why the file exports nothing; `export {}` is only there to
 * make it a module. It is still the contract: the gateway assumes an `id` and nothing else.
 *
 * @typedef {object} Message
 * @property {string} id
 */

export {};
