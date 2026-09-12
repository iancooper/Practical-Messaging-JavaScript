/**
 * The contract your application code implements so the pump can dispatch to it.
 *
 * A domain type in, and nothing else. No delivery, no channel, no headers, no ack. The
 * handler does not know the pump exists, which is exactly why a test can call it too.
 *
 * JavaScript has no interfaces, so this is a contract written down rather than one the
 * runtime enforces -- which is why the file exports nothing. `export {}` is only there to
 * make it a module.
 *
 * @typedef {object} Handler
 * @property {(message: import('./message.js').Message) => Promise<void>} handle
 */

export {};
