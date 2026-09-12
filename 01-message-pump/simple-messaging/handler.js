/**
 * The contract your application code implements so the pump can dispatch to it.
 *
 * JavaScript has no interfaces, so this is a contract written down rather than one the
 * runtime enforces -- which is why the file exports nothing. `export {}` is only there to
 * make it a module.
 *
 * @typedef {object} Handler
 * @property {(delivery: import('amqplib').GetMessage) => Promise<void>} handle
 */

export {};
