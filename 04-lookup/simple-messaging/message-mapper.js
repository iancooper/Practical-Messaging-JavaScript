/**
 * The Translate stage: a message body on the wire becomes a domain object.
 *
 * This is the seam. Everything on the broker side of it is the gateway's business;
 * everything on the other side is yours.
 *
 * It throws when the body is not the type this channel carries. That throw is a *different
 * kind of failure* from one thrown by a handler, and the pump has to treat it differently --
 * which is most of this exercise.
 *
 * @typedef {object} MessageMapper
 * @property {(body: string) => import('./message.js').Message} mapToRequest
 *   Throws {@link UnmappableMessageError} when the body is not a T and never will be.
 */

/**
 * "I was handed this message and I cannot read it."
 *
 * The gateway raises this so the pump does not have to know whether the body was JSON, XML
 * or protobuf. Retrying it is pointless: the bytes will not change.
 */
export class UnmappableMessageError extends Error {
  constructor(message, options) {
    super(message, options);
    this.name = 'UnmappableMessageError';
  }
}
