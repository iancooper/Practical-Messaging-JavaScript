/**
 * The Translate stage for this channel: a body becomes a {@link PlaceOrder}, or it
 * does not and we say so clearly.
 *
 * Note what it does *not* do: it does not log, it does not decide anything, and it does not
 * know a broker exists. It converts, or it throws.
 */
import { UnmappableMessageError } from '../simple-messaging/message-mapper.js';
import { PlaceOrder } from './place-order.js';

export class PlaceOrderMapper {
  mapToRequest(body) {
    try {
      return PlaceOrder.deserialize(body);
    } catch (e) {
      // Translate the deserializer's complaint into the gateway's vocabulary. The pump
      // should not have to know we chose JSON.
      throw new UnmappableMessageError(`Body is not a PlaceOrder: ${e.message}`, { cause: e });
    }
  }
}
