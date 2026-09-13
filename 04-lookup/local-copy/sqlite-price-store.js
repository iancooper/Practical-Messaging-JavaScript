/**
 * The local copy of the catalogue's prices, in a SQLite file.
 *
 * **A file is the cheapest durable store there is**, and durable is the only property the
 * exercise actually needs: the price consumer that fills this runs in its own process, and
 * Probes B, C and D all turn on what is still here after that process dies.
 *
 * It implements the `PriceStore` contract that model/ declares, so the domain reads prices
 * without knowing any of this exists. It also exposes {@link SqlitePriceStore#apply}, which
 * the domain does *not* know about: reading the copy is the domain's business, and maintaining
 * it is the price consumer's.
 *
 * **This is the only file in the exercise that names `node:sqlite`**, and `node check-domain.js`
 * is what keeps it that way.
 */
import { dirname, resolve } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { fileURLToPath } from 'node:url';

/** Sits in the exercise directory, so all four processes share one copy. */
export const DEFAULT_PATH = 'prices.db';

// ...which means beside this module's parent, not beside whatever you happened to `cd` into.
const EXERCISE_DIR = dirname(dirname(fileURLToPath(import.meta.url)));

export class SqlitePriceStore {
  #db;

  constructor(db) {
    this.#db = db;
  }

  static open(path = DEFAULT_PATH) {
    const db = new DatabaseSync(resolve(EXERCISE_DIR, path));

    // WAL, because a reader and a writer are two different processes here and the default
    // journal would have them locking each other out. This is a real decision and not
    // boilerplate: "my local copy is a file" stops being free the moment two processes
    // want it at once.
    db.prepare('PRAGMA journal_mode=WAL').get();
    db.exec('PRAGMA busy_timeout=5000');

    // The price is TEXT rather than REAL, on purpose, and JavaScript makes the reason awkward
    // to state honestly: a price is a decimal, SQLite's REAL is a double, and `number` is
    // already that double -- so we cannot claim to be preserving a precision we never had.
    //
    // What TEXT buys is that the *file* is exact. The column holds the digits '9.99', not the
    // nearest double to them waiting to be printed back by somebody else's rounding rules. So
    // the copy is readable as a decimal by anything that has one, which is the same column the
    // other language repos chose for the same reason -- and storing money in a float, a bug
    // that takes months to surface, stays out of the durable copy at least.
    db.exec(`
      CREATE TABLE IF NOT EXISTS prices (
        sku        TEXT PRIMARY KEY,
        price      TEXT NOT NULL,
        changed_at TEXT NOT NULL,
        applied_at TEXT NOT NULL
      )`);

    return new SqlitePriceStore(db);
  }

  // node:sqlite is synchronous -- these are file reads, measured in microseconds, and there is
  // nothing to wait for. They are `async` because the contract model/ declares is `async`, and
  // that is the right way round: the domain should not have to be rewritten the day somebody
  // puts this behind something that genuinely does wait. (If they do, read the README again
  // about what a network call inside the handler costs you.)

  /** @returns {Promise<import('../model/price-store.js').Price|null>} */
  async lookup(sku) {
    const row = this.#db
      .prepare('SELECT price, changed_at, applied_at FROM prices WHERE sku = $sku')
      .get({ sku });

    if (row === undefined) return null;

    return {
      sku,
      amount: Number(row.price),
      changedAt: row.changed_at,
      appliedAt: row.applied_at,
    };
  }

  async count() {
    return this.#db.prepare('SELECT COUNT(*) AS n FROM prices').get().n;
  }

  async newestChangedAt() {
    // MAX over ISO-8601 text is MAX over the instants, because ISO-8601 in UTC sorts the way
    // it reads. That is not luck; it is why the format is worth insisting on.
    return this.#db.prepare('SELECT MAX(changed_at) AS newest FROM prices').get().newest ?? null;
  }

  /**
   * Write a price change into the copy. **Last writer wins**, which is only safe because
   * a PriceChanged is a snapshot -- run this twice with the same event and the row ends up
   * the same. That is Probe D's whole payout, and it was decided in step 1.
   *
   * @returns {Promise<string>} when the row was written, for Probe A's arithmetic.
   */
  async apply(event) {
    const appliedAt = new Date().toISOString();

    this.#db.prepare(`
      INSERT INTO prices (sku, price, changed_at, applied_at)
      VALUES ($sku, $price, $changed_at, $applied_at)
      ON CONFLICT(sku) DO UPDATE SET
        price      = excluded.price,
        changed_at = excluded.changed_at,
        applied_at = excluded.applied_at
    `).run({
      sku: event.sku,
      price: String(event.price),
      changed_at: event.changedAt,
      applied_at: appliedAt,
    });

    return appliedAt;
  }

  close() {
    this.#db.close();
  }
}
