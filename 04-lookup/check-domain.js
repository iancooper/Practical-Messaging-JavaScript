#!/usr/bin/env node
/**
 * Does the domain know what is underneath it?
 *
 *     node check-domain.js
 *
 * In the C# version of these exercises the check is a compile error: leave a package out of the
 * domain project and the build fails until the last type from it is out of the domain too.
 * JavaScript has no compiler to do that for you, so this script does it instead -- it walks
 * every module under model/ and fails if any of them names a library from the banned list.
 *
 * **The list is no longer only about brokers, and that is exercise 4's point.** Exercises 1 to 3
 * kept RabbitMQ and Kafka out of the domain; this one keeps SQLite out of it as well, for
 * exactly the same reason and with exactly the same check. The seam you built in the first half
 * hour, for a broker, turns out to hold against a storage technology nobody had mentioned yet.
 *
 * It looks at all four ways a module can be named: `import ... from`, a bare `import`,
 * `require(...)`, and `import(...)` -- which is also how a type is borrowed inside a JSDoc
 * comment. A handler that says its parameter is an `import('amqplib').GetMessage` has a
 * dependency on the broker whether or not a single byte of amqplib is loaded at run time,
 * and this script counts it.
 *
 * It is the same check, and it is still mechanical rather than a matter of opinion. In exercise 1
 * it ships failing and making it pass is part of the fix; here it already passes, and running it
 * after any change to model/ is how it stays that way.
 */
import { readdirSync, readFileSync, statSync } from 'node:fs';
import { basename, dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const DOMAIN = join(dirname(fileURLToPath(import.meta.url)), 'model');

/**
 * What the domain is not allowed to name. Two brokers and a database, which is a slightly odd
 * list until you notice that it is one rule: **model/ talks to simple-messaging/ and to nothing
 * else.** Everything here is something an outer layer chose and the domain must not inherit.
 *
 * `node:sqlite` is a built-in module rather than a package, and it needs no special handling --
 * it has no '/' in it, so packageOf() hands it back whole and it matches on the nose.
 */
const BANNED_PACKAGES = ['amqplib', '@confluentinc/kafka-javascript', 'node:sqlite'];

/** "a, b or c" -- because "a or b or c" is the sort of thing this repo notices. */
function list(names) {
  return names.length < 2
    ? names.join('')
    : `${names.slice(0, -1).join(', ')} or ${names.at(-1)}`;
}

// `from 'x'`, `import 'x'`, `require('x')` and `import('x')`. This is a text scan rather than
// a parse, because Node ships no parser we could use -- which is close enough for a check
// over a directory this size, and honest about being a smoke alarm rather than a compiler.
const SPECIFIER = /(?:\bfrom|\bimport|\brequire)\s*\(?\s*['"]([^'"]+)['"]/g;

/** The package a specifier belongs to: 'amqplib/callback_api' is still amqplib. */
function packageOf(specifier) {
  const parts = specifier.split('/');
  return specifier.startsWith('@') ? parts.slice(0, 2).join('/') : parts[0];
}

function* jsFilesIn(directory) {
  for (const entry of readdirSync(directory).sort()) {
    const path = join(directory, entry);
    if (statSync(path).isDirectory()) yield* jsFilesIn(path);
    else if (entry.endsWith('.js')) yield path;
  }
}

/** Every module this file names, with the line it is named on. */
function* specifiersIn(path) {
  const lines = readFileSync(path, 'utf8').split('\n');
  for (const [index, line] of lines.entries()) {
    for (const match of line.matchAll(SPECIFIER)) {
      yield { specifier: match[1], line: index + 1 };
    }
  }
}

const offences = [];
for (const path of jsFilesIn(DOMAIN)) {
  for (const { specifier, line } of specifiersIn(path)) {
    if (BANNED_PACKAGES.includes(packageOf(specifier))) {
      offences.push({ path, line, specifier });
    }
  }
}

if (offences.length === 0) {
  console.log(`PASS: nothing under ${basename(DOMAIN)}/ names ${list(BANNED_PACKAGES)}.`);
  console.log('      The domain does not know which broker this is, or where the prices are kept.');
  process.exit(0);
}

console.log('FAIL: the domain names a library that is not its business.\n');
for (const { path, line, specifier } of offences) {
  console.log(`  ${basename(dirname(path))}/${basename(path)}:${line}  names ${specifier}`);
}
console.log(
  '\n  model/ depends on simple-messaging/ -- the gateway contracts -- and on nothing else.' +
  '\n  A broker library in here means the Translate stage has not been done and the broker' +
  '\n  has leaked into your application code. node:sqlite in here means the local copy has' +
  '\n  stopped being a decision somebody made and become a fact about the domain, which is' +
  '\n  the same mistake wearing different clothes. See README.md.');
process.exit(1);
