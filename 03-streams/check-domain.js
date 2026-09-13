#!/usr/bin/env node
/**
 * Does the domain know which broker this is?
 *
 *     node check-domain.js
 *
 * In the C# version of this exercise the check is a compile error: delete the broker package
 * from the domain project and the build fails until the last broker type is out of it.
 * JavaScript has no compiler to do that for you, so this script does it instead -- it walks
 * every module under model/ and fails if any of them names a broker library.
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
const BROKER_PACKAGES = ['amqplib', '@confluentinc/kafka-javascript'];

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
    if (BROKER_PACKAGES.includes(packageOf(specifier))) {
      offences.push({ path, line, specifier });
    }
  }
}

if (offences.length === 0) {
  console.log(`PASS: nothing under ${basename(DOMAIN)}/ names ${BROKER_PACKAGES.join(' or ')}.`);
  console.log('      The domain does not know which broker this is.');
  process.exit(0);
}

console.log('FAIL: the domain names the broker\'s client library.\n');
for (const { path, line, specifier } of offences) {
  console.log(`  ${basename(dirname(path))}/${basename(path)}:${line}  names ${specifier}`);
}
console.log(
  '\n  A handler is a method over a domain type. If it needs the broker\'s library to' +
  '\n  state its own signature, the Translate stage has not been done -- it has leaked' +
  '\n  into your application code. See PROBE.md.');
process.exit(1);
