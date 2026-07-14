/**
 * Register the .js → .ts loader hook for integration tests.
 *
 * Usage: node --experimental-strip-types --import ./test/support/register-loader.mjs --test test/integration/*.test.ts
 *
 * Handles two issues:
 * 1. Source files use .js import extensions (TypeScript ESM convention) but
 *    files on disk are .ts — the loader rewrites .js → .ts at resolve time.
 * 2. Node 26's strip-types mode supports the source syntax used by this suite;
 *    the removed --experimental-transform-types flag must not be passed.
 */

import { register } from "node:module";

register(new URL("./ts-loader.mjs", import.meta.url));
