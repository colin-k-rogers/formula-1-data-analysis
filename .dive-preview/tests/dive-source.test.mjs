// Guards every dives/<name>/src/dive.tsx against the ways a Dive can compile
// locally but still fail once deployed — most importantly the deployer's
// single-line REQUIRED_DATABASES strip (see DEPLOYER_STRIP below).
import assert from "node:assert/strict";
import { readdirSync, readFileSync, statSync } from "node:fs";
import path from "node:path";
import { describe, test } from "node:test";
import { fileURLToPath } from "node:url";
import esbuild from "esbuild";

const REPO_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..", "..");
const DIVES_DIR = path.join(REPO_ROOT, "dives");

// Byte-identical to the regex the Blueprints deployer applies to a Dive's
// source before uploading it (md_blueprints/deploy.py, `_deploy_dive`): it
// drops the declaration's *line*, because blueprint.yml's requiredResources
// is what MotherDuck actually mounts. A declaration spanning several lines
// therefore leaves its array body behind as a stray statement.
const DEPLOYER_STRIP = /export const REQUIRED_DATABASES[^\n]*\n/g;

const DECLARATION_PREFIX = "export const REQUIRED_DATABASES";

function diveSources() {
  return readdirSync(DIVES_DIR)
    .map((name) => ({ name, file: path.join(DIVES_DIR, name, "src", "dive.tsx") }))
    .filter(({ file }) => statSync(file, { throwIfNoEntry: false })?.isFile());
}

/** esbuild's own errors carry the location but not the offending source line,
 * which is the first thing you want when a test like this fails. */
function compileTsx(source, label) {
  try {
    esbuild.transformSync(source, { loader: "tsx", sourcefile: label });
  } catch (err) {
    const [first] = err.errors ?? [];
    if (!first) throw err;
    const { line, column, lineText } = first.location ?? {};
    assert.fail(`${label}:${line}:${column} ${first.text}\n  ${lineText ?? ""}`);
  }
}

const sources = diveSources();

test("dives/ contains at least one dive source", () => {
  assert.ok(sources.length > 0, `no src/dive.tsx found under ${DIVES_DIR}`);
});

for (const { name, file } of sources) {
  describe(`dives/${name}`, () => {
    const source = readFileSync(file, "utf8");

    test("compiles as TSX", () => {
      compileTsx(source, `dives/${name}/src/dive.tsx`);
    });

    test("declares REQUIRED_DATABASES on a single line", () => {
      const line = source.split("\n").find((l) => l.startsWith(DECLARATION_PREFIX));
      assert.ok(line, `no \`${DECLARATION_PREFIX}\` declaration found`);
      // A wrapped declaration doesn't parse on its own — which is exactly the
      // fragment the deployer leaves behind after stripping this line.
      compileTsx(line, `dives/${name}/src/dive.tsx (REQUIRED_DATABASES line)`);
    });

    test("still compiles after the deployer strips REQUIRED_DATABASES", () => {
      compileTsx(source.replace(DEPLOYER_STRIP, ""), `dives/${name}/src/dive.tsx (deployed)`);
    });

    test("exports a default component and REQUIRED_DATABASES", async () => {
      // Both are re-exported by `make preview`'s generated shim, so a missing
      // one breaks local preview even though the deployed Dive only needs
      // the default export.
      const built = await esbuild.build({
        stdin: { contents: source, loader: "tsx", sourcefile: `dives/${name}/src/dive.tsx` },
        bundle: true,
        external: ["*"],
        format: "esm",
        write: false,
        metafile: true,
        logLevel: "silent",
      });
      const exports = Object.values(built.metafile.outputs).flatMap((o) => o.exports);
      assert.deepEqual([...exports].sort(), ["REQUIRED_DATABASES", "default"]);
    });
  });
}
