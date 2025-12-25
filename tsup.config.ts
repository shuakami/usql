import { defineConfig } from "tsup";

export default defineConfig({
  entry: ["src/index.ts"],
  format: ["cjs"],
  target: "node18",
  sourcemap: true,
  clean: true,
  dts: false,
  splitting: false,
  treeshake: true,
  banner: {
    js: "#!/usr/bin/env node"
  }
});
