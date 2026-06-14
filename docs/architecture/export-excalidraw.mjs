// Render a .excalidraw file to SVG + PNG using a headless browser.
//
// Uses @excalidraw/excalidraw (the main package) rather than the legacy
// @excalidraw/utils@0.1.2 — the modern exportToSvg/exportToBlob embed ALL font
// families (incl. the newer Excalifont / Comic Shanns used by the KSTD library)
// as base64 into the output, so operator symbols and legend text render.
//
// One-time setup (this is a Gradle project, so Node deps are not vendored):
//   npm i puppeteer
//   npx puppeteer browsers install chrome
//   # headless Chrome runtime libs (Debian/Ubuntu): sudo apt-get install -y libnss3 libasound2t64
//
// Run (needs a local http origin for the dynamic ESM import):
//   ( cd docs/architecture && python3 -m http.server 8765 ) &
//   node docs/architecture/export-excalidraw.mjs docs/architecture/worktask-streams-topology-kstd.excalidraw
//
// Usage: node export-excalidraw.mjs <path-to.excalidraw>
import fs from "node:fs";
import path from "node:path";
import puppeteer from "puppeteer";

const VER = "0.18.0";
const input = process.argv[2] || "worktask-streams-topology-kstd.excalidraw";
const base = input.replace(/\.excalidraw$/, "");
const json = fs.readFileSync(input, "utf8");

const browser = await puppeteer.launch({
  headless: "new",
  args: ["--no-sandbox", "--disable-setuid-sandbox"],
});
try {
  const page = await browser.newPage();
  page.on("console", (m) => { const t = m.text(); if (/error|fail/i.test(t)) console.log("[page]", t); });
  // A real http origin is required for the dynamic ESM import to resolve.
  await page.goto("http://localhost:8765/", { waitUntil: "domcontentloaded" }).catch(() => {});

  const { svg, png } = await page.evaluate(async (j, ver) => {
    window.EXCALIDRAW_ASSET_PATH = `https://esm.sh/@excalidraw/excalidraw@${ver}/dist/prod/`;
    const mod = await import(`https://esm.sh/@excalidraw/excalidraw@${ver}`);
    const { exportToSvg, exportToBlob } = mod;
    const data = JSON.parse(j);
    const elements = data.elements;
    const appState = { ...data.appState, exportBackground: true, exportEmbedScene: false };
    const files = data.files || {};

    const svgEl = await exportToSvg({ elements, appState, files });
    // give embedded webfonts a moment to resolve before rasterising
    if (document.fonts && document.fonts.ready) await document.fonts.ready;
    const svgStr = svgEl.outerHTML;

    const blob = await exportToBlob({ elements, appState, files, mimeType: "image/png", quality: 1 });
    const pngStr = await new Promise((resolve) => {
      const r = new FileReader();
      r.onloadend = () => resolve(r.result);
      r.readAsDataURL(blob);
    });
    return { svg: svgStr, png: pngStr };
  }, json, VER);

  fs.writeFileSync(`${base}.svg`, svg);
  console.log("wrote", path.basename(`${base}.svg`));
  fs.writeFileSync(`${base}.png`, Buffer.from(png.replace(/^data:image\/png;base64,/, ""), "base64"));
  console.log("wrote", path.basename(`${base}.png`));

  // sanity: warn if the SVG embeds no fonts (would mean text won't render portably)
  if (!/@font-face|data:font|data:application\/font/.test(svg))
    console.log("WARNING: no embedded @font-face found in SVG — fonts may not have embedded");
} finally {
  await browser.close();
}
