// Chụp Spark UI cho slide: node ui_shots.js <outdir> <jobs.json>
// jobs.json: [{name, url, clickText?, selector?, width?, height?}]
const puppeteer = require('C:/Program Files/nodejs/node_modules/@mermaid-js/mermaid-cli/node_modules/puppeteer');
const fs = require('fs');
(async () => {
  const [outdir, spec] = process.argv.slice(2);
  const shots = JSON.parse(fs.readFileSync(spec, 'utf8'));
  const browser = await puppeteer.launch({
    executablePath: 'C:/Program Files/Google/Chrome/Application/chrome.exe',
    args: ['--no-sandbox'],
  });
  const page = await browser.newPage();
  for (const s of shots) {
    await page.setViewport({ width: s.width || 1400, height: s.height || 900, deviceScaleFactor: 2 });
    await page.goto(s.url, { waitUntil: 'networkidle0' });
    if (s.clickText) {
      await page.evaluate((t) => {
        const a = [...document.querySelectorAll('a, span')].find(e => e.textContent.trim() === t);
        if (a) a.click();
      }, s.clickText);
      await new Promise(r => setTimeout(r, 2500));
    }
    const opts = { path: `${outdir}/${s.name}.png` };
    if (s.selector) {
      const el = await page.$(s.selector);
      if (el) { await el.screenshot(opts); console.log('ok', s.name); continue; }
    }
    if (s.clip) opts.clip = s.clip;
    await page.screenshot(opts);
    console.log('ok', s.name);
  }
  await browser.close();
})();
