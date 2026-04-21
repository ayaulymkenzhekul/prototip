const fs = require('fs');
const path = require('path');

const root = path.resolve(__dirname, '..');
const outDir = path.join(root, 'data');
const detailDir = path.join(outDir, 'rates-details');
const sourceFile = path.join(root, 'smeta.csv');
const materialFile = path.join(root, 'material.csv');

function ensureDir(dir) {
  fs.mkdirSync(dir, { recursive: true });
}

function forEachCsvRow(text, onRow) {
  let row = [];
  let field = '';
  let inQuotes = false;

  for (let i = 0; i < text.length; i += 1) {
    const ch = text[i];
    const next = text[i + 1];

    if (ch === '"') {
      if (!inQuotes && field === '') {
        inQuotes = true;
        continue;
      }
      if (inQuotes && (next === '\n' || next === '\r' || next === undefined || (next === ',' && isBoundaryComma(text, i)))) {
        inQuotes = false;
        continue;
      }
      field += ch;
      continue;
    }

    if (ch === ',' && !inQuotes) {
      row.push(field);
      field = '';
      continue;
    }

    if ((ch === '\n' || ch === '\r') && !inQuotes) {
      if (ch === '\r' && next === '\n') i += 1;
      if (field.length || row.length) {
        row.push(field);
        onRow(row);
      }
      row = [];
      field = '';
      continue;
    }

    field += ch;
  }

  if (field.length || row.length) {
    row.push(field);
    onRow(row);
  }
}

function isBoundaryComma(text, quoteIndex) {
  let j = quoteIndex + 2;
  while (text[j] === ' ') j += 1;
  return text[j] !== "'";
}

function pickLang(raw, key, fallback = '') {
  if (!raw) return fallback;
  const marker = `'"${key}'":'"`;
  const start = raw.indexOf(marker);
  if (start === -1) return fallback;
  const valueStart = start + marker.length;
  const end = raw.indexOf('\'"', valueStart);
  if (end === -1) return fallback;
  return cleanup(raw.slice(valueStart, end));
}

function cleanup(value) {
  return String(value || '')
    .replace(/\\n/g, '\n')
    .replace(/\\r/g, '')
    .replace(/\\"/g, '"')
    .trim();
}

function parseResources(raw) {
  if (!raw) return [];
  const resources = [];
  const re = /\{([\s\S]*?)\}/g;
  let match;
  while ((match = re.exec(raw))) {
    const block = match[1];
    const code = pickLang(block, 'CDE');
    const name = pickLang(block, 'NRU') || pickLang(block, 'NKZ');
    if (!code || !name) continue;
    const type = code.startsWith('1-') || code === '0990100' ? 'труд' : 'ресурс';
    resources.push({ t: type, name, norm: code });
  }
  return resources;
}

function sectionFromCode(code) {
  const prefix = String(code || '').split('-')[0];
  const n = Number(prefix);
  if (n >= 1100 && n < 1200) return 'str';
  if (n >= 1200 && n < 1300) return 'rem';
  if (n >= 2000 && n < 3000) return 'mont';
  return 'rsrk';
}

function chunkFromCode(code) {
  return String(code || 'misc').split('-')[0] || 'misc';
}

function build() {
  ensureDir(detailDir);

  const text = fs.readFileSync(sourceFile, 'utf8');
  const index = [];
  const chunkState = new Map();

  function appendDetail(chunk, code, detail) {
    const file = path.join(detailDir, `${chunk}.json`);
    let state = chunkState.get(chunk);
    if (!state) {
      fs.writeFileSync(file, '{', 'utf8');
      state = { count: 0 };
      chunkState.set(chunk, state);
    }
    const prefix = state.count ? ',' : '';
    fs.appendFileSync(file, `${prefix}${JSON.stringify(code)}:${JSON.stringify(detail)}`, 'utf8');
    state.count += 1;
  }

  let rowCount = 0;
  forEachCsvRow(text, (cols) => {
    rowCount += 1;
    if (cols.length < 3) return;
    const code = cleanup(cols[1]);
    if (!code) return;

    const name = pickLang(cols[2], 'NRU') || pickLang(cols[2], 'NKZ') || code;
    const unit = pickLang(cols[5], 'SRU') || pickLang(cols[5], 'SKZ') || 'ед.';
    const section = sectionFromCode(code);
    const chunk = chunkFromCode(code);
    const resources = parseResources(cols[3]);
    const comp = pickLang(cols[4], 'WRU') || pickLang(cols[4], 'WKZ') || name;
    const detail = {
      code,
      section,
      name,
      unit,
      comp,
      res: resources,
      mats: resources
        .filter((r) => !String(r.t).includes('труд'))
        .slice(0, 24)
        .map((r) => ({ name: r.name, unit: '', norm: r.norm })),
      coefs: 'РСН РК',
      price: 'нет цены',
      ai: false,
      conf: 0
    };

    index.push({
      code,
      section,
      name,
      unit,
      chunk
    });

    appendDetail(chunk, code, detail);

    if (rowCount % 10000 === 0) {
      process.stdout.write(`processed ${rowCount}\r`);
    }
  });

  fs.writeFileSync(
    path.join(outDir, 'rates-index.json'),
    JSON.stringify({ generatedAt: new Date().toISOString(), count: index.length, items: index }),
    'utf8'
  );

  chunkState.forEach((_, chunk) => {
    fs.appendFileSync(path.join(detailDir, `${chunk}.json`), '}', 'utf8');
  });

  console.log(`\nBuilt ${index.length} rates into ${chunkState.size} detail chunks.`);
}

function buildMaterials() {
  if (!fs.existsSync(materialFile)) return;
  const text = fs.readFileSync(materialFile, 'utf8');
  const items = [];

  forEachCsvRow(text, (cols) => {
    if (cols.length < 3) return;
    const code = cleanup(cols[1]);
    if (!code) return;
    const name = pickLang(cols[2], 'NRU') || pickLang(cols[2], 'NKZ') || code;
    const unit = pickLang(cols[5], 'SRU') || pickLang(cols[5], 'SKZ') || 'ед.';
    const section = chunkFromCode(code);
    items.push({ code, name, unit, section });
  });

  fs.writeFileSync(
    path.join(outDir, 'materials-index.json'),
    JSON.stringify({ generatedAt: new Date().toISOString(), count: items.length, items }),
    'utf8'
  );
  console.log(`Built ${items.length} materials.`);
}

build();
buildMaterials();
