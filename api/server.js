const http = require('http');
const fs = require('fs');
const path = require('path');

const PORT = Number(process.env.PORT || 3001);
const HOST = process.env.HOST || '0.0.0.0';
const UPSTREAM_URL = process.env.CA_UPSTREAM_URL || 'https://ai-test.erg.kz/api/assistant-core/chats/ask-assistant';
const ASSISTANT_ID = Number(process.env.CA_ASSISTANT_ID || 6500);
const AUTH_TOKEN = process.env.CA_AUTH_TOKEN || '';
const AUTH_HEADER = process.env.CA_AUTH_HEADER || 'Authorization';
const AUTH_SCHEME = process.env.CA_AUTH_SCHEME || 'Bearer';
const COOKIE_HEADER = process.env.CA_COOKIE || '';
const ORIGIN_HEADER = process.env.CA_ORIGIN || 'https://ai-test.erg.kz';
const REFERER_HEADER = process.env.CA_REFERER || 'https://ai-test.erg.kz/api/assistant-core/swagger/index.html';
const ALLOWED_ORIGINS = (process.env.CA_ALLOWED_ORIGINS || '*')
  .split(',')
  .map(item => item.trim())
  .filter(Boolean);
const ROOT = path.resolve(__dirname, '..');

if (process.env.CA_INSECURE_TLS === '1') {
  process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';
}

const mimeTypes = {
  '.html': 'text/html; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.js': 'application/javascript; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.jpeg': 'image/jpeg',
  '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon'
};

function getCorsOrigin(req) {
  if (ALLOWED_ORIGINS.includes('*')) return '*';
  const requestOrigin = req && req.headers ? req.headers.origin : '';
  if (requestOrigin && ALLOWED_ORIGINS.includes(requestOrigin)) return requestOrigin;
  return ALLOWED_ORIGINS[0] || '*';
}

function send(req, res, status, body, headers = {}) {
  res.writeHead(status, {
    'Access-Control-Allow-Origin': getCorsOrigin(req),
    'Vary': 'Origin',
    'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type,Accept',
    ...headers
  });
  res.end(body);
}

function sendJson(req, res, status, data) {
  send(req, res, status, JSON.stringify(data), { 'Content-Type': 'application/json; charset=utf-8' });
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    let body = '';
    req.on('data', chunk => {
      body += chunk;
      if (body.length > 1_000_000) {
        reject(new Error('Request body is too large'));
        req.destroy();
      }
    });
    req.on('end', () => resolve(body));
    req.on('error', reject);
  });
}

function safeStaticPath(urlPath) {
  const decoded = decodeURIComponent(urlPath.split('?')[0]);
  const requested = decoded === '/' ? '/index.html' : decoded;
  const full = path.resolve(ROOT, `.${requested}`);
  if (!full.startsWith(ROOT)) return null;
  return full;
}

function getAuthHeaderValue() {
  if (!AUTH_TOKEN) return '';
  const trimmed = String(AUTH_TOKEN).trim();
  if (!AUTH_SCHEME) return trimmed;
  if (/^(Bearer|Basic)\s+/i.test(trimmed)) return trimmed;
  return `${AUTH_SCHEME} ${trimmed}`;
}

function getProxyStatus() {
  return {
    ok: true,
    upstreamUrl: UPSTREAM_URL,
    assistantId: ASSISTANT_ID,
    hasCookie: Boolean(COOKIE_HEADER),
    cookieLength: COOKIE_HEADER.length,
    hasAuth: Boolean(AUTH_TOKEN),
    authHeader: AUTH_HEADER,
    authScheme: AUTH_SCHEME,
    authLength: AUTH_TOKEN.length,
    origin: ORIGIN_HEADER,
    referer: REFERER_HEADER
  };
}

function normalizeGroup(row) {
  const id = String(row.id || row.OBOSNOV || row.obosnov || '').trim();
  const parentRaw = row.parentId || row.PARENT || row.parent || null;
  const parentValue = parentRaw === null || parentRaw === undefined ? '' : String(parentRaw).trim();
  const parentId = !parentValue || parentValue.toUpperCase() === 'NULL' ? null : parentValue;
  const level = Number(row.level || row.LEVEL || 0);
  return {
    id,
    nameRu: String(row.nameRu || row.RU || row.ru || id).trim(),
    nameKz: String(row.nameKz || row.KZ || row.kz || '').trim(),
    level,
    parentId: parentId || null
  };
}

function buildGroupTree(rows) {
  const normalized = rows
    .map(normalizeGroup)
    .filter(group => group.id && (group.level === 1 || group.level === 2));
  const byId = new Map();
  normalized.forEach(group => byId.set(group.id, { ...group, children: [] }));
  const roots = [];
  byId.forEach(group => {
    const parent = group.parentId ? byId.get(group.parentId) : null;
    if (parent && group.level > parent.level) {
      parent.children.push(group);
    } else {
      roots.push(group);
    }
  });
  const sortGroups = list => {
    list.sort((a, b) => String(a.id).localeCompare(String(b.id), 'ru'));
    list.forEach(item => sortGroups(item.children));
  };
  sortGroups(roots);
  return { flat: normalized, tree: roots };
}

function readGroupsFile() {
  const full = path.join(ROOT, 'data', 'groups.json');
  if (!fs.existsSync(full)) {
    return [];
  }
  const raw = fs.readFileSync(full, 'utf8').replace(/^\uFEFF/, '');
  const parsed = JSON.parse(raw);
  return Array.isArray(parsed) ? parsed : (Array.isArray(parsed.groups) ? parsed.groups : []);
}

function handleGroups(req, res) {
  try {
    const url = new URL(req.url || '/api/groups', `http://${req.headers.host || 'localhost'}`);
    const format = url.searchParams.get('format') || 'tree';
    const result = buildGroupTree(readGroupsFile());
    sendJson(req, res, 200, {
      ok: true,
      source: 'data/groups.json',
      count: result.flat.length,
      groups: format === 'flat' ? result.flat : result.tree
    });
  } catch (error) {
    sendJson(req, res, 500, {
      ok: false,
      error: error && error.message ? error.message : 'Failed to read groups'
    });
  }
}

async function handleAssistant(req, res) {
  try {
    const raw = await readBody(req);
    const input = raw ? JSON.parse(raw) : {};
    const message = String(input.message || '').trim();
    if (!message) {
      sendJson(req, res, 400, { ok: false, error: 'message is required' });
      return;
    }

    const upstreamPayload = {
      assistantId: Number(input.assistantId || ASSISTANT_ID),
      message,
      utcOffset: Number(input.utcOffset || 0),
      agentMode: Boolean(input.agentMode || false),
      contextId: Number(input.contextId || 0),
      useInternet: Boolean(input.useInternet || false),
      problemRouter: Boolean(input.problemRouter || false)
    };

    const upstreamHeaders = {
      accept: 'application/json',
      'Content-Type': 'application/json',
      origin: ORIGIN_HEADER,
      referer: REFERER_HEADER
    };
    const authValue = getAuthHeaderValue();
    if (authValue) {
      upstreamHeaders[AUTH_HEADER] = authValue;
    }
    if (COOKIE_HEADER) {
      upstreamHeaders.Cookie = COOKIE_HEADER;
    }

    const upstreamResponse = await fetch(UPSTREAM_URL, {
      method: 'POST',
      headers: upstreamHeaders,
      body: JSON.stringify(upstreamPayload)
    });

    const text = await upstreamResponse.text();
    let data;
    try {
      data = text ? JSON.parse(text) : null;
    } catch {
      data = { raw: text };
    }

    sendJson(req, res, upstreamResponse.ok ? 200 : upstreamResponse.status, {
      ok: upstreamResponse.ok,
      upstreamStatus: upstreamResponse.status,
      data
    });
  } catch (error) {
    const cause = error && error.cause && error.cause.message ? error.cause.message : undefined;
    sendJson(req, res, 502, {
      ok: false,
      error: cause ? `${error && error.message ? error.message : 'Assistant proxy error'}: ${cause}` : (error && error.message ? error.message : 'Assistant proxy error'),
      cause
    });
  }
}

function handleStatic(req, res) {
  const full = safeStaticPath(req.url || '/');
  if (!full) {
    send(req, res, 403, 'Forbidden', { 'Content-Type': 'text/plain; charset=utf-8' });
    return;
  }
  fs.readFile(full, (error, content) => {
    if (error) {
      send(req, res, 404, 'Not found', { 'Content-Type': 'text/plain; charset=utf-8' });
      return;
    }
    const type = mimeTypes[path.extname(full).toLowerCase()] || 'application/octet-stream';
    send(req, res, 200, content, { 'Content-Type': type });
  });
}

const server = http.createServer((req, res) => {
  if (req.method === 'OPTIONS') {
    send(req, res, 204, '');
    return;
  }
  if (req.url && req.url.startsWith('/api/assistant-status')) {
    sendJson(req, res, 200, getProxyStatus());
    return;
  }
  if (req.url && req.url.startsWith('/api/groups')) {
    if (req.method !== 'GET') {
      sendJson(req, res, 405, { ok: false, error: 'Method not allowed' });
      return;
    }
    handleGroups(req, res);
    return;
  }
  if (req.url && req.url.startsWith('/api/defect-assistant')) {
    if (req.method !== 'POST') {
      sendJson(req, res, 405, { ok: false, error: 'Method not allowed' });
      return;
    }
    handleAssistant(req, res);
    return;
  }
  handleStatic(req, res);
});

server.listen(PORT, HOST, () => {
  const visibleHost = HOST === '0.0.0.0' ? '127.0.0.1' : HOST;
  console.log(`Prototype running at http://${visibleHost}:${PORT}`);
  console.log(`Assistant proxy: POST http://${visibleHost}:${PORT}/api/defect-assistant`);
});
