const http = require('http');
const fs = require('fs');
const path = require('path');

const PORT = Number(process.env.PORT || 3001);
const UPSTREAM_URL = process.env.CA_UPSTREAM_URL || 'https://ai-test.erg.kz/api/assistant-core/chats/ask-assistant';
const ASSISTANT_ID = Number(process.env.CA_ASSISTANT_ID || 6500);
const AUTH_TOKEN = process.env.CA_AUTH_TOKEN || '';
const AUTH_HEADER = process.env.CA_AUTH_HEADER || 'Authorization';
const AUTH_SCHEME = process.env.CA_AUTH_SCHEME || 'Bearer';
const COOKIE_HEADER = process.env.CA_COOKIE || '';
const ORIGIN_HEADER = process.env.CA_ORIGIN || 'https://ai-test.erg.kz';
const REFERER_HEADER = process.env.CA_REFERER || 'https://ai-test.erg.kz/api/assistant-core/swagger/index.html';
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

function send(res, status, body, headers = {}) {
  res.writeHead(status, {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type,Accept',
    ...headers
  });
  res.end(body);
}

function sendJson(res, status, data) {
  send(res, status, JSON.stringify(data), { 'Content-Type': 'application/json; charset=utf-8' });
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

async function handleAssistant(req, res) {
  try {
    const raw = await readBody(req);
    const input = raw ? JSON.parse(raw) : {};
    const message = String(input.message || '').trim();
    if (!message) {
      sendJson(res, 400, { ok: false, error: 'message is required' });
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

    sendJson(res, upstreamResponse.ok ? 200 : upstreamResponse.status, {
      ok: upstreamResponse.ok,
      upstreamStatus: upstreamResponse.status,
      data
    });
  } catch (error) {
    const cause = error && error.cause && error.cause.message ? error.cause.message : undefined;
    sendJson(res, 502, {
      ok: false,
      error: cause ? `${error && error.message ? error.message : 'Assistant proxy error'}: ${cause}` : (error && error.message ? error.message : 'Assistant proxy error'),
      cause
    });
  }
}

function handleStatic(req, res) {
  const full = safeStaticPath(req.url || '/');
  if (!full) {
    send(res, 403, 'Forbidden', { 'Content-Type': 'text/plain; charset=utf-8' });
    return;
  }
  fs.readFile(full, (error, content) => {
    if (error) {
      send(res, 404, 'Not found', { 'Content-Type': 'text/plain; charset=utf-8' });
      return;
    }
    const type = mimeTypes[path.extname(full).toLowerCase()] || 'application/octet-stream';
    send(res, 200, content, { 'Content-Type': type });
  });
}

const server = http.createServer((req, res) => {
  if (req.method === 'OPTIONS') {
    send(res, 204, '');
    return;
  }
  if (req.url && req.url.startsWith('/api/assistant-status')) {
    sendJson(res, 200, getProxyStatus());
    return;
  }
  if (req.url && req.url.startsWith('/api/defect-assistant')) {
    if (req.method !== 'POST') {
      sendJson(res, 405, { ok: false, error: 'Method not allowed' });
      return;
    }
    handleAssistant(req, res);
    return;
  }
  handleStatic(req, res);
});

server.listen(PORT, '127.0.0.1', () => {
  console.log(`Prototype running at http://127.0.0.1:${PORT}`);
  console.log(`Assistant proxy: POST http://127.0.0.1:${PORT}/api/defect-assistant`);
});
