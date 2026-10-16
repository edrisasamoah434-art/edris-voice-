// EDRIS voice relay: a tiny always-on WebSocket server for Twilio ConversationRelay.
// Twilio does the listening (Deepgram) and speaking (streaming TTS) and handles barge-in.
// For each finished sentence from Edward, this relay asks edrisai.com for EDRIS's reply and
// streams her words back to Twilio as they arrive, so she starts speaking in well under a second.
// It holds no API keys: just RELAY_SECRET (shared with edrisai.com) and EDRIS_URL.
// Every call carries a one-time token that edrisai.com only issues after Edward's sign-in code.
const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = Number(process.env.PORT) || 10000;
const EDRIS = (process.env.EDRIS_URL || 'https://edrisai.com').replace(/\/+$/, '');
const SECRET = process.env.RELAY_SECRET || '';
const log = (...a) => console.log(new Date().toISOString(), ...a);

const server = http.createServer((req, res) => {
  if (req.url === '/health' || req.url === '/') { res.writeHead(200, { 'content-type': 'text/plain' }); return res.end('ok'); }
  res.writeHead(404); res.end();
});
const wss = new WebSocketServer({ noServer: true, maxPayload: 256 * 1024 });
server.on('upgrade', (req, socket, head) => {
  if (!req.url.startsWith('/ws')) { socket.destroy(); return; }
  wss.handleUpgrade(req, socket, head, (ws) => wss.emit('connection', ws, req));
});

async function edris(payload, signal) {
  return fetch(EDRIS + '/api/voice', {
    method: 'POST', signal,
    headers: { 'content-type': 'application/json', 'x-relay-secret': SECRET },
    body: JSON.stringify({ op: 'relay', ...payload }),
  });
}

wss.on('connection', (ws) => {
  let token = '', callSid = '', busy = null, spokenPrev, ended = false;
  const latency = [];
  const send = (o) => { if (ws.readyState === 1) ws.send(JSON.stringify(o)); };

  async function turn(text) {
    if (busy) busy.abort();
    const ctl = new AbortController(); busy = ctl;
    const t0 = Date.now(); let first = 0, out = '';
    const prev = spokenPrev; spokenPrev = undefined;
    try {
      const r = await edris({ action: 'turn', token, text, spokenPrev: prev }, ctl.signal);
      if (r.status === 423) { send({ type: 'text', token: 'EDRIS is on emergency stop. Release it in the app first. Goodbye.', last: true }); return hangSoon(); }
      if (!r.ok || !r.body) throw new Error('edris ' + r.status);
      const dec = new TextDecoder(); let buf = '', bye = false;
      for await (const chunk of r.body) {
        buf += dec.decode(chunk, { stream: true });
        let i;
        while ((i = buf.indexOf('\n')) > -1) {
          const line = buf.slice(0, i); buf = buf.slice(i + 1);
          if (!line.trim()) continue;
          let m; try { m = JSON.parse(line); } catch (e) { continue; }
          if (typeof m.t === 'string' && m.t) {
            if (!first) { first = Date.now() - t0; latency.push(first); log('call', callSid, 'first words in', first, 'ms'); }
            out += m.t; send({ type: 'text', token: m.t, last: false });
          }
          if (m.done) bye = !!m.bye;
        }
      }
      send({ type: 'text', token: '', last: true });
      if (bye) hangSoon();
    } catch (e) {
      if (ctl.signal.aborted) return;          // he interrupted; a new turn takes over
      log('turn error', callSid, e.message);
      send({ type: 'text', token: out ? '' : 'Sorry, I lost my train of thought. Could you say that again?', last: true });
    } finally { if (busy === ctl) busy = null; }
  }
  // Let her goodbye finish playing, then end the call.
  function hangSoon() { setTimeout(() => send({ type: 'end' }), 3500); }

  ws.on('message', (raw) => {
    let m; try { m = JSON.parse(raw.toString()); } catch (e) { return; }
    if (m.type === 'setup') {
      token = String((m.customParameters && m.customParameters.token) || '');
      callSid = String(m.callSid || '');
      log('call start', callSid);
      if (!/^[0-9a-f]{48}$/.test(token)) { log('no token, closing'); send({ type: 'end' }); }
      return;
    }
    if (m.type === 'prompt') {
      if (m.last === false) return;            // wait for the finished sentence
      const text = String(m.voicePrompt || '').trim();
      if (text) turn(text);
      return;
    }
    if (m.type === 'interrupt') {
      // He started talking over her: stop, and remember only what she actually said.
      if (busy) busy.abort();
      spokenPrev = String(m.utteranceUntilInterrupt || '');
      return;
    }
    if (m.type === 'error') log('twilio error', callSid, m.description || '');
  });

  ws.on('close', () => {
    if (ended) return; ended = true;
    if (busy) busy.abort();
    log('call end', callSid, 'turns', latency.length, latency.length ? 'median first words ' + latency.slice().sort((a, b) => a - b)[Math.floor(latency.length / 2)] + ' ms' : '');
    if (token) edris({ action: 'end', token, latency }).catch(() => {});
  });
  ws.on('error', () => {});
});

if (!SECRET || SECRET.length < 24) log('WARNING: RELAY_SECRET is missing or short; edrisai.com will refuse every call.');
server.listen(PORT, () => log('EDRIS voice relay listening on', PORT));
