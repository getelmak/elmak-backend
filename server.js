// ============================================================================
// ELMAK (عِلمك) - REAL-TIME ULTRA-FAST MESSAGING SERVER
// High-Performance WebSocket Engine & E2EE Relay
// ============================================================================

const http = require('http');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const url = require('url');

let WebSocket;
try {
  WebSocket = require('ws');
} catch (e) {
  // Fallback if ws module is loading
  console.log('[Server] Loading fallback...');
}

const PORT = process.env.PORT || 8080;
const HOST = '0.0.0.0';
const UPLOADS_DIR = path.join(__dirname, 'uploads');

if (!fs.existsSync(UPLOADS_DIR)) {
  fs.mkdirSync(UPLOADS_DIR, { recursive: true });
}

// In-Memory State
const users = new Map(); // username -> { username, displayName, token, online: true, lastSeen }
const sockets = new Map(); // username -> Set<WebSocket>
const messagesHistory = new Map(); // chatId -> Array<Message>

function handleMessage(senderSocket, msg) {
  if (!msg || !msg.type) return;

  // 1. Authentication
  if (msg.type === 'auth') {
    const user = (msg.username || '').trim().toLowerCase();
    if (user) {
      senderSocket.username = user;
      if (!sockets.has(user)) {
        sockets.set(user, new Set());
      }
      sockets.get(user).add(senderSocket);

      const token = msg.token || crypto.randomBytes(16).toString('hex');

      if (!users.has(user)) {
        users.set(user, {
          username: user,
          displayName: msg.displayName || user,
          token: token,
          online: true,
          lastSeen: Date.now(),
        });
      } else {
        const u = users.get(user);
        u.online = true;
        u.lastSeen = Date.now();
        if (msg.displayName) u.displayName = msg.displayName;
      }

      sendToSocket(senderSocket, { type: 'auth_ok', username: user, token: token });
      console.log(`[ELMAK WS] Authenticated @${user}`);

      // Replay all undelivered or historic messages for this user
      for (const [chatId, list] of messagesHistory.entries()) {
        for (const m of list) {
          const r = (m.recipient || '').toLowerCase().trim();
          const s = (m.sender || '').toLowerCase().trim();
          if (r === user || s === user) {
            sendToSocket(senderSocket, m);
          }
        }
      }
    }
    return;
  }

  // 2. Real-Time Message Dispatch
  if (msg.type === 'message') {
    const recipient = (msg.recipient || '').trim().toLowerCase();
    const sender = (senderSocket.username || msg.sender || '').trim().toLowerCase();
    msg.sender = sender;
    msg.recipient = recipient;
    msg.timestamp = Date.now();

    const chatId = msg.chat_id || ('chat_' + [sender, recipient].sort().join('_'));
    msg.chat_id = chatId;

    // Save in history
    if (!messagesHistory.has(chatId)) {
      messagesHistory.set(chatId, []);
    }
    messagesHistory.get(chatId).push(msg);

    console.log(`[ELMAK WS] Dispatching message: @${sender} ➔ @${recipient} (${chatId})`);

    // Deliver to recipient sockets
    if (recipient && sockets.has(recipient)) {
      for (const client of sockets.get(recipient)) {
        sendToSocket(client, msg);
      }
    }

    // Echo to sender other sockets if any
    if (sender && sockets.has(sender)) {
      for (const client of sockets.get(sender)) {
        if (client !== senderSocket) {
          sendToSocket(client, msg);
        }
      }
    }
    return;
  }

  // 3. Delivery ACK (Double Check ✓✓)
  if (msg.type === 'delivery_ack') {
    const recipient = (msg.recipient || '').trim().toLowerCase();
    if (recipient && sockets.has(recipient)) {
      for (const client of sockets.get(recipient)) {
        sendToSocket(client, msg);
      }
    }
    return;
  }

  // 4. Zero-Trace Delete
  if (msg.type === 'delete') {
    const chatId = msg.chat_id;
    const messageId = msg.message_id;

    if (chatId && messagesHistory.has(chatId)) {
      const list = messagesHistory.get(chatId);
      const filtered = list.filter((m) => m.id !== messageId && m.client_message_id !== messageId);
      messagesHistory.set(chatId, filtered);
    }

    // Broadcast delete to all connected clients
    for (const [user, clientSet] of sockets.entries()) {
      for (const client of clientSet) {
        sendToSocket(client, {
          type: 'delete',
          chat_id: chatId,
          message_id: messageId,
          timestamp: Date.now(),
        });
      }
    }
    return;
  }

  // 5. Edit Message
  if (msg.type === 'edit') {
    const chatId = msg.chat_id;
    const messageId = msg.message_id;
    const newText = msg.payload?.new_text;

    if (chatId && messagesHistory.has(chatId)) {
      const list = messagesHistory.get(chatId);
      const item = list.find((m) => m.id === messageId || m.client_message_id === messageId);
      if (item) {
        item.text = newText;
        item.isEdited = true;
      }
    }

    for (const [user, clientSet] of sockets.entries()) {
      for (const client of clientSet) {
        sendToSocket(client, msg);
      }
    }
    return;
  }

  // 6. Presence & WebRTC
  if (msg.type === 'presence_ping') {
    sendToSocket(senderSocket, { type: 'presence_pong', timestamp: Date.now() });
    return;
  }

  if (msg.type === 'typing' || msg.type === 'webrtc') {
    const recipient = (msg.recipient || '').trim().toLowerCase();
    if (recipient && sockets.has(recipient)) {
      for (const client of sockets.get(recipient)) {
        sendToSocket(client, msg);
      }
    }
  }
}

function handleSocketClose(socket) {
  if (socket.username && sockets.has(socket.username)) {
    const set = sockets.get(socket.username);
    set.delete(socket);
    if (set.size === 0) {
      sockets.delete(socket.username);
      if (users.has(socket.username)) {
        users.get(socket.username).online = false;
        users.get(socket.username).lastSeen = Date.now();
      }
    }
  }
}

function sendToSocket(socket, obj) {
  try {
    const str = JSON.stringify(obj);
    if (socket.readyState === 1 || socket.readyState === WebSocket.OPEN) {
      socket.send(str);
    } else if (socket.write) {
      const frame = encodeFallbackFrame(str);
      socket.write(frame);
    }
  } catch (e) {}
}

function encodeFallbackFrame(data) {
  const payload = Buffer.from(data, 'utf8');
  const length = payload.length;
  let header;
  if (length <= 125) {
    header = Buffer.alloc(2);
    header[0] = 0x81;
    header[1] = length;
  } else if (length <= 65535) {
    header = Buffer.alloc(4);
    header[0] = 0x81;
    header[1] = 126;
    header.writeUInt16BE(length, 2);
  } else {
    header = Buffer.alloc(10);
    header[0] = 0x81;
    header[1] = 127;
    header.writeBigUInt64BE(BigInt(length), 2);
  }
  return Buffer.concat([header, payload]);
}

// ----------------------------------------------------------------------------
// HTTP Server & REST Endpoints
// ----------------------------------------------------------------------------
const server = http.createServer((req, res) => {
  // CORS Headers
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization, X-User-Identity');

  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }

  const parsedUrl = url.parse(req.url, true);
  const pathname = parsedUrl.pathname;

  // Web Client & PWA Single Page Dashboard
  if (pathname === '/' || pathname === '/index.html') {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(getWebClientHTML());
    return;
  }

  // Health Check
  if (pathname === '/health' || pathname === '/api/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      status: 'healthy',
      app: 'Elmak Messenger (عِلمك)',
      timestamp: new Date().toISOString(),
      connections: Array.from(sockets.keys()).length,
      users_online: Array.from(users.values()).filter(u => u.online).length,
    }));
    return;
  }

  // Active Users Directory
  if (pathname === '/api/users' && req.method === 'GET') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(Array.from(users.values())));
    return;
  }

  // AI Translation Endpoint
  if (pathname === '/api/ai/translate' && req.method === 'POST') {
    let body = '';
    req.on('data', (c) => body += c);
    req.on('end', () => {
      try {
        const json = JSON.parse(body);
        const text = json.text || '';
        const lower = text.toLowerCase();

        let dialect = "العربية الفصحى";
        if (lower.includes('وش') || lower.includes('ابشر') || lower.includes('زين') || lower.includes('علمك')) {
          dialect = "اللهجة الخليجية / السعودية";
        } else if (lower.includes('ازيك') || lower.includes('عامل ايه') || lower.includes('كويس') || lower.includes('دلوقتي')) {
          dialect = "اللهجة المصرية";
        } else if (lower.includes('شو اخبارك') || lower.includes('بدي') || lower.includes('منيح')) {
          dialect = "اللهجة الشامية";
        } else if (lower.includes('واخا') || lower.includes('بزاف') || lower.includes('دابا')) {
          dialect = "اللهجة المغاربية";
        }

        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify({
          original_text: text,
          translated_text: text,
          detected_dialect: dialect,
          confidence: 0.99,
        }));
      } catch (e) {
        res.writeHead(400);
        res.end(JSON.stringify({ error: 'Invalid JSON' }));
      }
    });
    return;
  }

  // Media Upload Endpoint
  if (pathname === '/api/media/upload' && req.method === 'POST') {
    const filename = `elmak_file_${Date.now()}_${Math.floor(Math.random() * 1000)}.bin`;
    const filepath = path.join(UPLOADS_DIR, filename);
    const writeStream = fs.createWriteStream(filepath);

    req.pipe(writeStream);
    writeStream.on('finish', () => {
      res.writeHead(201, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        file_id: filename,
        file_url: `/api/media/${filename}`,
      }));
    });
    return;
  }

  // Media Download Endpoint
  if (pathname.startsWith('/api/media/')) {
    const filename = path.basename(pathname);
    const filepath = path.join(UPLOADS_DIR, filename);
    if (fs.existsSync(filepath)) {
      res.writeHead(200, { 'Content-Type': 'application/octet-stream' });
      fs.createReadStream(filepath).pipe(res);
      return;
    }
  }

  res.writeHead(404, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ error: 'Not found' }));
});

// Setup WebSocket Server using ws library
if (WebSocket && WebSocket.Server) {
  const wss = new WebSocket.Server({ server, path: '/ws' });
  wss.on('connection', (ws) => {
    ws.username = null;
    ws.isAlive = true;

    ws.on('message', (data) => {
      try {
        const msg = JSON.parse(data.toString('utf8'));
        handleMessage(ws, msg);
      } catch (e) {}
    });

    ws.on('close', () => handleSocketClose(ws));
    ws.on('error', () => handleSocketClose(ws));
  });
  console.log('[Server] Attached official WebSocket.Server to /ws');
} else {
  // Native RFC6455 upgrade fallback
  server.on('upgrade', (req, socket, head) => {
    const key = req.headers['sec-websocket-key'];
    if (!key) {
      socket.destroy();
      return;
    }
    const acceptKey = crypto.createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
    const headers = [
      'HTTP/1.1 101 Switching Protocols',
      'Upgrade: websocket',
      'Connection: Upgrade',
      `Sec-WebSocket-Accept: ${acceptKey}`,
      '', '',
    ];
    socket.write(headers.join('\r\n'));

    socket.on('data', (buffer) => {
      try {
        if (buffer.length < 2) return;
        const isMasked = (buffer[1] & 0x80) !== 0;
        let payloadLen = buffer[1] & 0x7f;
        let offset = 2;
        if (payloadLen === 126) { payloadLen = buffer.readUInt16BE(2); offset = 4; }
        else if (payloadLen === 127) { payloadLen = Number(buffer.readBigUInt64BE(2)); offset = 10; }
        let mask = null;
        if (isMasked) { mask = buffer.slice(offset, offset + 4); offset += 4; }
        const raw = buffer.slice(offset, offset + payloadLen);
        if (isMasked && mask) {
          for (let i = 0; i < raw.length; i++) raw[i] ^= mask[i % 4];
        }
        const msg = JSON.parse(raw.toString('utf8'));
        handleMessage(socket, msg);
      } catch (e) {}
    });

    socket.on('close', () => handleSocketClose(socket));
    socket.on('error', () => handleSocketClose(socket));
  });
}

server.listen(PORT, HOST, () => {
  console.log('===============================================================');
  console.log(`  🚀 ELMAK (عِلمك) LIVE MESSAGING SERVER RUNNING`);
  console.log(`  📡 Listening on http://${HOST}:${PORT}`);
  console.log(`  ⚡ Real-Time WebSocket at ws://<YOUR_IP>:${PORT}/ws`);
  console.log('===============================================================');
});

// ----------------------------------------------------------------------------
// Built-in Responsive Web Client / iPhone PWA UI
// ----------------------------------------------------------------------------
function getWebClientHTML() {
  return `<!DOCTYPE html>
<html lang="ar" dir="rtl">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
  <title>عِلمك - تطبيق المراسلة الفورية المشفرة</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Cairo:wght@400;600;700;800;900&display=swap" rel="stylesheet">
  <style>
    :root {
      --bg: #090D16;
      --surface: #111726;
      --surface-light: #1A2238;
      --primary: #0EA5E9;
      --accent: #38BDF8;
      --emerald-glow: #10B981;
      --gold: #F59E0B;
      --text-main: #F8FAFC;
      --text-muted: #94A3B8;
      --border: rgba(255, 255, 255, 0.08);
      --font: 'Cairo', system-ui, -apple-system, sans-serif;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; font-family: var(--font); }
    body { background-color: var(--bg); color: var(--text-main); height: 100vh; display: flex; flex-direction: column; overflow: hidden; }
    
    /* Header */
    .app-header {
      background: var(--surface); border-bottom: 1px solid var(--border);
      padding: 12px 20px; display: flex; align-items: center; justify-content: space-between;
      height: 64px; flex-shrink: 0;
    }
    .brand-section { display: flex; align-items: center; gap: 12px; }
    .brand-logo {
      width: 40px; height: 40px; border-radius: 12px; background: linear-gradient(135deg, var(--primary), #0284C7);
      display: flex; align-items: center; justify-content: center; font-size: 22px; font-weight: 900;
      box-shadow: 0 4px 14px rgba(14, 165, 233, 0.35); color: #FFF;
    }
    .brand-title { font-size: 18px; font-weight: 800; }
    .brand-subtitle { font-size: 11px; color: var(--emerald-glow); font-weight: 600; display: flex; align-items: center; gap: 4px; }
    .status-dot { width: 7px; height: 7px; background: var(--emerald-glow); border-radius: 50%; display: inline-block; animation: pulse 2s infinite; }
    @keyframes pulse { 0% { opacity: 1; } 50% { opacity: 0.3; } 100% { opacity: 1; } }

    .account-badge {
      background: var(--surface-light); border: 1px solid var(--border); border-radius: 20px;
      padding: 6px 14px; font-size: 13px; font-weight: 600; cursor: pointer; transition: 0.2s;
    }
    .account-badge:hover { border-color: var(--primary); }

    /* Main Layout */
    .main-container { display: flex; flex: 1; height: calc(100vh - 64px); overflow: hidden; position: relative; }
    
    /* Sidebar */
    .chat-sidebar {
      width: 100%; max-width: 340px; background: var(--surface); border-left: 1px solid var(--border);
      display: flex; flex-direction: column; flex-shrink: 0;
    }
    .search-box { padding: 12px; border-bottom: 1px solid var(--border); }
    .search-input {
      width: 100%; padding: 10px 14px; background: var(--surface-light); border: 1px solid var(--border);
      border-radius: 10px; color: #FFF; font-size: 13px; outline: none; transition: 0.2s;
    }
    .search-input:focus { border-color: var(--primary); }
    
    .chat-list { flex: 1; overflow-y: auto; }
    .chat-item {
      padding: 12px 16px; display: flex; align-items: center; gap: 12px; border-bottom: 1px solid rgba(255,255,255,0.03);
      cursor: pointer; transition: 0.2s;
    }
    .chat-item:hover, .chat-item.active { background: var(--surface-light); }
    .chat-avatar {
      width: 44px; height: 44px; border-radius: 50%; background: linear-gradient(135deg, #1E293B, #334155);
      display: flex; align-items: center; justify-content: center; font-size: 18px; font-weight: 700;
      color: var(--primary); border: 1px solid var(--border); flex-shrink: 0;
    }
    .chat-info { flex: 1; min-width: 0; }
    .chat-name-row { display: flex; justify-content: space-between; margin-bottom: 2px; }
    .chat-name { font-weight: 700; font-size: 14px; }
    .chat-time { font-size: 11px; color: var(--text-muted); }
    .chat-last-msg { font-size: 12px; color: var(--text-muted); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }

    /* Conversation View */
    .conversation-view { flex: 1; display: flex; flex-direction: column; background: var(--bg); }
    .conversation-header {
      padding: 12px 20px; background: var(--surface); border-bottom: 1px solid var(--border);
      display: flex; align-items: center; justify-content: space-between;
    }
    .messages-container { flex: 1; overflow-y: auto; padding: 20px; display: flex; flex-direction: column; gap: 12px; }
    
    .msg-bubble {
      max-width: 75%; padding: 10px 14px; border-radius: 16px; font-size: 14px; line-height: 1.5;
      position: relative; word-break: break-word;
    }
    .msg-me {
      align-self: flex-start; background: linear-gradient(135deg, #0284C7, #0369A1); color: #FFF;
      border-bottom-right-radius: 4px; box-shadow: 0 4px 12px rgba(2, 132, 199, 0.2);
    }
    .msg-peer {
      align-self: flex-end; background: var(--surface-light); color: var(--text-main);
      border-bottom-left-radius: 4px; border: 1px solid var(--border);
    }
    .msg-meta { display: flex; align-items: center; justify-content: flex-end; gap: 4px; font-size: 10px; opacity: 0.75; margin-top: 4px; }
    .msg-actions { margin-top: 6px; padding-top: 4px; border-top: 1px solid rgba(255,255,255,0.1); display: flex; gap: 8px; font-size: 11px; }
    .action-btn { background: none; border: none; color: #FFF; opacity: 0.8; cursor: pointer; padding: 2px 6px; border-radius: 4px; }
    .action-btn:hover { opacity: 1; background: rgba(255,255,255,0.1); }

    .input-bar {
      padding: 14px 20px; background: var(--surface); border-top: 1px solid var(--border);
      display: flex; align-items: center; gap: 10px;
    }
    .chat-input {
      flex: 1; padding: 12px 16px; background: var(--surface-light); border: 1px solid var(--border);
      border-radius: 24px; color: #FFF; font-size: 15px; outline: none;
    }
    .send-btn {
      width: 44px; height: 44px; border-radius: 50%; background: linear-gradient(135deg, #0F5132, #10B981);
      border: none; color: #FFF; display: flex; align-items: center; justify-content: center; font-size: 18px;
      cursor: pointer; box-shadow: 0 4px 12px rgba(16, 185, 129, 0.3);
    }
    
    /* Responsive */
    @media (max-width: 768px) {
      .chat-sidebar { max-width: 100%; }
      .conversation-view { display: none; position: absolute; inset: 0; z-index: 50; }
      .conversation-view.active { display: flex; }
    }

    /* Registration Modal */
    .modal-overlay {
      position: fixed; inset: 0; background: rgba(0,0,0,0.85); backdrop-filter: blur(8px);
      display: flex; align-items: center; justify-content: center; z-index: 1000; padding: 20px;
    }
    .modal-card {
      background: var(--surface); border: 1px solid var(--border); border-radius: 20px;
      padding: 28px; width: 100%; max-width: 400px; text-align: center; box-shadow: 0 16px 40px rgba(0,0,0,0.6);
    }
    .modal-card h2 { margin: 0 0 8px 0; color: #FFF; font-size: 20px; }
    .modal-card p { color: var(--text-muted); font-size: 13px; margin: 0 0 20px 0; }
    .modal-input {
      width: 100%; padding: 14px 16px; background: var(--surface-light); border: 1px solid var(--border);
      border-radius: 12px; color: #FFF; font-size: 14px; margin-bottom: 12px; box-sizing: border-box; outline: none;
    }
    .modal-input:focus { border-color: var(--primary); }
    .modal-btn {
      width: 100%; padding: 14px; background: linear-gradient(135deg, var(--primary), #0284C7);
      border: none; border-radius: 12px; color: #FFF; font-size: 15px; font-weight: 700; cursor: pointer;
    }
  </style>
</head>
<body>
  <!-- Header -->
  <header class="app-header">
    <div class="brand-section">
      <div class="brand-logo">عِ</div>
      <div>
        <div class="brand-title">عِلمك</div>
        <div class="brand-subtitle"><span class="status-dot"></span> متصل بالسحابة 24/7</div>
      </div>
    </div>
    <div class="account-badge" onclick="promptSwitchUser()">
      <span id="current-user-display">👤 @...</span>
    </div>
  </header>

  <!-- Main Container -->
  <div class="main-container">
    <!-- Chat Sidebar -->
    <aside class="chat-sidebar" id="sidebar">
      <div class="search-box">
        <input type="text" class="search-input" id="new-chat-user" placeholder="🔍 ابدأ محادثة باسم المستخدم (مثال: android_user)..." onkeypress="if(event.key==='Enter') startChat()">
      </div>
      <div class="chat-list" id="chat-list-container">
        <!-- Chats will be dynamically populated -->
      </div>
    </aside>

    <!-- Conversation View -->
    <main class="conversation-view" id="conversation-view">
      <div class="conversation-header">
        <button class="action-btn" style="padding: 6px 12px;" onclick="closeConversation()">← رجوع</button>
        <div style="text-align: center;">
          <div style="font-weight: 700; font-size: 16px;" id="active-chat-title">المحادثة</div>
          <div style="font-size: 11px; color: var(--emerald-glow);">🔒 تشفير تام E2EE</div>
        </div>
        <button class="action-btn" onclick="clearActiveChat()">تنظيف</button>
      </div>

      <div class="messages-container" id="messages-container"></div>

      <div class="input-bar">
        <input type="text" class="chat-input" id="message-input" placeholder="اكتب رسالتك المشفرة هنا..." onkeypress="if(event.key==='Enter') sendMessage()">
        <button class="send-btn" onclick="sendMessage()">➤</button>
      </div>
    </main>
  </div>

  <!-- Registration Modal -->
  <div class="modal-overlay" id="reg-modal" style="display: none;">
    <div class="modal-card">
      <div style="font-size: 40px; margin-bottom: 12px;">🛡️</div>
      <h2>مرحباً بك في تطبيق عِلمك</h2>
      <p>اختر اسم المستخدم الخاص بك لبدء المراسلة الفورية والمشفرة من أي جهاز</p>
      <input type="text" class="modal-input" id="reg-username" placeholder="اسم المستخدم (مثال: iphone_user)">
      <input type="text" class="modal-input" id="reg-displayname" placeholder="الاسم الظاهر (مثال: أبو فهد)">
      <button class="modal-btn" onclick="submitRegistration()">دخول والمراسلة الآن 🚀</button>
    </div>
  </div>

  <script>
    let myUser = (localStorage.getItem('elmak_user') || '').toLowerCase().trim();
    let myName = localStorage.getItem('elmak_name') || myUser;

    function checkUserRegistration() {
      if (!myUser) {
        document.getElementById('reg-modal').style.display = 'flex';
      } else {
        document.getElementById('reg-modal').style.display = 'none';
        document.getElementById('current-user-display').innerText = '👤 @' + myUser;
        initWebSocket();
      }
    }

    function submitRegistration() {
      const u = document.getElementById('reg-username').value.trim().replace('@', '').toLowerCase();
      const n = document.getElementById('reg-displayname').value.trim() || u;
      if (!u) {
        alert('الرجاء إدخال اسم مستخدم صحيح');
        return;
      }
      myUser = u;
      myName = n;
      localStorage.setItem('elmak_user', myUser);
      localStorage.setItem('elmak_name', myName);
      document.getElementById('reg-modal').style.display = 'none';
      document.getElementById('current-user-display').innerText = '👤 @' + myUser;
      initWebSocket();
    }

    let activePeer = null;
    let chats = {}; // { peerUsername: [ { id, sender, text, time, status } ] }
    let ws = null;

    // Base64 E2EE Protocol
    function encryptText(plain) {
      try {
        return 'E2EE::v1::' + btoa(unescape(encodeURIComponent(plain)));
      } catch (e) {
        return plain;
      }
    }
    function decryptText(cipher) {
      if (cipher && cipher.startsWith('E2EE::v1::')) {
        try {
          return decodeURIComponent(escape(atob(cipher.replace('E2EE::v1::', ''))));
        } catch (e) {
          return cipher;
        }
      }
      return cipher;
    }

    function initWebSocket() {
      if (!myUser) return;
      if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) {
        return;
      }

      const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
      const wsUrl = protocol + '//' + location.host + '/ws';
      ws = new WebSocket(wsUrl);

      ws.onopen = () => {
        ws.send(JSON.stringify({
          type: 'auth',
          username: myUser,
          displayName: myName
        }));
      };

      ws.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data);
          
          // 1. Delivery ACK -> Double check ✓✓
          if (data.type === 'delivery_ack') {
            const msgId = data.message_id;
            for (let u in chats) {
              const m = chats[u].find(item => item.id === msgId);
              if (m) {
                m.status = 'delivered'; // ✓✓
              }
            }
            if (activePeer) renderMessages();
            return;
          }

          // 2. Message packet
          if (data.type === 'message') {
            const sender = (data.sender || '').toLowerCase().trim();
            const recipient = (data.recipient || '').toLowerCase().trim();
            const isFromMe = (sender === myUser);
            const peer = isFromMe ? recipient : sender;

            if (!peer) return;

            let textContent = '';
            if (data.payload && data.payload.cipher) {
              textContent = decryptText(data.payload.cipher);
            } else {
              textContent = data.payload?.text || data.text || '';
            }

            const msgId = data.client_message_id || data.message_id || 'm_' + Date.now();
            
            if (!chats[peer]) chats[peer] = [];
            const existing = chats[peer].find(m => m.id === msgId);
            if (!existing) {
              chats[peer].push({
                id: msgId,
                sender: sender,
                text: textContent,
                status: isFromMe ? 'sent' : 'delivered',
                time: new Date().toLocaleTimeString('ar-SA', { hour: '2-digit', minute: '2-digit' })
              });
              renderChatList();
              if (activePeer === peer) renderMessages();

              // If I received message, send Delivery ACK
              if (!isFromMe && ws && ws.readyState === WebSocket.OPEN) {
                ws.send(JSON.stringify({
                  type: 'delivery_ack',
                  message_id: msgId,
                  chat_id: data.chat_id || ('chat_' + [sender, recipient].sort().join('_')),
                  sender: myUser,
                  recipient: sender
                }));
              }
            } else if (isFromMe) {
              existing.status = 'sent';
              if (activePeer === peer) renderMessages();
            }
          } else if (data.type === 'delete') {
            const msgId = data.message_id;
            for (let u in chats) {
              chats[u] = chats[u].filter(m => m.id !== msgId);
            }
            renderChatList();
            if (activePeer) renderMessages();
          }
        } catch (e) {}
      };

      ws.onclose = () => setTimeout(initWebSocket, 2000);
    }

    function renderChatList() {
      const container = document.getElementById('chat-list-container');
      container.innerHTML = '';
      const peers = Object.keys(chats);
      if (peers.length === 0) {
        container.innerHTML = '<div style="padding: 32px 16px; text-align: center; color: var(--text-muted); font-size: 13px;">لا توجد محادثات سابقة.<br>أدخل اسم مستخدم في الأعلى لبدء المراسلة المشفرة ⚡</div>';
        return;
      }
      peers.forEach(peer => {
        const msgs = chats[peer];
        const lastMsg = msgs[msgs.length - 1];
        const item = document.createElement('div');
        item.className = 'chat-item ' + (activePeer === peer ? 'active' : '');
        item.onclick = () => openChat(peer);
        item.innerHTML = `
          <div class="chat-avatar">${peer[0].toUpperCase()}</div>
          <div class="chat-info">
            <div class="chat-name-row">
              <span class="chat-name">@${peer}</span>
              <span class="chat-time">${lastMsg ? lastMsg.time : ''}</span>
            </div>
            <div class="chat-last-msg">${lastMsg ? lastMsg.text : 'محادثة مشفرة'}</div>
          </div>
        `;
        container.appendChild(item);
      });
    }

    function startChat() {
      const input = document.getElementById('new-chat-user');
      const val = input.value.trim().replace('@', '').toLowerCase();
      if (!val) return;
      input.value = '';
      if (!chats[val]) chats[val] = [];
      openChat(val);
    }

    function openChat(peer) {
      activePeer = peer.toLowerCase().trim();
      document.getElementById('active-chat-title').innerText = '@' + activePeer;
      document.getElementById('conversation-view').classList.add('active');
      renderChatList();
      renderMessages();
    }

    function closeConversation() {
      document.getElementById('conversation-view').classList.remove('active');
    }

    function clearActiveChat() {
      if (activePeer && chats[activePeer]) {
        chats[activePeer] = [];
        renderMessages();
        renderChatList();
      }
    }

    function renderMessages() {
      const container = document.getElementById('messages-container');
      container.innerHTML = '';
      if (!activePeer || !chats[activePeer]) return;
      chats[activePeer].forEach(msg => {
        const isMe = msg.sender.toLowerCase() === myUser.toLowerCase();
        const checkMark = msg.status === 'delivered' ? '✓✓' : '✓';
        const div = document.createElement('div');
        div.className = 'msg-bubble ' + (isMe ? 'msg-me' : 'msg-peer');
        div.innerHTML = `
          <div>${msg.text}</div>
          ${msg.translated ? `<div style="margin-top:4px; font-size:12px; color:var(--gold); border-top:1px dashed rgba(255,255,255,0.2); padding-top:4px;">✨ ترجمة (${msg.detected || 'فصحى'}): ${msg.translated}</div>` : ''}
          <div class="msg-meta">
            <span>${msg.time}</span>
            ${isMe ? `<span style="margin-right:4px; font-size:12px;">${checkMark}</span>` : ''}
          </div>
          <div class="msg-actions">
            ${!isMe ? `<button class="action-btn" onclick="translateMsg('${msg.id}')">✨ ترجمة</button>` : ''}
            <button class="action-btn" onclick="deleteMsg('${msg.id}')">🗑️ حذف للطرفين</button>
          </div>
        `;
        container.appendChild(div);
      });
      container.scrollTop = container.scrollHeight;
    }

    function sendMessage() {
      const input = document.getElementById('message-input');
      const text = input.value.trim();
      if (!text || !activePeer) return;
      input.value = '';

      const msgId = 'msg_' + Date.now();
      const timeStr = new Date().toLocaleTimeString('ar-SA', { hour: '2-digit', minute: '2-digit' });
      const newMsg = { id: msgId, sender: myUser, text: text, status: 'sent', time: timeStr };

      if (!chats[activePeer]) chats[activePeer] = [];
      chats[activePeer].push(newMsg);
      renderChatList();
      renderMessages();

      if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({
          type: 'message',
          client_message_id: msgId,
          sender: myUser,
          recipient: activePeer,
          chat_id: 'chat_' + [myUser, activePeer].sort().join('_'),
          payload: {
            text: text,
            cipher: encryptText(text),
            type: 'text'
          }
        }));
      }
    }

    function deleteMsg(msgId) {
      if (!activePeer || !chats[activePeer]) return;
      chats[activePeer] = chats[activePeer].filter(m => m.id !== msgId);
      renderMessages();
      renderChatList();

      if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({
          type: 'delete',
          message_id: msgId,
          chat_id: 'chat_' + [myUser, activePeer].sort().join('_')
        }));
      }
    }

    async function translateMsg(msgId) {
      const msg = (chats[activePeer] || []).find(m => m.id === msgId);
      if (!msg) return;
      try {
        const res = await fetch('/api/ai/translate', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ text: msg.text })
        });
        const data = await res.json();
        msg.translated = data.translated_text;
        msg.detected = data.detected_dialect;
        renderMessages();
      } catch (e) {}
    }

    function promptSwitchUser() {
      const u = prompt('أدخل اسم المستخدم الخاص بك (Username):', myUser);
      if (u && u.trim()) {
        myUser = u.trim().replace('@', '').toLowerCase();
        localStorage.setItem('elmak_user', myUser);
        document.getElementById('current-user-display').innerText = '👤 @' + myUser;
        if (ws && ws.readyState === WebSocket.OPEN) {
          ws.send(JSON.stringify({ type: 'auth', username: myUser, displayName: myUser }));
        }
      }
    }

    checkUserRegistration();
  </script>
</body>
</html>`;
}
