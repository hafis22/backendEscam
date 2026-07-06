const express    = require('express');
const cors       = require('cors');
const bodyParser = require('body-parser');
const multer     = require('multer');
const FormData   = require('form-data');
const fetch      = require('node-fetch');
const mysql      = require('mysql2/promise');
const http       = require('http');
const { WebSocketServer } = require('ws');

const app    = express();
const PORT   = process.env.PORT || 5000;
const upload = multer({ storage: multer.memoryStorage() });

app.use(cors({
  origin: [
    'https://frontend-espcam-g61d.vercel.app',
    'https://frontendespcam-production.up.railway.app',
    'http://localhost:3000',
  ],
  credentials: true,
}));
app.use(bodyParser.json());

// ── HTTP + WebSocket Server ────────────────────────────────
const server = http.createServer(app);
const wss    = new WebSocketServer({ server });

// Set frontend clients yang subscribe live frame + hasil deteksi
const frontendClients = new Set();
let   esp32WsClient   = null;

// ── FLAG BYTE (sama dengan ESP32) ─────────────────────────
const FLAG_FRAME  = 0x01;  // frame live biasa
const FLAG_DETECT = 0x02;  // frame untuk deteksi YOLO

// ── Broadcast ke semua frontend ───────────────────────────
function broadcastBinary(data) {
  for (const client of frontendClients) {
    if (client.readyState === 1) {
      client.send(data, { binary: true });
    }
  }
}

function broadcastJSON(obj) {
  const msg = JSON.stringify(obj);
  for (const client of frontendClients) {
    if (client.readyState === 1) {
      client.send(msg);
    }
  }
}

// ── Proses YOLO di background (non-blocking) ──────────────
async function prosesYOLO(jpegBuffer) {
  const YOLO_URL = process.env.YOLO_SERVICE_URL;
  if (!YOLO_URL) {
    console.error('[YOLO] YOLO_SERVICE_URL tidak di-set');
    broadcastJSON({ type: 'detect_error', message: 'YOLO service URL belum dikonfigurasi' });
    return;
  }

  try {
    console.log(`[YOLO] Kirim frame ${jpegBuffer.length} bytes ke YOLO...`);

    const formData = new FormData();
    formData.append('foto', jpegBuffer, {
      filename:    'foto.jpg',
      contentType: 'image/jpeg',
    });

    const response = await fetch(`${YOLO_URL}/detect`, {
      method:  'POST',
      body:    formData,
      headers: formData.getHeaders(),
      signal:  AbortSignal.timeout(110000),
    });

    const rawText = await response.text();

    if (!response.ok) {
      console.error(`[YOLO] HTTP ${response.status}:`, rawText.slice(0, 200));
      broadcastJSON({ type: 'detect_error', message: `YOLO error HTTP ${response.status}` });
      return;
    }

    let hasil;
    try {
      hasil = JSON.parse(rawText);
    } catch (_) {
      console.error('[YOLO] Response non-JSON:', rawText.slice(0, 200));
      broadcastJSON({ type: 'detect_error', message: 'YOLO response tidak valid' });
      return;
    }

    // Simpan ke DB
    try {
      await pool.query(
        'INSERT INTO deteksi_logs (penyakit, confidence) VALUES (?, ?)',
        [hasil.penyakit || 'Tidak terdeteksi', hasil.confidence || 0]
      );
    } catch (dbErr) {
      console.error('[YOLO] Gagal simpan ke DB:', dbErr.message);
    }

    console.log(`[YOLO] Hasil: ${hasil.penyakit} (${hasil.confidence}%)`);

    // Push hasil ke semua frontend via WebSocket
    broadcastJSON({
      type:       'detect_result',
      penyakit:   hasil.penyakit   || 'Tidak terdeteksi',
      confidence: hasil.confidence || 0,
      timestamp:  new Date().toISOString(),
    });

    // Beri tahu ESP32 bahwa deteksi selesai
    if (esp32WsClient && esp32WsClient.readyState === 1) {
      esp32WsClient.send(JSON.stringify({ type: 'detect_done' }));
    }

  } catch (err) {
    console.error('[YOLO] Error:', err.message);
    broadcastJSON({ type: 'detect_error', message: 'Gagal konek ke YOLO service' });

    // Tetap beri tahu ESP32 supaya isDetecting di-reset
    if (esp32WsClient && esp32WsClient.readyState === 1) {
      esp32WsClient.send(JSON.stringify({ type: 'detect_done' }));
    }
  }
}

// ── WebSocket connections ──────────────────────────────────
wss.on('connection', (ws, req) => {
  const url = req.url || '';

  // ── Koneksi dari ESP32 ──────────────────────────────
  if (url === '/ws/esp32') {
    console.log('[WS] ESP32 terhubung');
    esp32WsClient     = ws;
    esp32State.online = true;
    esp32State.lastSeen = new Date();

    ws.on('message', (data, isBinary) => {
      if (!isBinary || data.length < 2) return;

      const flag     = data[0];           // byte pertama = flag
      const jpegBuf  = data.slice(1);     // sisa = JPEG

      esp32State.online   = true;
      esp32State.lastSeen = new Date();

      if (flag === FLAG_FRAME) {
        // Frame live biasa — broadcast langsung ke frontend
        esp32Frame = jpegBuf;
        broadcastBinary(jpegBuf);

      } else if (flag === FLAG_DETECT) {
        // Frame deteksi — broadcast dulu (live tetap jalan), lalu proses YOLO async
        esp32Frame = jpegBuf;
        broadcastBinary(jpegBuf);

        console.log('[WS] Terima frame deteksi dari ESP32, proses YOLO...');
        // Tidak di-await — non-blocking, live stream tetap jalan
        prosesYOLO(Buffer.from(jpegBuf)).catch(err =>
          console.error('[YOLO] Uncaught:', err.message)
        );

      } else {
        // Flag tidak dikenal — anggap frame biasa
        broadcastBinary(data);
      }
    });

    ws.on('close', () => {
      console.log('[WS] ESP32 disconnect');
      esp32WsClient     = null;
      esp32State.online = false;
    });

    ws.on('error', (err) => {
      console.error('[WS] ESP32 error:', err.message);
    });

  // ── Koneksi dari frontend ───────────────────────────
  } else if (url === '/ws/frame') {
    console.log('[WS] Frontend client terhubung');
    frontendClients.add(ws);

    // Kirim frame terakhir yang ada supaya frontend langsung tampil
    if (esp32Frame) {
      ws.send(esp32Frame, { binary: true });
    }

    ws.on('close', () => {
      frontendClients.delete(ws);
      console.log('[WS] Frontend client disconnect');
    });

    ws.on('error', (err) => {
      console.error('[WS] Frontend error:', err.message);
      frontendClients.delete(ws);
    });
  }
});

// ── MySQL Connection Pool ──────────────────────────────────
let pool;

if (process.env.MYSQL_URL) {
  const url = new URL(process.env.MYSQL_URL);
  pool = mysql.createPool({
    host:               url.hostname,
    user:               url.username,
    password:           url.password,
    database:           url.pathname.replace('/', ''),
    port:               Number(url.port) || 3306,
    waitForConnections: true,
    connectionLimit:    10,
    queueLimit:         0,
  });
  console.log(`MySQL → ${url.hostname}:${url.port}${url.pathname}`);
} else {
  pool = mysql.createPool({
    host:               process.env.DB_HOST     || 'localhost',
    user:               process.env.DB_USER     || 'root',
    password:           process.env.DB_PASSWORD || '',
    database:           process.env.DB_NAME     || 'railway',
    port:               process.env.DB_PORT     || 3306,
    waitForConnections: true,
    connectionLimit:    10,
    queueLimit:         0,
  });
  console.log('MySQL → localhost');
}

// ── MQTT ───────────────────────────────────────────────────
const mqtt = require('mqtt');

const MQTT_URL   = process.env.MQTT_URL   || 'mqtt://202.10.40.129:1883';
const MQTT_USER  = process.env.MQTT_USER  || 'greenhouse';
const MQTT_PASS  = process.env.MQTT_PASS  || 'Vanilijaya12';
const MQTT_TOPIC = process.env.MQTT_TOPIC || 'iot/sensor/data';

const mqttClient = mqtt.connect(MQTT_URL, {
  username:        MQTT_USER,
  password:        MQTT_PASS,
  clientId:        'backend_railway_' + Math.random().toString(16).slice(2),
  reconnectPeriod: 5000,
});

mqttClient.on('connect', () => {
  console.log('[MQTT] Terhubung ke broker:', MQTT_URL);
  mqttClient.subscribe(MQTT_TOPIC, (err) => {
    if (err) console.error('[MQTT] Gagal subscribe:', err);
    else     console.log('[MQTT] Subscribe topic:', MQTT_TOPIC);
  });
});

mqttClient.on('message', async (topic, message) => {
  try {
    const payload = JSON.parse(message.toString());
    console.log('[MQTT] Data masuk:', JSON.stringify(payload));

    if (payload.suhu_light !== undefined || payload.lembab_light !== undefined) {
      await pool.query(
        `INSERT INTO sensor_logs (temp_lingkungan, humidity_lingkungan, lux) VALUES (?, ?, ?)`,
        [payload.suhu_light ?? null, payload.lembab_light ?? null, payload.intensitas ?? null]
      );
      return;
    }

    if (payload.suhu_soil !== undefined) {
      await pool.query(
        `INSERT INTO sensor_logs (temp_tanaman, humidity_tanaman, ec, ph, nitrogen, fosfor, kalium) VALUES (?, ?, ?, ?, ?, ?, ?)`,
        [payload.suhu_soil ?? null, payload.lembab_soil ?? null, payload.conductivity ?? null,
         payload.ph ?? null, payload.n ?? null, payload.p ?? null, payload.k ?? null]
      );
      return;
    }

    if (payload.suhu_soil2 !== undefined) {
      await pool.query(
        `INSERT INTO sensor_logs (temp_tanaman, humidity_tanaman, ec, ph, nitrogen, fosfor, kalium) VALUES (?, ?, ?, ?, ?, ?, ?)`,
        [payload.suhu_soil2 ?? null, payload.lembab_soil2 ?? null, payload.conductivity2 ?? null,
         payload.ph2 ?? null, payload.n2 ?? null, payload.p2 ?? null, payload.k2 ?? null]
      );
      return;
    }

    console.log('[MQTT] Format tidak dikenal, skip');
  } catch (err) {
    console.error('[MQTT] Gagal proses pesan:', err.message);
  }
});

mqttClient.on('error',      (err) => console.error('[MQTT] Error:', err.message));
mqttClient.on('disconnect', ()    => console.log('[MQTT] Disconnect'));
mqttClient.on('reconnect',  ()    => console.log('[MQTT] Reconnecting...'));

// ── Auto-migrate ───────────────────────────────────────────
(async () => {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS sensor_logs (
        id int NOT NULL AUTO_INCREMENT,
        temp_lingkungan float DEFAULT NULL,
        humidity_lingkungan float DEFAULT NULL,
        lux float DEFAULT NULL,
        temp_tanaman float DEFAULT NULL,
        humidity_tanaman float DEFAULT NULL,
        ph float DEFAULT NULL,
        ec float DEFAULT NULL,
        nitrogen float DEFAULT NULL,
        fosfor float DEFAULT NULL,
        kalium float DEFAULT NULL,
        created_at timestamp NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (id)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    `);
    await pool.query(`
      CREATE TABLE IF NOT EXISTS deteksi_logs (
        id int NOT NULL AUTO_INCREMENT,
        penyakit varchar(100) DEFAULT NULL,
        confidence float DEFAULT NULL,
        created_at timestamp NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (id)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    `);
    await pool.query(`
      CREATE TABLE IF NOT EXISTS esp32_state (
        id int NOT NULL DEFAULT 1,
        ip varchar(20) DEFAULT NULL,
        last_seen timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (id)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    `);
    console.log('[DB] Tabel siap');
  } catch (err) {
    console.error('[DB] Gagal auto-migrate:', err.message);
  }
})();

// ── ESP32 state ────────────────────────────────────────────
let esp32State = { ip: null, online: false, lastSeen: null };
let esp32Frame = null;

(async () => {
  try {
    const [rows] = await pool.query('SELECT * FROM esp32_state LIMIT 1');
    if (rows.length > 0) {
      esp32State = { ip: rows[0].ip, online: false, lastSeen: rows[0].last_seen };
      console.log('[ESP32] State loaded dari DB:', esp32State.ip);
    }
  } catch (err) {
    console.error('[ESP32] Gagal load state:', err.message);
  }
})();

// ═══════════════════════════════════════════════════════════
//  REST API ENDPOINTS
// ═══════════════════════════════════════════════════════════

app.get('/', (_req, res) => {
  res.json({ status: 'ok', message: 'Smart Farm Backend is running' });
});

// ── Sensor ─────────────────────────────────────────────────
app.get('/api/sensor', async (_req, res) => {
  try {
    const [lingRows] = await pool.query(
      `SELECT * FROM sensor_logs WHERE temp_lingkungan IS NOT NULL ORDER BY created_at DESC LIMIT 1`
    );
    const [tanRows] = await pool.query(
      `SELECT * FROM sensor_logs WHERE temp_tanaman IS NOT NULL ORDER BY created_at DESC LIMIT 1`
    );

    const ling = lingRows[0] || {};
    const tan  = tanRows[0]  || {};

    res.json({
      lingkungan: {
        temperature: ling.temp_lingkungan    ?? null,
        humidity:    ling.humidity_lingkungan ?? null,
        lux:         ling.lux               ?? null,
      },
      tanaman: {
        temperature: tan.temp_tanaman     ?? null,
        humidity:    tan.humidity_tanaman ?? null,
        ph:          tan.ph               ?? null,
        ec:          tan.ec               ?? null,
        nitrogen:    tan.nitrogen         ?? null,
        fosfor:      tan.fosfor           ?? null,
        kalium:      tan.kalium           ?? null,
      },
    });
  } catch (err) {
    console.error('GET /api/sensor error:', err);
    res.status(500).json({ error: 'Gagal ambil data sensor' });
  }
});

app.post('/api/sensor/lingkungan', async (req, res) => {
  const { temperature, humidity, lux } = req.body;
  try {
    await pool.query(
      `INSERT INTO sensor_logs (temp_lingkungan, humidity_lingkungan, lux) VALUES (?, ?, ?)`,
      [temperature, humidity, lux]
    );
    res.json({ status: 'ok' });
  } catch (err) {
    res.status(500).json({ error: 'Gagal simpan data lingkungan' });
  }
});

app.post('/api/sensor/tanaman', async (req, res) => {
  const { temperature, humidity, ph, ec, nitrogen, fosfor, kalium } = req.body;
  try {
    await pool.query(
      `INSERT INTO sensor_logs (temp_tanaman, humidity_tanaman, ph, ec, nitrogen, fosfor, kalium) VALUES (?, ?, ?, ?, ?, ?, ?)`,
      [temperature, humidity, ph, ec, nitrogen, fosfor, kalium]
    );
    res.json({ status: 'ok' });
  } catch (err) {
    res.status(500).json({ error: 'Gagal simpan data tanaman' });
  }
});

// ── History ────────────────────────────────────────────────
app.get('/api/history/sensor', async (req, res) => {
  const { dari, sampai } = req.query;
  try {
    const whereBase = dari && sampai ? ' AND DATE(created_at) BETWEEN ? AND ?' : '';
    const params    = dari && sampai ? [dari, sampai] : [];

    const [lingRows] = await pool.query(
      `SELECT * FROM sensor_logs WHERE temp_lingkungan IS NOT NULL${whereBase} ORDER BY created_at DESC LIMIT 200`,
      params
    );
    const [tanRows] = await pool.query(
      `SELECT * FROM sensor_logs WHERE temp_tanaman IS NOT NULL${whereBase} ORDER BY created_at DESC LIMIT 200`,
      params
    );

    const allRows = [...lingRows, ...tanRows]
      .sort((a, b) => new Date(b.created_at) - new Date(a.created_at))
      .slice(0, 500);

    res.json(allRows.map(r => ({
      id:         r.id,
      timestamp:  r.created_at,
      lingkungan: { temperature: r.temp_lingkungan, humidity: r.humidity_lingkungan, lux: r.lux },
      tanaman:    { temperature: r.temp_tanaman, humidity: r.humidity_tanaman, ph: r.ph, ec: r.ec, nitrogen: r.nitrogen, fosfor: r.fosfor, kalium: r.kalium },
    })));
  } catch (err) {
    res.status(500).json({ error: 'Gagal ambil history sensor' });
  }
});

app.get('/api/history/deteksi', async (req, res) => {
  const { dari, sampai } = req.query;
  try {
    let query  = 'SELECT * FROM deteksi_logs';
    const params = [];
    if (dari && sampai) { query += ' WHERE DATE(created_at) BETWEEN ? AND ?'; params.push(dari, sampai); }
    query += ' ORDER BY created_at DESC LIMIT 200';

    const [rows] = await pool.query(query, params);
    res.json(rows.map(r => ({
      id:         r.id,
      penyakit:   r.penyakit,
      confidence: r.confidence,
      timestamp:  r.created_at,
    })));
  } catch (err) {
    res.status(500).json({ error: 'Gagal ambil history deteksi' });
  }
});

// ── ESP32 ──────────────────────────────────────────────────
app.post('/api/esp32/register', async (req, res) => {
  const { ip } = req.body;
  if (!ip) return res.status(400).json({ error: 'IP diperlukan' });
  esp32State = { ip, online: true, lastSeen: new Date() };
  try {
    await pool.query(
      `INSERT INTO esp32_state (id, ip) VALUES (1, ?) ON DUPLICATE KEY UPDATE ip = VALUES(ip), last_seen = CURRENT_TIMESTAMP`,
      [ip]
    );
  } catch (err) { console.error('[ESP32] Gagal simpan state:', err.message); }
  res.json({ status: 'ok' });
});

app.get('/api/esp32/ip', (_req, res) => {
  // Anggap offline kalau ESP32 tidak kirim frame lebih dari 15 detik
  if (esp32State.lastSeen) {
    const diff = (new Date() - new Date(esp32State.lastSeen)) / 1000;
    if (diff > 15) esp32State.online = false;
  }
  // Cek juga apakah WS ESP32 masih connect
  const wsAlive = esp32WsClient !== null && esp32WsClient.readyState === 1;
  res.json({ ...esp32State, wsConnected: wsAlive });
});

app.get('/api/esp32/hasil', async (_req, res) => {
  try {
    const [rows] = await pool.query(
      'SELECT * FROM deteksi_logs ORDER BY created_at DESC LIMIT 1'
    );
    if (rows.length === 0) return res.json({ penyakit: null, confidence: 0 });
    res.json({ penyakit: rows[0].penyakit, confidence: rows[0].confidence, timestamp: rows[0].created_at });
  } catch (err) {
    res.status(500).json({ error: 'Gagal ambil hasil deteksi' });
  }
});

// Endpoint deteksi via REST (fallback — untuk kamera HP di frontend)
app.post('/api/deteksi', upload.single('foto'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: 'File foto tidak ditemukan' });

    const formData = new FormData();
    formData.append('foto', req.file.buffer, {
      filename:    'foto.jpg',
      contentType: req.file.mimetype,
    });

    const YOLO_URL = process.env.YOLO_SERVICE_URL;
    if (!YOLO_URL) return res.status(500).json({ error: 'YOLO service URL belum dikonfigurasi' });

    const response = await fetch(`${YOLO_URL}/detect`, {
      method:  'POST',
      body:    formData,
      headers: formData.getHeaders(),
      signal:  AbortSignal.timeout(110000),
    });

    const rawText = await response.text();
    if (!response.ok) return res.status(502).json({ error: `YOLO error HTTP ${response.status}`, detail: rawText.slice(0, 200) });

    let hasil;
    try { hasil = JSON.parse(rawText); }
    catch (_) { return res.status(502).json({ error: 'YOLO response tidak valid', detail: rawText.slice(0, 200) }); }

    await pool.query(
      'INSERT INTO deteksi_logs (penyakit, confidence) VALUES (?, ?)',
      [hasil.penyakit || 'Tidak terdeteksi', hasil.confidence || 0]
    );

    res.json(hasil);
  } catch (err) {
    console.error('[DETEKSI] Error:', err);
    res.status(500).json({ error: 'Gagal konek ke YOLO service' });
  }
});

// Legacy endpoints kamera
const ESP32CAM_STREAM_URL  = process.env.ESP32CAM_STREAM_URL  || 'http://10.247.104.11';
const ESP32CAM_CAPTURE_URL = process.env.ESP32CAM_CAPTURE_URL || 'http://10.247.104.11:81/capture';

app.get('/api/camera', (_req, res) => {
  res.json({ stream_url: ESP32CAM_STREAM_URL, capture_url: ESP32CAM_CAPTURE_URL });
});

// ── Start server ───────────────────────────────────────────
server.listen(PORT, () => {
  console.log(`Server berjalan di http://localhost:${PORT}`);

  // Keep-alive YOLO
  const YOLO_URL = process.env.YOLO_SERVICE_URL;
  if (YOLO_URL) {
    setInterval(async () => {
      try {
        const r = await fetch(`${YOLO_URL}/health`, { signal: AbortSignal.timeout(10000) });
        console.log(`[PING] YOLO health: ${r.status}`);
      } catch (e) { console.warn('[PING] YOLO tidak response:', e.message); }
    }, 4 * 60 * 1000);
    console.log('[PING] Keep-alive YOLO aktif');
  }

  // Self ping
  const SELF_URL = process.env.RAILWAY_PUBLIC_DOMAIN
    ? `https://${process.env.RAILWAY_PUBLIC_DOMAIN}` : null;
  if (SELF_URL) {
    setInterval(async () => {
      try {
        const r = await fetch(`${SELF_URL}/`, { signal: AbortSignal.timeout(10000) });
        console.log(`[PING] Self health: ${r.status}`);
      } catch (e) { console.warn('[PING] Self ping gagal:', e.message); }
    }, 4 * 60 * 1000);
    console.log(`[PING] Keep-alive self aktif: ${SELF_URL}`);
  }
});
