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
// Raw buffer untuk frame ESP32 — harus sebelum bodyParser.json
app.use('/api/esp32/frame', express.raw({ type: 'image/jpeg', limit: '200kb' }));
app.use(bodyParser.json());

// ── HTTP + WebSocket Server ────────────────────────────
const server = http.createServer(app);
const wss    = new WebSocketServer({ server });

// Simpan semua frontend client yang subscribe live frame
const frontendClients = new Set();
let esp32WsClient = null; // koneksi WebSocket dari ESP32

wss.on('connection', (ws, req) => {
  const url = req.url || '';

  if (url === '/ws/esp32') {
    // Koneksi dari ESP32
    console.log('[WS] ESP32 terhubung');
    esp32WsClient = ws;
    esp32State.online   = true;
    esp32State.lastSeen = new Date();

    ws.on('message', (data, isBinary) => {
      if (!isBinary) return; // hanya terima binary (JPEG)
      esp32Frame          = data;
      esp32State.online   = true;
      esp32State.lastSeen = new Date();

      // Broadcast ke semua frontend client
      for (const client of frontendClients) {
        if (client.readyState === 1) { // OPEN
          client.send(data, { binary: true });
        }
      }
    });

    ws.on('close', () => {
      console.log('[WS] ESP32 disconnect');
      esp32WsClient     = null;
      esp32State.online = false;
    });

  } else if (url === '/ws/frame') {
    // Koneksi dari frontend
    console.log('[WS] Frontend client terhubung');
    frontendClients.add(ws);

    ws.on('close', () => {
      frontendClients.delete(ws);
    });
  }
});

// ── MySQL Connection Pool ──────────────────────────────
let pool;

if (process.env.MYSQL_URL) {
  // Parse MYSQL_URL: mysql://user:pass@host:port/database
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
  console.log(`MySQL terhubung ke ${url.hostname}:${url.port}${url.pathname}`);
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
  console.log('MySQL terhubung ke localhost');
}

// ── IP ESP32-CAM ───────────────────────────────────────
const ESP32CAM_STREAM_URL  = process.env.ESP32CAM_STREAM_URL  || 'http://10.247.104.11';
const ESP32CAM_CAPTURE_URL = process.env.ESP32CAM_CAPTURE_URL || 'http://10.247.104.11:81/capture';

// ── MQTT Subscriber — terima data sensor ──────────────
const mqtt = require('mqtt');

const MQTT_URL      = process.env.MQTT_URL      || 'mqtt://202.10.40.129:1883';
const MQTT_USER     = process.env.MQTT_USER     || 'greenhouse';
const MQTT_PASS     = process.env.MQTT_PASS     || 'Vanilijaya12';
const MQTT_TOPIC    = process.env.MQTT_TOPIC    || 'iot/sensor/data';

const mqttClient = mqtt.connect(MQTT_URL, {
  username:       MQTT_USER,
  password:       MQTT_PASS,
  clientId:       'backend_railway_' + Math.random().toString(16).slice(2),
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
    const keys = Object.keys(payload);
    console.log('[MQTT] Keys:', JSON.stringify(keys));
    console.log('[MQTT] Data masuk:', JSON.stringify(payload));

    // Tipe lingkungan: suhu_light, lembab_light, intensitas
    if (payload.suhu_light !== undefined || payload.lembab_light !== undefined) {
      await pool.query(
        `INSERT INTO sensor_logs (temp_lingkungan, humidity_lingkungan, lux)
         VALUES (?, ?, ?)`,
        [payload.suhu_light ?? null, payload.lembab_light ?? null, payload.intensitas ?? null]
      );
      console.log('[MQTT] Data lingkungan tersimpan');
      return;
    }

    // Tipe tanaman zone 1: suhu_soil, lembab_soil, conductivity, ph, n, p, k
    if (payload.suhu_soil !== undefined) {
      await pool.query(
        `INSERT INTO sensor_logs (temp_tanaman, humidity_tanaman, ec, ph, nitrogen, fosfor, kalium)
         VALUES (?, ?, ?, ?, ?, ?, ?)`,
        [payload.suhu_soil ?? null, payload.lembab_soil ?? null, payload.conductivity ?? null,
         payload.ph ?? null, payload.n ?? null, payload.p ?? null, payload.k ?? null]
      );
      console.log('[MQTT] Data tanaman zone1 tersimpan');
      return;
    }

    // Tipe tanaman zone 2: suhu_soil2, lembab_soil2, conductivity2, ph2, n2, p2, k2
    if (payload.suhu_soil2 !== undefined) {
      await pool.query(
        `INSERT INTO sensor_logs (temp_tanaman, humidity_tanaman, ec, ph, nitrogen, fosfor, kalium)
         VALUES (?, ?, ?, ?, ?, ?, ?)`,
        [payload.suhu_soil2 ?? null, payload.lembab_soil2 ?? null, payload.conductivity2 ?? null,
         payload.ph2 ?? null, payload.n2 ?? null, payload.p2 ?? null, payload.k2 ?? null]
      );
      console.log('[MQTT] Data tanaman zone2 tersimpan');
      return;
    }

    console.log('[MQTT] Format tidak dikenal, skip');
  } catch (err) {
    console.error('[MQTT] Gagal proses pesan:', err.message);
  }
});

mqttClient.on('error',      (err) => console.error('[MQTT] Error:', err.message));
mqttClient.on('disconnect', ()    => console.log('[MQTT] Disconnect dari broker'));
mqttClient.on('reconnect',  ()    => console.log('[MQTT] Reconnecting...'));

// ── Auto-migrate: buat tabel kalau belum ada ──────────
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

// ── Health check ───────────────────────────────────────
app.get('/', (_req, res) => {
  res.json({ status: 'ok', message: 'Smart Farm Backend is running' });
});

// ══════════════════════════════════════════════════════
// SENSOR — data terkini (ambil row terakhir dari DB)
// ══════════════════════════════════════════════════════

app.get('/api/sensor', async (_req, res) => {
  try {
    // Ambil data lingkungan terbaru (yang punya suhu_light/lembab_light)
    const [lingRows] = await pool.query(
      `SELECT * FROM sensor_logs 
       WHERE temp_lingkungan IS NOT NULL 
       ORDER BY created_at DESC LIMIT 1`
    );
    // Ambil data tanaman terbaru (yang punya temp_tanaman)
    const [tanRows] = await pool.query(
      `SELECT * FROM sensor_logs 
       WHERE temp_tanaman IS NOT NULL 
       ORDER BY created_at DESC LIMIT 1`
    );

    const ling = lingRows[0] || {};
    const tan  = tanRows[0]  || {};

    if (!lingRows.length && !tanRows.length) {
      // Belum ada data, return default kosong (bukan error)
      return res.json({
        lingkungan: { temperature: null, humidity: null, lux: null },
        tanaman:    { temperature: null, humidity: null, ph: null, ec: null, nitrogen: null, fosfor: null, kalium: null },
      });
    }

    res.json({
      lingkungan: {
        temperature: ling.temp_lingkungan  ?? null,
        humidity:    ling.humidity_lingkungan ?? null,
        lux:         ling.lux              ?? null,
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

// POST — ESP32 kirim data lingkungan → simpan ke DB
app.post('/api/sensor/lingkungan', async (req, res) => {
  const { temperature, humidity, lux } = req.body;
  try {
    await pool.query(
      `INSERT INTO sensor_logs (temp_lingkungan, humidity_lingkungan, lux)
       VALUES (?, ?, ?)`,
      [temperature, humidity, lux]
    );
    console.log('Data lingkungan disimpan:', req.body);
    res.json({ status: 'ok' });
  } catch (err) {
    console.error('POST /api/sensor/lingkungan error:', err);
    res.status(500).json({ error: 'Gagal simpan data lingkungan' });
  }
});

// POST — ESP32 kirim data tanaman → simpan ke DB
app.post('/api/sensor/tanaman', async (req, res) => {
  const { temperature, humidity, ph, ec, nitrogen, fosfor, kalium } = req.body;
  try {
    await pool.query(
      `INSERT INTO sensor_logs (temp_tanaman, humidity_tanaman, ph, ec, nitrogen, fosfor, kalium)
       VALUES (?, ?, ?, ?, ?, ?, ?)`,
      [temperature, humidity, ph, ec, nitrogen, fosfor, kalium]
    );
    console.log('Data tanaman disimpan:', req.body);
    res.json({ status: 'ok' });
  } catch (err) {
    console.error('POST /api/sensor/tanaman error:', err);
    res.status(500).json({ error: 'Gagal simpan data tanaman' });
  }
});

// ══════════════════════════════════════════════════════
// HISTORY — dari DB dengan filter tanggal
// ══════════════════════════════════════════════════════

// GET — history sensor
app.get('/api/history/sensor', async (req, res) => {
  const { dari, sampai } = req.query;
  try {
    let whereBase = '';
    const params = [];

    if (dari && sampai) {
      whereBase = ' AND DATE(created_at) BETWEEN ? AND ?';
      params.push(dari, sampai);
      params.push(dari, sampai);
    }

    // Ambil history lingkungan (hanya row yang punya data lingkungan)
    const [lingRows] = await pool.query(
      `SELECT * FROM sensor_logs WHERE temp_lingkungan IS NOT NULL${whereBase} ORDER BY created_at DESC LIMIT 200`,
      dari && sampai ? [dari, sampai] : []
    );

    // Ambil history tanaman (hanya row yang punya data tanaman)
    const [tanRows] = await pool.query(
      `SELECT * FROM sensor_logs WHERE temp_tanaman IS NOT NULL${whereBase} ORDER BY created_at DESC LIMIT 200`,
      dari && sampai ? [dari, sampai] : []
    );

    // Gabung dan sort berdasarkan waktu
    const allRows = [...lingRows, ...tanRows].sort(
      (a, b) => new Date(b.created_at) - new Date(a.created_at)
    ).slice(0, 500);

    const result = allRows.map(r => ({
      id:        r.id,
      timestamp: r.created_at,
      lingkungan: {
        temperature: r.temp_lingkungan,
        humidity:    r.humidity_lingkungan,
        lux:         r.lux,
      },
      tanaman: {
        temperature: r.temp_tanaman,
        humidity:    r.humidity_tanaman,
        ph:          r.ph,
        ec:          r.ec,
        nitrogen:    r.nitrogen,
        fosfor:      r.fosfor,
        kalium:      r.kalium,
      },
    }));
    res.json(result);
  } catch (err) {
    console.error('GET /api/history/sensor error:', err);
    res.status(500).json({ error: 'Gagal ambil history sensor' });
  }
});

// GET — history deteksi
app.get('/api/history/deteksi', async (req, res) => {
  const { dari, sampai } = req.query;
  try {
    let query    = 'SELECT * FROM deteksi_logs';
    const params = [];

    if (dari && sampai) {
      query += ' WHERE DATE(created_at) BETWEEN ? AND ?';
      params.push(dari, sampai);
    }
    query += ' ORDER BY created_at DESC LIMIT 200';

    const [rows] = await pool.query(query, params);
    const result = rows.map(r => ({
      id:         r.id,
      penyakit:   r.penyakit,
      confidence: r.confidence,
      timestamp:  r.created_at,
    }));
    res.json(result);
  } catch (err) {
    console.error('GET /api/history/deteksi error:', err);
    res.status(500).json({ error: 'Gagal ambil history deteksi' });
  }
});

// ══════════════════════════════════════════════════════
// ESP32-CAM
// ══════════════════════════════════════════════════════

// State IP ESP32 terkini (cache di memori + backup ke DB)
let esp32State = { ip: null, online: false, lastSeen: null };
let esp32Frame = null; // Buffer JPEG frame terakhir dari ESP32

// Load state ESP32 dari DB saat server start
(async () => {
  try {
    const [rows] = await pool.query(
      "SELECT * FROM esp32_state LIMIT 1"
    );
    if (rows.length > 0) {
      esp32State = {
        ip:       rows[0].ip,
        online:   false,
        lastSeen: rows[0].last_seen,
      };
      console.log('ESP32 state loaded dari DB:', esp32State.ip);
    }
  } catch (err) {
    console.error('Gagal load esp32_state:', err.message);
  }
})();

// POST — ESP32 push frame JPEG ke backend (binary langsung)
app.post('/api/esp32/frame', (req, res) => {
  // req.body sudah berupa Buffer dari express.raw()
  const buf = Buffer.isBuffer(req.body) ? req.body : Buffer.concat([]);

  // Fallback: baca stream manual kalau express.raw tidak jalan
  if (!buf || buf.length < 100) {
    const chunks = [];
    req.on('data', chunk => chunks.push(chunk));
    req.on('end', () => {
      const rawBuf = Buffer.concat(chunks);
      console.log(`[FRAME] Stream fallback: ${rawBuf.length} bytes`);
      if (rawBuf.length < 100) return res.status(400).json({ error: 'Frame terlalu kecil' });
      esp32Frame          = rawBuf;
      esp32State.online   = true;
      esp32State.lastSeen = new Date();
      for (const client of frontendClients) {
        if (client.readyState === 1) client.send(rawBuf, { binary: true });
      }
      res.json({ status: 'ok' });
    });
    return;
  }

  console.log(`[FRAME] Diterima: ${buf.length} bytes`);
  esp32Frame          = buf;
  esp32State.online   = true;
  esp32State.lastSeen = new Date();
  for (const client of frontendClients) {
    if (client.readyState === 1) client.send(buf, { binary: true });
  }
  res.json({ status: 'ok' });
});

// GET — frontend ambil frame terakhir
app.get('/api/esp32/frame', (_req, res) => {
  if (!esp32Frame) {
    return res.status(503).json({ error: 'Belum ada frame' });
  }
  // Anggap stale kalau ESP32 tidak kirim frame lebih dari 5 detik
  // (frameTask kirim tiap 1.2 detik, jadi 5 detik = toleransi 4x miss)
  if (esp32State.lastSeen) {
    const diffSec = (new Date() - new Date(esp32State.lastSeen)) / 1000;
    if (diffSec > 5) {
      esp32State.online = false;
      return res.status(503).json({ error: 'ESP32 offline (frame stale)', lastSeen: esp32State.lastSeen });
    }
  }
  res.setHeader('Content-Type', 'image/jpeg');
  res.setHeader('Cache-Control', 'no-cache, no-store');
  res.send(esp32Frame);
});

// POST — ESP32 register IP saat nyala
app.post('/api/esp32/register', async (req, res) => {
  const { ip } = req.body;
  if (!ip) return res.status(400).json({ error: 'IP diperlukan' });
  esp32State = { ip, online: true, lastSeen: new Date() };
  console.log('ESP32 register IP:', ip);
  // Simpan ke DB
  try {
    await pool.query(
      `INSERT INTO esp32_state (id, ip) VALUES (1, ?)
       ON DUPLICATE KEY UPDATE ip = VALUES(ip), last_seen = CURRENT_TIMESTAMP`,
      [ip]
    );
  } catch (err) {
    console.error('Gagal simpan esp32_state:', err.message);
  }
  res.json({ status: 'ok' });
});

// GET — cek status & IP ESP32
app.get('/api/esp32/ip', (_req, res) => {
  // Anggap offline kalau lebih dari 15 detik tidak ada frame masuk
  if (esp32State.lastSeen) {
    const diff = (new Date() - new Date(esp32State.lastSeen)) / 1000;
    if (diff > 15) esp32State.online = false;
  }
  res.json(esp32State);
});

// GET — frontend ambil hasil deteksi terbaru
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

// GET — proxy capture foto dari ESP32-CAM
app.get('/api/esp32/capture', async (_req, res) => {
  if (!esp32State.ip) {
    return res.status(503).json({ error: 'ESP32-CAM tidak online' });
  }
  try {
    const response = await fetch(`http://${esp32State.ip}/capture`, {
      signal: AbortSignal.timeout(10000),
    });
    if (!response.ok) {
      esp32State.online = false;
      return res.status(500).json({ error: 'Gagal capture' });
    }
    const buffer = await response.buffer();
    // Berhasil capture = ESP32 online
    esp32State.online   = true;
    esp32State.lastSeen = new Date();
    res.setHeader('Content-Type', 'image/jpeg');
    res.send(buffer);
  } catch (err) {
    esp32State.online = false;
    console.error('Gagal capture ESP32-CAM:', err);
    res.status(500).json({ error: 'Gagal konek ke ESP32-CAM' });
  }
});

// GET — kirim URL kamera ke web (legacy)
app.get('/api/camera', (_req, res) => {
  res.json({
    stream_url:  ESP32CAM_STREAM_URL,
    capture_url: ESP32CAM_CAPTURE_URL,
  });
});

// GET — proxy capture dari ESP32-CAM (legacy)
app.get('/api/camera/capture', async (_req, res) => {
  try {
    const response = await fetch(ESP32CAM_CAPTURE_URL);
    if (!response.ok) {
      return res.status(500).json({ error: 'Gagal mengambil gambar dari ESP32-CAM' });
    }
    const buffer = await response.buffer();
    res.setHeader('Content-Type', 'image/jpeg');
    res.send(buffer);
  } catch (err) {
    console.error('Gagal capture ESP32-CAM:', err);
    res.status(500).json({ error: 'Gagal konek ke ESP32-CAM' });
  }
});

// ══════════════════════════════════════════════════════
// DETEKSI PENYAKIT — kirim ke YOLO, simpan hasil ke DB
// ══════════════════════════════════════════════════════

app.post('/api/deteksi', upload.single('foto'), async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ error: 'File foto tidak ditemukan' });
    }

    const formData = new FormData();
    formData.append('foto', req.file.buffer, {
      filename:    'foto.jpg',
      contentType: req.file.mimetype,
    });

    const YOLO_URL = process.env.YOLO_SERVICE_URL;
    if (!YOLO_URL) {
      console.error('YOLO_SERVICE_URL tidak di-set!');
      return res.status(500).json({ error: 'YOLO service URL belum dikonfigurasi' });
    }
    const response = await fetch(`${YOLO_URL}/detect`, {
      method:  'POST',
      body:    formData,
      headers: formData.getHeaders(),
      signal:  AbortSignal.timeout(60000), // 60 detik timeout
    });

    // Baca body sebagai text dulu — hindari crash kalau YOLO return non-JSON
    const rawText = await response.text();

    if (!response.ok) {
      console.error(`YOLO HTTP ${response.status}:`, rawText.slice(0, 200));
      return res.status(502).json({
        error: `YOLO service error (HTTP ${response.status})`,
        detail: rawText.slice(0, 200),
      });
    }

    let hasil;
    try {
      hasil = JSON.parse(rawText);
    } catch (_) {
      console.error('YOLO return non-JSON:', rawText.slice(0, 200));
      return res.status(502).json({
        error: 'YOLO service return response tidak valid',
        detail: rawText.slice(0, 200),
      });
    }

    // Simpan hasil deteksi ke DB
    await pool.query(
      'INSERT INTO deteksi_logs (penyakit, confidence) VALUES (?, ?)',
      [hasil.penyakit || 'Tidak terdeteksi', hasil.confidence || 0]
    );

    res.json(hasil);
  } catch (err) {
    console.error('Gagal deteksi:', err);
    res.status(500).json({ error: 'Gagal konek ke YOLO service' });
  }
});

// ── Start ──────────────────────────────────────────────
server.listen(PORT, () => {
  console.log(`Server jalan di http://localhost:${PORT}`);
  console.log(`ESP32-CAM Stream : ${ESP32CAM_STREAM_URL}`);
  console.log(`ESP32-CAM Capture: ${ESP32CAM_CAPTURE_URL}`);

  // ── Keep-alive pinger — cegah Railway sleep ──────────
  // Hit YOLO /health tiap 4 menit supaya service tidak tidur
  const YOLO_URL = process.env.YOLO_SERVICE_URL;
  if (YOLO_URL) {
    setInterval(async () => {
      try {
        const r = await fetch(`${YOLO_URL}/health`, { signal: AbortSignal.timeout(10000) });
        console.log(`[PING] YOLO health: ${r.status}`);
      } catch (e) {
        console.warn('[PING] YOLO tidak response:', e.message);
      }
    }, 4 * 60 * 1000); // tiap 4 menit
    console.log('[PING] Keep-alive YOLO aktif');
  }
});
