const express    = require('express');
const cors       = require('cors');
const bodyParser = require('body-parser');
const multer     = require('multer');
const FormData   = require('form-data');
const fetch      = require('node-fetch');
const mysql      = require('mysql2/promise');

const app    = express();
const PORT   = process.env.PORT || 5000;
const upload = multer({ storage: multer.memoryStorage() });

app.use(cors());
app.use(bodyParser.json());

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
    database:           process.env.DB_NAME     || 'smart_farm_hafis',
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

// ── Health check ───────────────────────────────────────
app.get('/', (_req, res) => {
  res.json({ status: 'ok', message: 'Smart Farm Backend is running' });
});

// ══════════════════════════════════════════════════════
// SENSOR — data terkini (ambil row terakhir dari DB)
// ══════════════════════════════════════════════════════

app.get('/api/sensor', async (_req, res) => {
  try {
    const [rows] = await pool.query(
      'SELECT * FROM sensor_logs ORDER BY created_at DESC LIMIT 1'
    );
    if (rows.length === 0) {
      return res.status(404).json({ error: 'Belum ada data sensor' });
    }
    const r = rows[0];
    res.json({
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
    let query  = 'SELECT * FROM sensor_logs';
    const params = [];

    if (dari && sampai) {
      query += ' WHERE DATE(created_at) BETWEEN ? AND ?';
      params.push(dari, sampai);
    }
    query += ' ORDER BY created_at DESC LIMIT 500';

    const [rows] = await pool.query(query, params);
    const result = rows.map(r => ({
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

// State IP ESP32 terkini
let esp32State = { ip: null, online: false, lastSeen: null };

// POST — ESP32 register IP saat nyala
app.post('/api/esp32/register', (req, res) => {
  const { ip } = req.body;
  if (!ip) return res.status(400).json({ error: 'IP diperlukan' });
  esp32State = { ip, online: true, lastSeen: new Date() };
  console.log('ESP32 register IP:', ip);
  res.json({ status: 'ok' });
});

// GET — cek status & IP ESP32
app.get('/api/esp32/ip', (_req, res) => {
  // Anggap offline kalau lebih dari 10 menit tidak ada kabar
  if (esp32State.lastSeen) {
    const diff = (new Date() - new Date(esp32State.lastSeen)) / 1000;
    if (diff > 600) esp32State.online = false;
  }
  res.json(esp32State);
});

// GET — proxy stream MJPEG dari ESP32-CAM
app.get('/api/esp32/stream', async (req, res) => {
  if (!esp32State.ip || !esp32State.online) {
    return res.status(503).json({ error: 'ESP32-CAM tidak online' });
  }
  try {
    const response = await fetch(`http://${esp32State.ip}/stream`, {
      signal: AbortSignal.timeout(10000),
    });
    res.setHeader('Content-Type', response.headers.get('content-type') || 'multipart/x-mixed-replace');
    res.setHeader('Cache-Control', 'no-cache');
    response.body.pipe(res);
    req.on('close', () => response.body.destroy());
  } catch (err) {
    console.error('Gagal proxy stream:', err);
    res.status(500).json({ error: 'Gagal konek ke ESP32-CAM' });
  }
});

// GET — proxy capture foto dari ESP32-CAM
app.get('/api/esp32/capture', async (_req, res) => {
  if (!esp32State.ip || !esp32State.online) {
    return res.status(503).json({ error: 'ESP32-CAM tidak online' });
  }
  try {
    const response = await fetch(`http://${esp32State.ip}/capture`, {
      signal: AbortSignal.timeout(10000),
    });
    if (!response.ok) return res.status(500).json({ error: 'Gagal capture' });
    const buffer = await response.buffer();
    res.setHeader('Content-Type', 'image/jpeg');
    res.send(buffer);
  } catch (err) {
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

    const YOLO_URL = process.env.YOLO_SERVICE_URL || 'http://localhost:8000';
    const response = await fetch(`${YOLO_URL}/detect`, {
      method:  'POST',
      body:    formData,
      headers: formData.getHeaders(),
    });

    const hasil = await response.json();

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
app.listen(PORT, () => {
  console.log(`Server jalan di http://localhost:${PORT}`);
  console.log(`ESP32-CAM Stream : ${ESP32CAM_STREAM_URL}`);
  console.log(`ESP32-CAM Capture: ${ESP32CAM_CAPTURE_URL}`);
});
