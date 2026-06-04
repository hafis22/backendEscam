const express    = require('express');
const cors       = require('cors');
const bodyParser = require('body-parser');
const multer     = require('multer');
const FormData   = require('form-data');
const fetch      = require('node-fetch');

const app    = express();
const PORT   = process.env.PORT || 5000;
const upload = multer({ storage: multer.memoryStorage() });

app.use(cors());
app.use(bodyParser.json());

// IP ESP32-CAM
const ESP32CAM_STREAM_URL  = 'http://10.247.104.11';
const ESP32CAM_CAPTURE_URL = 'http://10.247.104.11:81/capture';

// ── Helper ─────────────────────────────────────────────
function rand(min, max) {
  return +(min + Math.random() * (max - min)).toFixed(2);
}

// ── Data sensor di memori ──────────────────────────────
let dataSensor = {
  lingkungan: {
    temperature: rand(24, 32),
    humidity: rand(60, 85),
    lux: rand(800, 1800),
  },
  tanaman: {
    temperature: rand(22, 30),
    humidity: rand(70, 90),
    ph: rand(5.5, 7.5),
    ec: rand(0.8, 2.0),
    nitrogen: rand(100, 220),
    fosfor: rand(25, 80),
    kalium: rand(150, 280),
  },
};

// Update dummy tiap 3 detik
setInterval(() => {
  dataSensor = {
    lingkungan: {
      temperature: rand(24, 32),
      humidity: rand(60, 85),
      lux: rand(800, 1800),
    },
    tanaman: {
      temperature: rand(22, 30),
      humidity: rand(70, 90),
      ph: rand(5.5, 7.5),
      ec: rand(0.8, 2.0),
      nitrogen: rand(100, 220),
      fosfor: rand(25, 80),
      kalium: rand(150, 280),
    },
  };
}, 3000);

// ── Routes Sensor ──────────────────────────────────────

// GET — ambil data sensor terkini
app.get('/api/sensor', (req, res) => {
  res.json(dataSensor);
});

// POST — ESP32 kirim data lingkungan
app.post('/api/sensor/lingkungan', (req, res) => {
  dataSensor.lingkungan = req.body;
  console.log('Data lingkungan:', req.body);
  res.json({ status: 'ok' });
});

// POST — ESP32 kirim data tanaman
app.post('/api/sensor/tanaman', (req, res) => {
  dataSensor.tanaman = req.body;
  console.log('Data tanaman:', req.body);
  res.json({ status: 'ok' });
});

// ── Routes ESP32-CAM ───────────────────────────────────

// GET — kirim URL kamera ke web
app.get('/api/camera', (req, res) => {
  res.json({
    stream_url: ESP32CAM_STREAM_URL,
    capture_url: ESP32CAM_CAPTURE_URL,
  });
});

// GET — proxy capture dari ESP32-CAM
app.get('/api/camera/capture', async (req, res) => {
  try {
    const response = await fetch(ESP32CAM_CAPTURE_URL);

    if (!response.ok) {
      return res.status(500).json({
        error: 'Gagal mengambil gambar dari ESP32-CAM',
      });
    }

    const buffer = await response.buffer();

    res.setHeader('Content-Type', 'image/jpeg');
    res.send(buffer);
  } catch (err) {
    console.error('Gagal capture ESP32-CAM:', err);
    res.status(500).json({
      error: 'Gagal konek ke ESP32-CAM',
    });
  }
});

// ── Routes History ─────────────────────────────────────

// GET — history sensor dummy
app.get('/api/history/sensor', (req, res) => {
  const dummy = Array.from({ length: 10 }, (_, i) => {
    const d = new Date();
    d.setMinutes(d.getMinutes() - i * 10);

    return {
      id: i,
      timestamp: d.toISOString(),
      lingkungan: {
        temperature: rand(24, 32),
        humidity: rand(60, 85),
        lux: rand(800, 1800),
      },
      tanaman: {
        temperature: rand(22, 30),
        humidity: rand(70, 90),
        ph: rand(5.5, 7.5),
        ec: rand(0.8, 2.0),
        nitrogen: rand(100, 220),
        fosfor: rand(25, 80),
        kalium: rand(150, 280),
      },
    };
  });

  res.json(dummy);
});

// GET — history deteksi dummy
app.get('/api/history/deteksi', (req, res) => {
  const penyakitList = ['Sehat', 'Leaf spot', 'Anthracnose', 'Fusarium wilt'];

  const dummy = Array.from({ length: 5 }, (_, i) => {
    const d = new Date();
    d.setHours(d.getHours() - i);

    const penyakit = penyakitList[Math.floor(Math.random() * penyakitList.length)];

    return {
      id: i,
      penyakit,
      confidence: rand(80, 99),
      timestamp: d.toISOString(),
    };
  });

  res.json(dummy);
});

// POST — deteksi penyakit ke YOLO service
app.post('/api/deteksi', upload.single('foto'), async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({
        error: 'File foto tidak ditemukan',
      });
    }

    const formData = new FormData();

    formData.append('foto', req.file.buffer, {
      filename: 'foto.jpg',
      contentType: req.file.mimetype,
    });

    const YOLO_URL = process.env.YOLO_SERVICE_URL || 'http://localhost:8000';
    const response = await fetch(`${YOLO_URL}/detect`, {
      method: 'POST',
      body: formData,
      headers: formData.getHeaders(),
    });

    const hasil = await response.json();a
    res.json(hasil);
  } catch (err) {
    console.error('Gagal deteksi:', err);
    res.status(500).json({
      error: 'Gagal konek ke YOLO service',
    });
  }
});

// ── Start ──────────────────────────────────────────────
app.listen(PORT, () => {
  console.log(`Server jalan di http://localhost:${PORT}`);
  console.log(`ESP32-CAM Stream : ${ESP32CAM_STREAM_URL}`);
  console.log(`ESP32-CAM Capture: ${ESP32CAM_CAPTURE_URL}`);
});