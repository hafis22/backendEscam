-- ============================================================
-- Smart Farm - Railway MySQL Init Script
-- Jalankan sekali saat pertama kali setup MySQL di Railway
-- Database: railway (sudah dibuat otomatis oleh Railway)
-- ============================================================

CREATE TABLE IF NOT EXISTS `sensor_logs` (
  `id` int NOT NULL AUTO_INCREMENT,
  `temp_lingkungan` float DEFAULT NULL,
  `humidity_lingkungan` float DEFAULT NULL,
  `lux` float DEFAULT NULL,
  `temp_tanaman` float DEFAULT NULL,
  `humidity_tanaman` float DEFAULT NULL,
  `ph` float DEFAULT NULL,
  `ec` float DEFAULT NULL,
  `nitrogen` float DEFAULT NULL,
  `fosfor` float DEFAULT NULL,
  `kalium` float DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS `deteksi_logs` (
  `id` int NOT NULL AUTO_INCREMENT,
  `penyakit` varchar(100) DEFAULT NULL,
  `confidence` float DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS `esp32_state` (
  `id` int NOT NULL DEFAULT 1,
  `ip` varchar(20) DEFAULT NULL,
  `last_seen` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
