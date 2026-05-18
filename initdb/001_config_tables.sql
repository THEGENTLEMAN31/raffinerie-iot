CREATE TABLE IF NOT EXISTS machine_config (
    machine_id  TEXT PRIMARY KEY,
    machine_type TEXT NOT NULL DEFAULT 'pipe',
    enabled     BOOLEAN DEFAULT true,
    target_temp FLOAT DEFAULT 95.0,
    target_vib  FLOAT DEFAULT 1.2,
    alpha_temp  FLOAT DEFAULT 0.05,
    alpha_vib   FLOAT DEFAULT 0.2,
    created_at  TIMESTAMPTZ DEFAULT now(),
    updated_at  TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS alert_config (
    id              SERIAL PRIMARY KEY,
    machine_id      TEXT,
    type_capteur    TEXT,
    seuil_anomalie  FLOAT DEFAULT 0.7,
    updated_at      TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id          SERIAL PRIMARY KEY,
    component   TEXT NOT NULL,
    action      TEXT NOT NULL,
    status      TEXT NOT NULL,
    message     TEXT,
    started_at  TIMESTAMPTZ DEFAULT now(),
    finished_at TIMESTAMPTZ
);

INSERT INTO machine_config (machine_id, machine_type, enabled, target_temp, target_vib)
VALUES ('pipe-101', 'pipe', true, 95.0, 1.2)
ON CONFLICT (machine_id) DO NOTHING;

INSERT INTO alert_config (machine_id, type_capteur, seuil_anomalie)
SELECT NULL, NULL, 0.7
WHERE NOT EXISTS (SELECT 1 FROM alert_config WHERE machine_id IS NULL AND type_capteur IS NULL);
