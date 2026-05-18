import os, time, json
from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import psycopg2
import docker

DB_HOST = os.getenv("DB_HOST", "timescaledb")
DB_NAME = os.getenv("DB_NAME", "iotdb")
DB_USER = os.getenv("DB_USER", "admin")
DB_PASS = os.getenv("DB_PASS", "admin")

app = FastAPI(title="Raffinerie IoT - Interface de Controle")
templates = Jinja2Templates(directory="templates")

def db():
    return psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)

def docker_client():
    return docker.from_env()

# --- PAGES ---

@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request):
    status = {"simulateur": "inconnu", "spark": "inconnu", "bridge": "inconnu"}
    try:
        c = docker_client()
        for container in c.containers.list(all=True):
            name = container.name
            state = container.status
            if name == "simulateur":
                status["simulateur"] = state
            elif name == "mqtt-bridge":
                status["bridge"] = state
    except:
        pass
    spark_procs = 0
    try:
        c = docker_client()
        spark_container = c.containers.get("spark-master")
        exec_result = spark_container.exec_run("ps aux")
        spark_procs = exec_result.output.decode().count("traitement_kpi.py")
    except:
        pass
    status["spark"] = "running" if spark_procs > 0 else "stopped"

    derniere_pred = "aucune"
    try:
        conn = db()
        cur = conn.cursor()
        cur.execute("SELECT score_anomalie, type_capteur, timestamp FROM alertes_predictions ORDER BY timestamp DESC LIMIT 1")
        row = cur.fetchone()
        if row:
            score, capteur, ts = row
            derniere_pred = f"{capteur}: {score:.4f} a {ts}"
        cur.close()
        conn.close()
    except:
        pass

    nb_machines = 0
    machines = []
    try:
        conn = db()
        cur = conn.cursor()
        cur.execute("SELECT machine_id, machine_type, enabled FROM machine_config ORDER BY machine_id")
        machines = cur.fetchall()
        cur.execute("SELECT count(*) FROM machine_config WHERE enabled = true")
        nb_machines = cur.fetchone()[0]
        cur.close()
        conn.close()
    except:
        pass

    return templates.TemplateResponse(request, "dashboard.html", {
        "status": status,
        "derniere_pred": derniere_pred,
        "nb_machines": nb_machines,
        "machines": machines
    })

@app.get("/config", response_class=HTMLResponse)
def config_page(request: Request):
    machines = []
    try:
        conn = db()
        cur = conn.cursor()
        cur.execute("SELECT machine_id, machine_type, enabled, target_temp, target_vib, alpha_temp, alpha_vib FROM machine_config ORDER BY machine_id")
        machines = cur.fetchall()
        cur.close()
        conn.close()
    except:
        pass

    seuil = 0.7
    try:
        conn = db()
        cur = conn.cursor()
        cur.execute("SELECT seuil_anomalie FROM alert_config WHERE machine_id IS NULL AND type_capteur IS NULL LIMIT 1")
        row = cur.fetchone()
        if row:
            seuil = row[0]
        cur.close()
        conn.close()
    except:
        pass

    return templates.TemplateResponse(request, "config.html", {
        "machines": machines,
        "seuil": seuil
    })

@app.get("/status", response_class=HTMLResponse)
def status_fragment(request: Request):
    status = {"simulateur": "inconnu", "spark": "inconnu", "bridge": "inconnu"}
    try:
        c = docker_client()
        for container in c.containers.list(all=True):
            name = container.name
            state = container.status
            if name == "simulateur":
                status["simulateur"] = state
            elif name == "mqtt-bridge":
                status["bridge"] = state
    except:
        pass
    spark_procs = 0
    try:
        c = docker_client()
        spark_container = c.containers.get("spark-master")
        exec_result = spark_container.exec_run("ps aux")
        spark_procs = exec_result.output.decode().count("traitement_kpi.py")
    except:
        pass
    status["spark"] = "running" if spark_procs > 0 else "stopped"
    return templates.TemplateResponse(request, "status_fragment.html", {"status": status})

# --- ACTIONS PIPELINE ---

@app.post("/pipeline/start")
def pipeline_start():
    c = docker_client()
    results = []
    for name in ["simulateur", "mqtt-bridge"]:
        try:
            container = c.containers.get(name)
            if container.status != "running":
                container.start()
                results.append(f"{name} demarre")
            else:
                results.append(f"{name} deja en cours")
        except:
            results.append(f"{name} introuvable")

    spark_container = c.containers.get("spark-master")
    spark_container.exec_run("rm -rf /tmp/checkpoint_*")
    spark_container.exec_run(
        cmd='bash -c "PYTHONPATH=/home/spark/.ivy2/packages /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.executorEnv.PYTHONPATH=/home/spark/.ivy2/packages --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.1 /app/traitement_kpi.py"',
        detach=True
    )
    results.append("spark-submit lance")
    return {"status": "ok", "messages": results}

@app.post("/pipeline/stop")
def pipeline_stop():
    c = docker_client()
    results = []
    for name in ["simulateur", "mqtt-bridge"]:
        try:
            container = c.containers.get(name)
            container.stop()
            results.append(f"{name} arrete")
        except:
            results.append(f"{name} introuvable")

    spark_container = c.containers.get("spark-master")
    spark_container.exec_run("pkill -f traitement_kpi.py")
    results.append("spark-submit tue")
    return {"status": "ok", "messages": results}

@app.post("/pipeline/restart")
def pipeline_restart():
    pipeline_stop()
    time.sleep(2)
    pipeline_start()
    return {"status": "ok", "message": "Pipeline redemarree"}

# --- CONFIG MACHINES ---

@app.post("/config/machines/add")
def add_machine(machine_id: str = Form(...), machine_type: str = Form("pipe"),
                target_temp: float = Form(95.0), target_vib: float = Form(1.2)):
    conn = db()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO machine_config (machine_id, machine_type, enabled, target_temp, target_vib) VALUES (%s, %s, true, %s, %s) ON CONFLICT (machine_id) DO UPDATE SET target_temp=%s, target_vib=%s, enabled=true",
        (machine_id, machine_type, target_temp, target_vib, target_temp, target_vib)
    )
    conn.commit()
    cur.close()
    conn.close()
    return {"status": "ok", "machine_id": machine_id}

@app.post("/config/machines/delete/{machine_id}")
def delete_machine(machine_id: str):
    conn = db()
    cur = conn.cursor()
    cur.execute("DELETE FROM machine_config WHERE machine_id=%s", (machine_id,))
    conn.commit()
    cur.close()
    conn.close()
    return {"status": "deleted", "machine_id": machine_id}

@app.post("/config/machines/toggle/{machine_id}")
def toggle_machine(machine_id: str):
    conn = db()
    cur = conn.cursor()
    cur.execute("UPDATE machine_config SET enabled = NOT enabled, updated_at = now() WHERE machine_id=%s RETURNING enabled", (machine_id,))
    row = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()
    if row:
        return {"status": "ok", "machine_id": machine_id, "enabled": row[0]}
    return {"status": "error", "message": "Machine introuvable"}

# --- CONFIG SEUIL ---

@app.post("/config/threshold")
def set_threshold(seuil: float = Form(...)):
    conn = db()
    cur = conn.cursor()
    cur.execute(
        "UPDATE alert_config SET seuil_anomalie=%s, updated_at=now() WHERE machine_id IS NULL AND type_capteur IS NULL",
        (seuil,)
    )
    conn.commit()
    cur.close()
    conn.close()
    return {"status": "ok", "seuil": seuil}
