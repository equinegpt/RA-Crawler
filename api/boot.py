# api/boot.py
import os, shutil, sqlite3, hashlib, time

SEED = "/opt/render/project/src/data/racing.db"
RUNTIME = "/data/racing.db"
META = "/data/.seed_meta.txt"

def sha256(path):
    try:
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                h.update(chunk)
        return h.hexdigest()
    except FileNotFoundError:
        return "(missing)"

def normalize_dates(db_path):
    try:
        con = sqlite3.connect(db_path)
        cur = con.cursor()
        cur.execute("""
            UPDATE race_program
            SET date = date(date)
            WHERE date IS NOT NULL AND trim(date) <> ''
        """)
        con.commit()
        cur.execute("SELECT COUNT(*) FROM race_program")
        rows = cur.fetchone()[0]
        cur.execute("SELECT MIN(date), MAX(date) FROM race_program")
        span = cur.fetchone()
        print(f"[boot] DB rows={rows} span={span}")
    except Exception as e:
        print("[boot] normalize_dates error:", e)
    finally:
        try:
            con.close()
        except Exception:
            pass

def main():
    os.makedirs("/data", exist_ok=True)

    seed_hash = sha256(SEED)
    runtime_hash_before = sha256(RUNTIME)
    print(f"[boot] SEED:    {SEED}")
    print(f"[boot] RUNTIME: {RUNTIME}")
    print(f"[boot] seed sha256:   {seed_hash}")
    print(f"[boot] runtime sha256 (before): {runtime_hash_before}")

    # Always overwrite runtime with seed
    shutil.copyfile(SEED, RUNTIME)
    runtime_hash_after = sha256(RUNTIME)
    print(f"[boot] Copied seed -> runtime")
    print(f"[boot] runtime sha256 (after):  {runtime_hash_after}")

    # Write marker so we can prove which seed got copied
    with open(META, "w") as f:
        f.write(f"time={time.strftime('%Y-%m-%d %H:%M:%S %Z')}\n")
        f.write(f"seed_sha256={seed_hash}\n")
        f.write(f"runtime_sha256={runtime_hash_after}\n")

    # Normalize dates
    normalize_dates(RUNTIME)

    # Point app to runtime DB
    os.environ["DATABASE_URL"] = "sqlite:////data/racing.db"
    port = os.getenv("PORT", "10000")
    print(f"[boot] Starting uvicorn on {port} DATABASE_URL={os.environ['DATABASE_URL']}")
    os.execvp("uvicorn", ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", port])

if __name__ == "__main__":
    main()
