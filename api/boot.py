# api/boot.py
import os
import uvicorn


def main() -> None:
    # Just log what we're using; do NOT override it.
    db_url = os.getenv("DATABASE_URL", "<unset>")
    print(f"[boot] Starting RA-Crawler with DATABASE_URL={db_url!r}")

    port = int(os.getenv("PORT", "10000"))
    # api.main imports db.get_engine() which reads DATABASE_URL
    # and calls ensure_schema(), so we don't have to do anything here.
    uvicorn.run("api.main:app", host="0.0.0.0", port=port)


if __name__ == "__main__":
    main()
