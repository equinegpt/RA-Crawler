# api/app.py
from typing import Optional

app = None  # FastAPI instance will end up here

# Try to import a module-level app from api.main
try:
    from .main import app as _app  # type: ignore[attr-defined]
    app = _app  # if this works, you're done
except Exception:
    # Fall back to a factory in api.main (create_app)
    try:
        from .main import create_app  # type: ignore[attr-defined]
    except Exception as e:
        raise RuntimeError(
            "Could not find `app` or `create_app()` in api.main"
        ) from e
    app = create_app()  # build the FastAPI app
