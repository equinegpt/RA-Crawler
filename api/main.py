# api/main.py
from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi import Response

# Import the router your /races endpoints live on.
# Most projects name it `router`. If your file exports `races_router`
# instead, the fallback import will pick it up.
try:
    from .races import router as races_router
except ImportError:
    from .races import races_router  # fallback if exported under this name

def auth():
    # No-op dependency to mirror your earlier setup.
    return True

app = FastAPI(title="Racing Australia API")

# (Optional) CORS for your app/ngrok/mobile tests
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/", include_in_schema=False)
def root():
    return {
        "name": "RA Crawler API",
        "try": ["/races", "/docs", "/redoc", "/healthz"]
    }

@app.get("/healthz", include_in_schema=False)
def healthz():
    return {"ok": True}

# Mount the routes
app.include_router(races_router, dependencies=[Depends(auth)])
