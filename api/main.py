# api/main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .races import races_router
from .db import ensure_schema

ensure_schema()
app = FastAPI()
app.include_router(races_router)

app = FastAPI(title="RA Program API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def health():
    return {"status": "ok"}

app.include_router(races_router)  # exposes /races

# Optional local run:
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api.main:app", host="0.0.0.0", port=8001, reload=True)
