from fastapi import FastAPI
from fastapi.testclient import TestClient
from pysparkassist.api.routes import router


def test_health_endpoint():
    app = FastAPI()
    app.include_router(router, prefix="/api")
    client = TestClient(app)
    resp = client.get("/api/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"
