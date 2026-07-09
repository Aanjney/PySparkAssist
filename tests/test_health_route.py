from fastapi import FastAPI
from fastapi.testclient import TestClient

from pysparkassist.api.routes import router


def test_health_route() -> None:
    app = FastAPI()
    app.include_router(router, prefix="/api")
    client = TestClient(app)
    response = client.get("/api/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
