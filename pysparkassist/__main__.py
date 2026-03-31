import uvicorn

from pysparkassist.api.app import create_app

uvicorn.run(create_app(), host="0.0.0.0", port=8000)
