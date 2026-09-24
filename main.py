"""本地启动入口：uvicorn main:app --reload。"""

from gateway.app import create_app

app = create_app()
