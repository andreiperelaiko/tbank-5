"""HTTP server exposing /health and /metrics endpoints.

Run in a background thread so it does not interfere with the Kafka poll loop.
"""
from __future__ import annotations

import logging
import threading
from typing import Callable

from flask import Flask, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

log = logging.getLogger(__name__)


def build_app(is_healthy: Callable[[], bool]) -> Flask:
    app = Flask(__name__)

    @app.route("/health")
    def health() -> Response:  # noqa: WPS430
        if is_healthy():
            return Response("OK", status=200, mimetype="text/plain")
        return Response("DOWN", status=503, mimetype="text/plain")

    @app.route("/metrics")
    def metrics() -> Response:  # noqa: WPS430
        return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)

    @app.route("/")
    def index() -> Response:  # noqa: WPS430
        return Response(
            "Smart Warehouse consumer\n"
            "GET /health  — liveness/readiness probe\n"
            "GET /metrics — Prometheus metrics\n",
            mimetype="text/plain",
        )

    return app


def start_http_server(host: str, port: int, is_healthy: Callable[[], bool]) -> None:
    app = build_app(is_healthy)

    def _run() -> None:
        from werkzeug.serving import make_server

        server = make_server(host, port, app, threaded=True)
        log.info("HTTP server listening", extra={"host": host, "port": port})
        server.serve_forever()

    thread = threading.Thread(target=_run, name="http-server", daemon=True)
    thread.start()
