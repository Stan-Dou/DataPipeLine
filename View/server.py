import json
import sys
from http import HTTPStatus
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
from urllib.parse import parse_qs, urlparse


ROOT_DIR = Path(__file__).resolve().parent.parent
VIEW_DIR = Path(__file__).resolve().parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from DAO.options_dao import OptionsDAO


class ViewController:
    def __init__(self, db_path: Path):
        self.dao = OptionsDAO(str(db_path))
        self.meta_cache = []
        self.sync()

    def sync(self):
        self.meta_cache = self.dao.get_company_metadata()

    def get_meta(self):
        return {"companies": self.meta_cache}

    def get_stock_series(self, symbol: str):
        identity = self.dao.get_stock_identity(symbol)
        if identity is None:
            return None
        return {
            "company": identity,
            "points": self.dao.get_closing_price_series(symbol),
        }

    def get_contract_series(self, symbol: str, expiry: str, strike: str, option_type: str):
        identity = self.dao.get_stock_identity(symbol)
        if identity is None:
            return None

        try:
            strike_value = float(strike)
        except ValueError:
            return None

        payload = self.dao.get_contract_series(identity["stock_code"], expiry, strike_value, option_type)
        if not payload:
            return None
        return payload

    def close(self):
        self.dao.close()


controller = ViewController(ROOT_DIR / "data" / "options.db")


class RequestHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(VIEW_DIR), **kwargs)

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path.startswith("/api/"):
            self._handle_api_get(parsed)
            return
        if parsed.path == "/":
            self.path = "/index.html"
        super().do_GET()

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/sync":
            controller.sync()
            self._send_json(HTTPStatus.OK, {"status": "ok", "meta": controller.get_meta()})
            return
        self._send_json(HTTPStatus.NOT_FOUND, {"error": "Unknown API endpoint"})

    def _handle_api_get(self, parsed):
        params = parse_qs(parsed.query)

        if parsed.path == "/api/meta":
            self._send_json(HTTPStatus.OK, controller.get_meta())
            return

        if parsed.path == "/api/stock-series":
            symbol = params.get("symbol", [""])[0]
            payload = controller.get_stock_series(symbol)
            if payload is None:
                self._send_json(HTTPStatus.NOT_FOUND, {"error": "Unknown company"})
                return
            self._send_json(HTTPStatus.OK, payload)
            return

        if parsed.path == "/api/contract-series":
            symbol = params.get("symbol", [""])[0]
            expiry = params.get("expiry", [""])[0]
            strike = params.get("strike", [""])[0]
            option_type = params.get("type", [""])[0].upper()
            payload = controller.get_contract_series(symbol, expiry, strike, option_type)
            if payload is None:
                self._send_json(HTTPStatus.BAD_REQUEST, {"error": "Invalid contract parameters"})
                return
            self._send_json(HTTPStatus.OK, payload)
            return

        self._send_json(HTTPStatus.NOT_FOUND, {"error": "Unknown API endpoint"})

    def _send_json(self, status: HTTPStatus, payload):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)


def run(port: int = 8000):
    server = HTTPServer(("127.0.0.1", port), RequestHandler)
    try:
        print(f"View server running at http://127.0.0.1:{port}")
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        controller.close()
        server.server_close()


if __name__ == "__main__":
    port = 8000
    if len(sys.argv) > 1:
        port = int(sys.argv[1])
    run(port)