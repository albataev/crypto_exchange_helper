from aiohttp import web
from modules_robot.web_interface.web_routes import setup_routes


app = web.Application()
# app.router.add_get('/', handle)
# app.router.add_get('/{name}', handle)
setup_routes(app)
web.run_app(app, host='127.0.0.1', port=8081)

