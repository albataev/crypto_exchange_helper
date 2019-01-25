from web_views import choose_buy, make_buy, show_monitor, sell_all, sell_one, auth, pokupay, log, choose_log

user_id = 1
def setup_routes(app):
    app.router.add_get('/', auth)
    app.router.add_get('/choose_buy', choose_buy)
    app.router.add_get('/make_buy', make_buy)
    app.router.add_get('/sell_all', sell_all)
    app.router.add_get('/show_monitor', show_monitor)
    app.router.add_get('/sell_one', sell_one)
    app.router.add_get('/pokupay', pokupay)
    app.router.add_get('/log', log)
    app.router.add_get('/choose_log', choose_log)