from bottle import Bottle, get, response
import json

route_prefix = 'test_service_5'

service = Bottle()

@service.route('/' + route_prefix)
def test_service_5():
    response.content_type = 'application/json'
    return json.dumps({'hello' : route_prefix})
