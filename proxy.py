import requests

username = "u07482d15574405cb-zone-custom-region-eu"
password = "u07482d15574405cb"
PROXY_DNS = "118.193.58.115:2334"
urlToGet = "http://ip-api.com/json"
proxy = {"http":"http://{}:{}@{}".format(username, password, PROXY_DNS)}
r = requests.get(urlToGet , proxies=proxy)

print("Response:{}".format(r.text))