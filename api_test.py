import requests
 
response = requests.get(
    "https://proxy.webshare.io/api/v2/proxy/list/?mode=direct&page=1&page_size=25",
    headers={"Authorization": "Token wemj46xw6m0q876m6i4x65j434bsh735dbef70hc"}
)

print(response.json())