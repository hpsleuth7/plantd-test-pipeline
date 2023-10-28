import requests

# send a post request
url = "http://localhost:3000/upload"
r = requests.post(url, files={"file": open("dummy.zip", "rb")})