import requests
import time
# send a post request
url = "http://localhost:3000/upload"
r = requests.post(url, files={"file": open("dummy.zip", "rb")})

# get current time in seconds
skip_time = 2 # number of seconds in skip interval
if (int(time.time()) // skip_time) % 2 == 0:
    print("Skipping time")
    exit() 
print("Not skipping time") 
# do dummy work
for i in range(10000):
    for j in range(2, i):
        if i % j == 0:
            break