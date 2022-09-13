import base64

name = "林蛋大"

b64_name = base64.b64encode(name.encode())
print(b64_name)