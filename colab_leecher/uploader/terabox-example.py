import requests
import hashlib
import os
import json
from urllib.parse import urlencode

# Step 1: Configuration
cookie = "lang=en; ndus=your_value_from_inspect-Application-cookie;"  # Your TeraBox cookie
js_token = "DA4E4759E025EC13B511FC097F814B1B7E46D77936149451D95E0F6DB0439816C3FA2BCB8A0148B5B12AF60E4C8D41D571E14F5BD0C830F85910367B4A58E3E9"  # Replace with your jsToken
dp_logid = "51937100964488500038"  # Replace with your dp-logid
bdstoken = "0ae23806418345e799b2608574e69b57"  # Replace with your bdstoken
file_path = "/kaggle/working/yourfile.zip"  # Replace with your file's path
destination_path = "/ShareDataset/yourfile.zip"  # Destination in TeraBox
chunk_size = 4 * 1024 * 1024  # 4MB chunks, adjust as needed

# Verify file exists
if not os.path.exists(file_path):
    print(f"File {file_path} does not exist. Please check the path.")
    exit()

file_name = os.path.basename(file_path)
file_size = os.path.getsize(file_path)
target_path = "/ShareDataset/"  # Parent directory

# Step 2: Compute MD5 hashes for file chunks
def compute_block_list(file_path, chunk_size):
    block_list = []
    with open(file_path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            md5 = hashlib.md5(chunk).hexdigest()
            block_list.append(md5)
    return block_list

block_list = compute_block_list(file_path, chunk_size)

# Step 3: Precreate upload
precreate_url = "https://www.1024terabox.com/api/precreate"
precreate_headers = {
    "Cookie": cookie,
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/x-www-form-urlencoded",
    "X-Requested-With": "XMLHttpRequest",
}
precreate_data = {
    "path": destination_path,
    "autoinit": 1,
    "target_path": target_path,
    "block_list": json.dumps(block_list),
    "size": file_size,
    "local_mtime": int(os.path.getmtime(file_path)),
    "app_id": "250528",
    "web": "1",
    "channel": "dubox",
    "clienttype": "0",
    "jsToken": js_token,
    "dp-logid": dp_logid,
    "bdstoken": bdstoken,
}

try:
    response = requests.post(precreate_url, headers=precreate_headers, data=urlencode(precreate_data))
    response.raise_for_status()
    result = response.json()
    if result.get("errno") != 0:
        print("Precreate Error:", result.get("errmsg", "Unknown error"))
        exit()
    uploadid = result["uploadid"]
except Exception as e:
    print("Precreate Failed:", str(e))
    print("Check jsToken, dp-logid, bdstoken, and cookie validity.")
    exit()

# Step 4: Upload file chunks
upload_url = "https://c-all.terabox.com/rest/2.0/pcs/superfile2"
upload_headers = {
    "Cookie": cookie,
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
}
partseq = 0

with open(file_path, "rb") as f:
    while True:
        chunk = f.read(chunk_size)
        if not chunk:
            break
        upload_params = {
            "method": "upload",
            "app_id": "250528",
            "channel": "dubox",
            "clienttype": "0",
            "web": "1",
            "path": destination_path,
            "uploadid": uploadid,
            "uploadsign": "0",
            "partseq": partseq,
            "bdstoken": bdstoken,
        }
        files = {"file": ("blob", chunk)}
        try:
            response = requests.post(upload_url, headers=upload_headers, params=upload_params, files=files)
            response.raise_for_status()
            result = response.json()
            if result.get("errno") != 0:
                print(f"Chunk {partseq} Upload Error:", result.get("errmsg", "Unknown error"))
                exit()
            partseq += 1
        except Exception as e:
            print(f"Chunk {partseq} Upload Failed:", str(e))
            exit()

# Step 5: Finalize upload
create_url = "https://www.1024terabox.com/api/create"
create_headers = precreate_headers
create_data = {
    "path": destination_path,
    "size": file_size,
    "uploadid": uploadid,
    "target_path": target_path,
    "block_list": json.dumps(block_list),
    "local_mtime": int(os.path.getmtime(file_path)),
    "isdir": "0",
    "rtype": "1",
    "bdstoken": bdstoken,
    "app_id": "250528",
    "web": "1",
    "channel": "dubox",
    "clienttype": "0",
    "jsToken": js_token,
    "dp-logid": dp_logid,
}

try:
    response = requests.post(create_url, headers=create_headers, data=urlencode(create_data))
    response.raise_for_status()
    result = response.json()
    if result.get("errno") != 0:
        print("Create Error:", result.get("errmsg", "Unknown error"))
        exit()
    print("✅ File uploaded successfully!")
    print("File Path in TeraBox:", result.get("path", destination_path))
except Exception as e:
    print("Create Failed:", str(e))
    print("Check jsToken, dp-logid, bdstoken, and cookie validity.")
    exit()

# Step 6: Verify upload
list_url = "https://www.1024terabox.com/api/list"
list_params = {
    "app_id": "250528",
    "web": "1",
    "channel": "dubox",
    "clienttype": "0",
    "jsToken": js_token,
    "dp-logid": dp_logid,
    "bdstoken": bdstoken,
    "order": "time",
    "desc": "1",
    "dir": target_path,
    "num": "100",
    "page": "1",
    "showempty": "0",
}

try:
    response = requests.get(list_url, headers=precreate_headers, params=list_params)
    response.raise_for_status()
    result = response.json()
    if result.get("errno") != 0:
        print("List Error:", result.get("errmsg", "Unknown error"))
    else:
        files = result.get("list", [])
        for file in files:
            if file["path"] == destination_path:
                print("Verification: File found in TeraBox!")
                print("Name:", file["server_filename"])
                print("Size:", file["size"])
                break
        else:
            print("Verification: File not found in TeraBox directory.")
except Exception as e:
    print("List Failed:", str(e))