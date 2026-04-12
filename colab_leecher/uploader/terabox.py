import os
import json
import hashlib
import logging
import requests
from time import time
from random import choices
from string import ascii_uppercase, digits
from asyncio import to_thread
from os import path as ospath
from urllib.parse import urlencode
from colab_leecher.utility.variables import Paths


def _normalize_remote_dir(remote_dir: str):
    if not remote_dir:
        return "/"
    if not remote_dir.startswith("/"):
        remote_dir = f"/{remote_dir}"
    return remote_dir.rstrip("/") or "/"


def _build_remote_path(remote_dir: str, file_name: str):
    remote_dir = _normalize_remote_dir(remote_dir)
    if remote_dir == "/":
        return f"/{file_name}"
    return f"{remote_dir}/{file_name}"


def _cookie_header():
    return f"lang=en; ndus={Paths.TERABOX_NDUS};"


def _dp_logid():
    if Paths.TERABOX_DP_LOGID:
        return Paths.TERABOX_DP_LOGID
    return "".join(choices(ascii_uppercase + digits, k=20))


def validate_terabox_credentials():
    if not Paths.TERABOX_NDUS:
        return False, "TERABOX_NDUS is not configured"
    if not Paths.TERABOX_JS_TOKEN:
        return False, "TERABOX_JS_TOKEN is not configured"
    return True, "ok"


def _request_headers(content_type: bool = False):
    headers = {
        "Cookie": _cookie_header(),
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/plain, */*",
        "X-Requested-With": "XMLHttpRequest",
    }
    if content_type:
        headers["Content-Type"] = "application/x-www-form-urlencoded"
    return headers


def _api_common(dp_logid: str):
    common = {
        "app_id": Paths.TERABOX_APP_ID,
        "web": "1",
        "channel": "dubox",
        "clienttype": "0",
        "jsToken": Paths.TERABOX_JS_TOKEN,
        "dp-logid": dp_logid,
    }
    if Paths.TERABOX_BDSTOKEN:
        common["bdstoken"] = Paths.TERABOX_BDSTOKEN
    return common


def _upload_single_file(file_path: str, remote_dir: str):
    if not ospath.exists(file_path) or not ospath.isfile(file_path):
        raise RuntimeError(f"Missing file for Terabox upload: {file_path}")

    file_name = ospath.basename(file_path)
    file_size = ospath.getsize(file_path)
    local_mtime = int(ospath.getmtime(file_path))
    with open(file_path, "rb") as file_buffer:
        file_md5 = hashlib.md5(file_buffer.read()).hexdigest()

    dp_logid = _dp_logid()
    common = _api_common(dp_logid)
    remote_path = _build_remote_path(remote_dir, file_name)

    precreate_url = "https://www.1024terabox.com/api/precreate"
    precreate_data = {
        "path": remote_path,
        "autoinit": 1,
        "target_path": _normalize_remote_dir(remote_dir),
        "block_list": json.dumps([file_md5]),
        "size": file_size,
        "local_mtime": local_mtime,
        **common,
    }

    precreate_res = requests.post(
        precreate_url,
        headers=_request_headers(content_type=True),
        data=urlencode(precreate_data),
        timeout=180,
    )
    precreate_res.raise_for_status()
    precreate_json = precreate_res.json()
    if precreate_json.get("errno") != 0:
        raise RuntimeError(
            f"Terabox precreate failed: {precreate_json.get('errmsg', 'Unknown error')}"
        )

    uploadid = precreate_json.get("uploadid")
    if not uploadid:
        raise RuntimeError("Terabox precreate failed: missing uploadid")

    upload_url = "https://c-all.terabox.com/rest/2.0/pcs/superfile2"
    upload_params = {
        "method": "upload",
        "app_id": Paths.TERABOX_APP_ID,
        "channel": "dubox",
        "clienttype": "0",
        "web": "1",
        "path": remote_path,
        "uploadid": uploadid,
        "uploadsign": "0",
        "partseq": 0,
    }
    if Paths.TERABOX_BDSTOKEN:
        upload_params["bdstoken"] = Paths.TERABOX_BDSTOKEN

    with open(file_path, "rb") as upload_buffer:
        upload_res = requests.post(
            upload_url,
            headers=_request_headers(content_type=False),
            params=upload_params,
            files={"file": ("blob", upload_buffer)},
            timeout=3600,
        )

    upload_res.raise_for_status()
    upload_json = upload_res.json()
    if upload_json.get("errno") not in [0, None]:
        raise RuntimeError(
            f"Terabox upload failed: {upload_json.get('errmsg', 'Unknown error')}"
        )

    create_url = "https://www.1024terabox.com/api/create"
    create_data = {
        "path": remote_path,
        "size": file_size,
        "uploadid": uploadid,
        "target_path": _normalize_remote_dir(remote_dir),
        "block_list": json.dumps([file_md5]),
        "local_mtime": local_mtime,
        "isdir": "0",
        "rtype": "1",
        **common,
    }

    create_res = requests.post(
        create_url,
        headers=_request_headers(content_type=True),
        data=urlencode(create_data),
        timeout=180,
    )
    create_res.raise_for_status()
    create_json = create_res.json()
    if create_json.get("errno") != 0:
        raise RuntimeError(
            f"Terabox create failed: {create_json.get('errmsg', 'Unknown error')}"
        )

    return {
        "file_name": file_name,
        "remote_path": create_json.get("path", remote_path),
        "size": file_size,
    }


async def upload_to_terabox(local_path: str, remote_dir: str = ""):
    is_ok, reason = validate_terabox_credentials()
    if not is_ok:
        raise RuntimeError(reason)

    target_remote_dir = _normalize_remote_dir(remote_dir or Paths.TERABOX_FOLDER)
    upload_results = []

    if ospath.isfile(local_path):
        uploaded = await to_thread(_upload_single_file, local_path, target_remote_dir)
        upload_results.append(uploaded)
    elif ospath.isdir(local_path):
        for root, _, files in os.walk(local_path):
            relative = ospath.relpath(root, local_path)
            if relative == ".":
                file_remote_dir = target_remote_dir
            else:
                relative = relative.replace(os.sep, "/")
                file_remote_dir = _normalize_remote_dir(f"{target_remote_dir}/{relative}")

            for file_name in sorted(files):
                file_path = ospath.join(root, file_name)
                uploaded = await to_thread(_upload_single_file, file_path, file_remote_dir)
                upload_results.append(uploaded)
    else:
        raise RuntimeError(f"Upload source not found: {local_path}")

    total_size = sum(item.get("size", 0) for item in upload_results)
    logging.info(
        "Terabox upload complete: %s files, %s bytes in %ss",
        len(upload_results),
        total_size,
        int(time()),
    )

    return upload_results
