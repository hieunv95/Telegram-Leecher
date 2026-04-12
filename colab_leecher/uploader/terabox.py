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

TERABOX_CHUNK_SIZE = 4 * 1024 * 1024


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


def _compute_block_list(file_path: str):
    block_list = []
    with open(file_path, "rb") as file_buffer:
        while True:
            chunk = file_buffer.read(TERABOX_CHUNK_SIZE)
            if not chunk:
                break
            block_list.append(hashlib.md5(chunk).hexdigest())
    return block_list


def _response_json(response, error_prefix: str):
    try:
        payload = response.json()
    except Exception as exc:
        raise RuntimeError(f"{error_prefix}: invalid response: {exc}") from exc

    if payload.get("errno") not in [0, None]:
        raise RuntimeError(f"{error_prefix}: {payload.get('errmsg', 'Unknown error')}")

    return payload


def _upload_single_file(
    file_path: str,
    remote_dir: str,
    progress_callback=None,
    file_index: int = 1,
    total_files: int = 1,
):
    if not ospath.exists(file_path) or not ospath.isfile(file_path):
        raise RuntimeError(f"Missing file for Terabox upload: {file_path}")

    file_name = ospath.basename(file_path)
    file_size = ospath.getsize(file_path)
    local_mtime = int(ospath.getmtime(file_path))
    block_list = _compute_block_list(file_path)

    dp_logid = _dp_logid()
    common = _api_common(dp_logid)
    remote_path = _build_remote_path(remote_dir, file_name)

    precreate_url = "https://www.1024terabox.com/api/precreate"
    precreate_data = {
        "path": remote_path,
        "autoinit": 1,
        "target_path": _normalize_remote_dir(remote_dir),
        "block_list": json.dumps(block_list),
        "size": file_size,
        "local_mtime": local_mtime,
        **common,
    }

    with requests.Session() as session:
        precreate_res = session.post(
            precreate_url,
            headers=_request_headers(content_type=True),
            data=urlencode(precreate_data),
            timeout=180,
        )
        precreate_json = _response_json(precreate_res, "Terabox precreate failed")

        uploadid = precreate_json.get("uploadid")
        if not uploadid:
            raise RuntimeError("Terabox precreate failed: missing uploadid")

        upload_url = "https://c-all.terabox.com/rest/2.0/pcs/superfile2"
        upload_headers = _request_headers(content_type=False)
        if "Content-Type" in upload_headers:
            upload_headers.pop("Content-Type", None)

        with open(file_path, "rb") as upload_buffer:
            partseq = 0
            while True:
                chunk = upload_buffer.read(TERABOX_CHUNK_SIZE)
                if not chunk:
                    break

                upload_params = {
                    "method": "upload",
                    "app_id": Paths.TERABOX_APP_ID,
                    "channel": "dubox",
                    "clienttype": "0",
                    "web": "1",
                    "path": remote_path,
                    "uploadid": uploadid,
                    "uploadsign": "0",
                    "partseq": partseq,
                }
                if Paths.TERABOX_BDSTOKEN:
                    upload_params["bdstoken"] = Paths.TERABOX_BDSTOKEN

                upload_res = session.post(
                    upload_url,
                    headers=upload_headers,
                    params=upload_params,
                    files={"file": ("blob", chunk)},
                    timeout=3600,
                )
                _response_json(upload_res, f"Terabox chunk {partseq} upload failed")
                logging.info(
                    "Terabox chunk uploaded: %s part %s of %s",
                    file_name,
                    partseq + 1,
                    len(block_list),
                )
                if progress_callback:
                    try:
                        progress_callback(
                            {
                                "file_name": file_name,
                                "file_index": file_index,
                                "total_files": total_files,
                                "partseq": partseq + 1,
                                "total_parts": len(block_list),
                                "remote_path": remote_path,
                                "size": file_size,
                            }
                        )
                    except Exception as callback_error:
                        logging.info("Terabox progress callback failed: %s", callback_error)
                partseq += 1

        create_url = "https://www.1024terabox.com/api/create"
        create_data = {
            "path": remote_path,
            "size": file_size,
            "uploadid": uploadid,
            "target_path": _normalize_remote_dir(remote_dir),
            "block_list": json.dumps(block_list),
            "local_mtime": local_mtime,
            "isdir": "0",
            "rtype": "1",
            **common,
        }

        create_res = session.post(
            create_url,
            headers=_request_headers(content_type=True),
            data=urlencode(create_data),
            timeout=180,
        )
        create_json = _response_json(create_res, "Terabox create failed")

    return {
        "file_name": file_name,
        "remote_path": create_json.get("path", remote_path),
        "size": file_size,
    }


async def upload_to_terabox(local_path: str, remote_dir: str = "", progress_callback=None):
    is_ok, reason = validate_terabox_credentials()
    if not is_ok:
        raise RuntimeError(reason)

    target_remote_dir = _normalize_remote_dir(remote_dir or Paths.TERABOX_FOLDER)
    upload_results = []

    if ospath.isfile(local_path):
        uploaded = await to_thread(
            _upload_single_file,
            local_path,
            target_remote_dir,
            progress_callback,
            1,
            1,
        )
        upload_results.append(uploaded)
    elif ospath.isdir(local_path):
        file_entries = []
        for root, _, files in os.walk(local_path):
            relative = ospath.relpath(root, local_path)
            if relative == ".":
                file_remote_dir = target_remote_dir
            else:
                relative = relative.replace(os.sep, "/")
                file_remote_dir = _normalize_remote_dir(f"{target_remote_dir}/{relative}")

            for file_name in sorted(files):
                file_path = ospath.join(root, file_name)
                file_entries.append((file_path, file_remote_dir))

        total_files = len(file_entries)
        for file_index, (file_path, file_remote_dir) in enumerate(file_entries, start=1):
            uploaded = await to_thread(
                _upload_single_file,
                file_path,
                file_remote_dir,
                progress_callback,
                file_index,
                total_files,
            )
            upload_results.append(uploaded)
    else:
        raise RuntimeError(f"Upload source not found: {local_path}")

    total_size = sum(item.get("size", 0) for item in upload_results)
    logging.info(
        "Terabox upload complete: %s files, %s bytes at %s",
        len(upload_results),
        total_size,
        time(),
    )

    return upload_results
