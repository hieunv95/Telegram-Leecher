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
from urllib.parse import urlencode, quote
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


def build_precreate_url(app_id: str, js_token: str, dp_logid: str):
    return (
        "https://dm.terabox.com/api/precreate"
        f"?app_id={app_id}&web=1&channel=dubox&clienttype=0"
        f"&jsToken={js_token}&dp-logid={dp_logid}"
    )


def build_upload_url(path_value: str, upload_id: str, app_id: str, partseq: int):
    encoded_path = quote(path_value, safe="")
    return (
        "https://kul-cdata.terabox.com/rest/2.0/pcs/superfile2"
        f"?method=upload&app_id={app_id}&channel=dubox&clienttype=0&web=1"
        f"&path={encoded_path}&uploadid={upload_id}&uploadsign=0&partseq={partseq}"
    )


def build_create_url(app_id: str, js_token: str, dp_logid: str):
    return (
        "https://dm.terabox.com/api/create"
        f"?app_id={app_id}&web=1&channel=dubox&clienttype=0"
        f"&jsToken={js_token}&dp-logid={dp_logid}"
    )


def build_list_url(app_id: str, directory: str, js_token: str, dp_logid: str):
    encoded_directory = quote(directory, safe="")
    return (
        "https://dm.terabox.com/api/list"
        f"?app_id={app_id}&web=1&channel=dubox&clienttype=0"
        f"&jsToken={js_token}&dp-logid={dp_logid}&order=time&desc=1"
        f"&dir={encoded_directory}&num=100&page=1&showempty=0"
    )


def build_video_download_url(app_id: str, video_path: str):
    encoded_path = quote(video_path, safe="")
    return (
        "https://dm.terabox.com/api/streaming"
        f"?path={encoded_path}&app_id={app_id}&clienttype=0"
        "&type=M3U8_FLV_264_480&vip=1"
    )


def _compute_block_list(file_path: str):
    file_size = ospath.getsize(file_path)
    if file_size <= TERABOX_CHUNK_SIZE:
        with open(file_path, "rb") as file_buffer:
            return [hashlib.md5(file_buffer.read()).hexdigest()]

    block_list = []
    with open(file_path, "rb") as file_buffer:
        while True:
            chunk = file_buffer.read(TERABOX_CHUNK_SIZE)
            if not chunk:
                break
            block_list.append(hashlib.md5(chunk).hexdigest())
    return block_list


def _iter_file_chunks(file_path: str):
    with open(file_path, "rb") as file_buffer:
        partseq = 0
        while True:
            chunk = file_buffer.read(TERABOX_CHUNK_SIZE)
            if not chunk:
                break
            yield partseq, chunk, hashlib.md5(chunk).hexdigest()
            partseq += 1


def _response_json(response, error_prefix: str, raise_on_error: bool = True):
    try:
        payload = response.json()
    except Exception as exc:
        logging.error("%s: invalid response body: %s", error_prefix, exc)
        raise RuntimeError(f"{error_prefix}: invalid response: {exc}") from exc

    errno = payload.get("errno")
    errmsg = payload.get("errmsg")

    if errno not in [0, None]:
        logging.error(
            "%s: errno=%s errmsg=%s",
            error_prefix,
            errno,
            errmsg or "Unknown error",
        )
        if raise_on_error:
            raise RuntimeError(
                f"{error_prefix}: errno={errno} errmsg={errmsg or 'Unknown error'}"
            )
    else:
        logging.debug("%s: errno=%s", error_prefix, errno)

    return payload


def _is_need_verify_error(errno, errmsg):
    try:
        if int(errno) == 4000023:
            return True
    except Exception:
        pass
    return "need verify" in str(errmsg or "").lower()


def precheck_terabox_upload_session(remote_dir: str = ""):
    """Validate Terabox session before download/upload by probing precreate API."""
    is_ok, reason = validate_terabox_credentials()
    if not is_ok:
        return {
            "ok": False,
            "expired": False,
            "errno": None,
            "errmsg": reason,
            "reason": reason,
        }

    dp_logid = _dp_logid()
    target_remote_dir = _normalize_remote_dir(remote_dir or Paths.TERABOX_FOLDER)
    dummy_name = f".precheck_{''.join(choices(ascii_uppercase + digits, k=10))}.tmp"
    dummy_path = _build_remote_path(target_remote_dir, dummy_name)

    precreate_url = build_precreate_url(
        Paths.TERABOX_APP_ID,
        Paths.TERABOX_JS_TOKEN,
        dp_logid,
    )
    precreate_data = {
        "path": dummy_path,
        "autoinit": 1,
        "target_path": target_remote_dir,
        "block_list": json.dumps([hashlib.md5(b"0").hexdigest()]),
        "size": 1,
        "local_mtime": int(time()),
    }
    if Paths.TERABOX_BDSTOKEN:
        precreate_data["bdstoken"] = Paths.TERABOX_BDSTOKEN

    try:
        with requests.Session() as session:
            precreate_res = session.post(
                precreate_url,
                headers=_request_headers(content_type=True),
                data=urlencode(precreate_data),
                timeout=120,
            )
            payload = _response_json(
                precreate_res,
                "Terabox precheck precreate",
                raise_on_error=False,
            )
    except Exception as exc:
        logging.error("Terabox precheck request failed: %s", exc)
        return {
            "ok": False,
            "expired": False,
            "errno": None,
            "errmsg": str(exc),
            "reason": f"precheck request failed: {exc}",
        }

    errno = payload.get("errno")
    errmsg = payload.get("errmsg")

    if _is_need_verify_error(errno, errmsg):
        logging.error(
            "Terabox jsToken verification required: errno=%s errmsg=%s",
            errno,
            errmsg,
        )
        return {
            "ok": False,
            "expired": True,
            "errno": errno,
            "errmsg": errmsg,
            "reason": f"Terabox jsToken needs verification (errno={errno}, errmsg={errmsg})",
        }

    if errno not in [0, None]:
        return {
            "ok": False,
            "expired": False,
            "errno": errno,
            "errmsg": errmsg,
            "reason": f"Terabox precheck failed (errno={errno}, errmsg={errmsg or 'Unknown error'})",
        }

    logging.info("Terabox precheck passed: errno=%s", errno)
    return {
        "ok": True,
        "expired": False,
        "errno": errno,
        "errmsg": errmsg,
        "reason": "ok",
    }


def _upload_chunk(session, upload_url: str, chunk: bytes, expected_md: str, partseq: int, upload_headers: dict, file_name: str):
    upload_res = session.post(
        upload_url,
        headers=upload_headers,
        files={"file": ("blob", chunk)},
        timeout=3600,
    )
    upload_json = _response_json(upload_res, f"Terabox chunk {partseq} upload failed")
    uploaded_md = upload_json.get("md5")
    if not uploaded_md:
        raise RuntimeError(f"Terabox chunk {partseq} upload failed: missing md5")
    if uploaded_md.lower() != expected_md.lower():
        raise RuntimeError(
            f"Terabox chunk {partseq} upload failed: md5 mismatch for {file_name}"
        )
    return uploaded_md


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
    precreate_block_list = _compute_block_list(file_path)
    use_chunking = file_size > TERABOX_CHUNK_SIZE

    dp_logid = _dp_logid()
    remote_path = _build_remote_path(remote_dir, file_name)

    precreate_url = build_precreate_url(
        Paths.TERABOX_APP_ID,
        Paths.TERABOX_JS_TOKEN,
        dp_logid,
    )
    precreate_data = {
        "path": remote_path,
        "autoinit": 1,
        "target_path": _normalize_remote_dir(remote_dir),
        "block_list": json.dumps(precreate_block_list),
        "size": file_size,
        "local_mtime": local_mtime,
    }
    if Paths.TERABOX_BDSTOKEN:
        precreate_data["bdstoken"] = Paths.TERABOX_BDSTOKEN

    logging.debug(
        "Terabox precreate metadata: file=%s size=%s blocks=%s first_md5=%s",
        file_name,
        file_size,
        len(precreate_block_list),
        precreate_block_list[0][:12] if precreate_block_list else "none",
    )

    def _emit_progress(uploaded_bytes: int, partseq: int, total_parts: int):
        if not progress_callback:
            return
        try:
            progress_callback(
                {
                    "file_name": file_name,
                    "file_index": file_index,
                    "total_files": total_files,
                    "partseq": partseq,
                    "total_parts": total_parts,
                    "remote_path": remote_path,
                    "size": file_size,
                    "uploaded_bytes": uploaded_bytes,
                    "total_bytes": file_size,
                }
            )
        except Exception as callback_error:
            logging.info("Terabox progress callback failed: %s", callback_error)

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

        upload_headers = _request_headers(content_type=False)
        if "Content-Type" in upload_headers:
            upload_headers.pop("Content-Type", None)

        uploaded_block_list = []
        uploaded_bytes = 0
        if use_chunking:
            total_parts = len(precreate_block_list)
            for partseq, chunk, expected_md in _iter_file_chunks(file_path):
                upload_url = build_upload_url(
                    remote_path,
                    uploadid,
                    Paths.TERABOX_APP_ID,
                    partseq,
                )
                if Paths.TERABOX_BDSTOKEN:
                    upload_url += f"&bdstoken={Paths.TERABOX_BDSTOKEN}"
                uploaded_md = _upload_chunk(
                    session,
                    upload_url,
                    chunk,
                    expected_md,
                    partseq,
                    upload_headers,
                    file_name,
                )
                uploaded_block_list.append(uploaded_md)
                uploaded_bytes += len(chunk)
                _emit_progress(uploaded_bytes, partseq + 1, total_parts)
                logging.info(
                    "Terabox chunk uploaded: %s part %s of %s",
                    file_name,
                    partseq + 1,
                    total_parts,
                )
        else:
            upload_url = build_upload_url(
                remote_path,
                uploadid,
                Paths.TERABOX_APP_ID,
                0,
            )
            if Paths.TERABOX_BDSTOKEN:
                upload_url += f"&bdstoken={Paths.TERABOX_BDSTOKEN}"
            with open(file_path, "rb") as upload_buffer:
                file_content = upload_buffer.read()
            uploaded_md = _upload_chunk(
                session,
                upload_url,
                file_content,
                precreate_block_list[0],
                0,
                upload_headers,
                file_name,
            )
            uploaded_block_list.append(uploaded_md)
            uploaded_bytes = file_size
            _emit_progress(uploaded_bytes, 1, 1)
            logging.info(
                "Terabox file uploaded: %s (%s bytes)",
                file_name,
                file_size,
            )

        create_url = build_create_url(
            Paths.TERABOX_APP_ID,
            Paths.TERABOX_JS_TOKEN,
            dp_logid,
        )
        create_data = {
            "path": remote_path,
            "size": file_size,
            "uploadid": uploadid,
            "target_path": _normalize_remote_dir(remote_dir),
            "block_list": json.dumps(uploaded_block_list),
            "local_mtime": local_mtime,
            "isdir": "0",
            "rtype": "1",
        }
        if Paths.TERABOX_BDSTOKEN:
            create_data["bdstoken"] = Paths.TERABOX_BDSTOKEN

        logging.debug(
            "Terabox create metadata: file=%s size=%s uploadid=%s blocks=%s",
            file_name,
            file_size,
            uploadid,
            len(uploaded_block_list),
        )

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
