# copyright 2023 © Xron Trix | https://github.com/Xrontrix10


import logging
import asyncio
from natsort import natsorted
from datetime import datetime
from asyncio import sleep, get_running_loop
from os import makedirs, path as ospath
from colab_leecher.downlader.mega import megadl
from colab_leecher.utility.handler import cancelTask
from colab_leecher.downlader.terabox import terabox_download
from colab_leecher.downlader.ytdl import YTDL_Status, get_YT_Name
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from colab_leecher.downlader.aria2 import aria2_Download, get_Aria2c_Name
from colab_leecher.downlader.telegram import TelegramDownload, media_Identifier
from colab_leecher.utility.variables import (
    BOT,
    Gdrive,
    Transfer,
    MSG,
    Messages,
    Aria2c,
    BotTimes,
)
from colab_leecher.utility.helper import (
    isYtdlComplete,
    keyboard,
    sysINFO,
    is_google_drive,
    is_mega,
    is_terabox,
    is_ytdl_link,
    is_telegram,
    safe_edit_status,
)
from colab_leecher.downlader.gdrive import (
    build_service,
    g_DownLoad,
    get_Gfolder_size,
    getFileMetadata,
    getIDFromURL,
)


CONCURRENT_URL_LIMIT = 10
_ytdl_download_lock = asyncio.Lock()


async def _download_single_source(link: str, num: int, is_ytdl: bool, down_path: str):
    message = "\n<b>Please Wait...</b> ⏳\n<i>Merging YTDL Video...</i> 🐬"
    if is_ytdl or is_ytdl_link(link):
        # YTDL uses shared globals for progress, so we keep only one active YTDL download.
        async with _ytdl_download_lock:
            await YTDL_Status(link, num, down_path)
            try:
                await safe_edit_status(
                    text=Messages.task_msg + Messages.status_head + message + sysINFO(),
                    reply_markup=keyboard(),
                )
            except Exception:
                pass
            while not isYtdlComplete():
                await sleep(2)
        return

    if is_google_drive(link):
        await g_DownLoad(link, num, down_path)
    elif is_telegram(link):
        await TelegramDownload(link, num, down_path)
    elif is_mega(link):
        await megadl(link, num)
    elif is_terabox(link):
        tera_dn = (
            f"<b>PLEASE WAIT ⌛</b>\n\n__Generating Download Link For__\n\n<code>{link}</code>"
        )
        try:
            await safe_edit_status(text=tera_dn + sysINFO(), reply_markup=keyboard())
        except Exception as e1:
            print(f"Couldn't Update text ! Because: {e1}")
        await terabox_download(link, num, down_path)
    else:
        aria2_dn = (
            f"<b>PLEASE WAIT ⌛</b>\n\n__Getting Download Info For__\n\n<code>{link}</code>"
        )
        try:
            await safe_edit_status(text=aria2_dn + sysINFO(), reply_markup=keyboard())
        except Exception as e1:
            print(f"Couldn't Update text ! Because: {e1}")
        Aria2c.link_info = False
        await aria2_Download(link, num, down_path)


async def downloadManager(
    source,
    is_ytdl: bool,
    on_source_complete=None,
    concurrent: bool = False,
    max_workers: int = CONCURRENT_URL_LIMIT,
):
    BotTimes.task_start = datetime.now()
    if not concurrent:
        for i, link in enumerate(source):
            try:
                await _download_single_source(link, i + 1, is_ytdl, Paths.down_path)
                if on_source_complete is not None:
                    await on_source_complete(
                        {
                            "source_index": i + 1,
                            "source_url": link,
                            "download_dir": Paths.down_path,
                        }
                    )
            except Exception as Error:
                err = f"Download Error at Link {i + 1}: {link}\nError: {str(Error)}"
                await cancelTask(err)
                logging.error(err)
                return False
        return True

    semaphore = asyncio.Semaphore(max_workers)
    tasks = []

    async def _worker(index: int, link: str):
        async with semaphore:
            source_num = index + 1
            source_dir = ospath.join(Paths.down_path, f"source_{source_num:03d}")
            if not ospath.exists(source_dir):
                makedirs(source_dir)
            try:
                await _download_single_source(link, source_num, is_ytdl, source_dir)
            except Exception as error:
                raise RuntimeError(
                    f"Download failed\nURL: {link}\nSource: {source_num}\nFile: {source_dir}\nError: {str(error)}"
                )
            if on_source_complete is not None:
                await on_source_complete(
                    {
                        "source_index": source_num,
                        "source_url": link,
                        "download_dir": source_dir,
                    }
                )

    for i, link in enumerate(source):
        tasks.append(asyncio.create_task(_worker(i, link)))

    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)

    first_error = None
    for completed in done:
        exc = completed.exception()
        if exc is not None:
            first_error = exc
            break

    if first_error is not None:
        for pending_task in pending:
            pending_task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        err = f"Concurrent Download Error: {str(first_error)}"
        await cancelTask(err)
        logging.error(err)
        return False

    if pending:
        await asyncio.gather(*pending)

    return True


async def calDownSize(sources):
    global TRANSFER_INFO
    for link in natsorted(sources):
        if is_google_drive(link):
            await build_service()
            id = await getIDFromURL(link)
            try:
                meta = getFileMetadata(id)
            except Exception as e:
                if "File not found" in str(e):
                    err = "The file link you gave either doesn't exist or You don't have access to it!"
                elif "Failed to retrieve" in str(e):
                    err = "Authorization Error with Google ! Make Sure you generated token.pickle !"
                else:
                    err = f"Error in G-API: {e}"
                logging.error(err)
                await cancelTask(err)
            else:
                if meta.get("mimeType") == "application/vnd.google-apps.folder":
                    Transfer.total_down_size += get_Gfolder_size(id)
                else:
                    Transfer.total_down_size += int(meta["size"])
        elif is_telegram(link):
            media, _ = await media_Identifier(link)
            if media is not None:
                size = media.file_size
                Transfer.total_down_size += size
            else:
                logging.error("Couldn't Download Telegram Message")
        else:
            pass


async def get_d_name(link: str):
    global Messages, Gdrive
    if len(BOT.Options.custom_name) != 0:
        Messages.download_name = BOT.Options.custom_name
        return
    if is_google_drive(link):
        id = await getIDFromURL(link)
        meta = getFileMetadata(id)
        Messages.download_name = meta["name"]
    elif is_telegram(link):
        media, _ = await media_Identifier(link)
        Messages.download_name = media.file_name if hasattr(media, "file_name") else "None"  # type: ignore
    elif is_ytdl_link(link):
        Messages.download_name = await get_YT_Name(link)
    elif is_mega(link):
        Messages.download_name = (
            "Don't Know 🥲 (Trying)"  # TODO: Get download name via megadl
        )
    else:
        Messages.download_name = get_Aria2c_Name(link)
