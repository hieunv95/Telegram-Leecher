# copyright 2024 © Xron Trix | https://github.com/Xrontrix10


import pytz
import asyncio
import shutil
import logging
from time import time
from datetime import datetime
from asyncio import sleep
from os import makedirs, path as ospath, system
from colab_leecher import OWNER, colab_bot, DUMP_ID
from colab_leecher.downlader.manager import calDownSize, get_d_name, downloadManager
from colab_leecher.utility.helper import (
    getSize,
    getTime,
    applyCustomName,
    keyboard,
    sizeUnit,
    status_bar,
    safe_edit_status,
    reset_status_edit_cache,
    sysINFO,
    is_google_drive,
    is_telegram,
    is_ytdl_link,
    is_mega,
    is_terabox,
    is_torrent,
)
from colab_leecher.utility.handler import (
    Leech,
    Unzip_Handler,
    Zip_Handler,
    SendLogs,
    cancelTask,
)
from colab_leecher.uploader.terabox import (
    upload_to_terabox,
    validate_terabox_credentials,
    precheck_terabox_upload_session,
)
from colab_leecher.utility.variables import (
    BOT,
    MSG,
    BotTimes,
    Messages,
    Paths,
    Aria2c,
    Transfer,
    TaskError,
)


async def task_starter(message, text):
    global BOT
    await message.delete()
    BOT.State.started = True
    if BOT.State.task_going == False:
        src_request_msg = await message.reply_text(text)
        return src_request_msg
    else:
        msg = await message.reply_text(
            "I am already working ! Please wait until I finish !!"
        )
        await sleep(15)
        await msg.delete()
        return None


async def taskScheduler():
    global BOT, MSG, BotTimes, Messages, Paths, Transfer, TaskError
    src_text = []
    is_dualzip, is_unzip, is_zip, is_dir = (
        BOT.Mode.type == "undzip",
        BOT.Mode.type == "unzip",
        BOT.Mode.type == "zip",
        BOT.Mode.mode == "dir-leech",
    )
    # Reset Texts
    Messages.download_name = ""
    Messages.task_msg = f"<b>🦞 TASK MODE » </b>"
    Messages.dump_task = (
        Messages.task_msg
        + f"<i>{BOT.Mode.type.capitalize()} {BOT.Mode.mode.capitalize()} as {BOT.Setting.stream_upload}</i>\n\n<b>🖇️ SOURCES » </b>"
    )
    Transfer.sent_file = []
    Transfer.sent_file_names = []
    Transfer.down_bytes = [0, 0]
    Transfer.up_bytes = [0, 0]
    Messages.download_name = ""
    Messages.task_msg = ""
    Messages.status_head = f"<b>📥 DOWNLOADING » </b>\n"

    if is_dir:
        if not ospath.exists(BOT.SOURCE[0]):
            TaskError.state = True
            TaskError.text = "Task Failed. Because: Provided Directory Path Not Exists"
            logging.error(TaskError.text)
            return
        if not ospath.exists(Paths.temp_dirleech_path):
            makedirs(Paths.temp_dirleech_path)
        Messages.dump_task += f"\n\n📂 <code>{BOT.SOURCE[0]}</code>"
        Transfer.total_down_size = getSize(BOT.SOURCE[0])
        Messages.download_name = ospath.basename(BOT.SOURCE[0])
    else:
        for link in BOT.SOURCE:
            if is_telegram(link):
                ida = "💬"
            elif is_google_drive(link):
                ida = "♻️"
            elif is_torrent(link):
                ida = "🧲"
                Messages.caution_msg = "\n\n⚠️<i><b> Torrents Are Strictly Prohibited in Google Colab</b>, Try to avoid Magnets !</i>"
            elif is_ytdl_link(link):
                ida = "🏮"
            elif is_terabox(link):
                ida = "🍑"
            elif is_mega(link):
                ida = "💾"
            else:
                ida = "🔗"
            code_link = f"\n\n{ida} <code>{link}</code>"
            if len(Messages.dump_task + code_link) >= 4096:
                src_text.append(Messages.dump_task)
                Messages.dump_task = code_link
            else:
                Messages.dump_task += code_link

    # Get the current date and time in the specified time zone
    cdt = datetime.now(pytz.timezone("Asia/Kolkata"))
    dt = cdt.strftime(" %d-%m-%Y")
    Messages.dump_task += f"\n\n<b>📆 Task Date » </b><i>{dt}</i>"

    src_text.append(Messages.dump_task)

    if ospath.exists(Paths.WORK_PATH):
        shutil.rmtree(Paths.WORK_PATH)
        # makedirs(Paths.WORK_PATH)
        makedirs(Paths.down_path)
    else:
        makedirs(Paths.WORK_PATH)
        makedirs(Paths.down_path)
    Messages.link_p = str(DUMP_ID)[4:]

    try:
        system(f"aria2c -d {Paths.WORK_PATH} -o Hero.jpg {Aria2c.pic_dwn_url}")
    except Exception:
        Paths.HERO_IMAGE = Paths.DEFAULT_HERO

    MSG.sent_msg = await colab_bot.send_message(chat_id=DUMP_ID, text=src_text[0])

    if len(src_text) > 1:
        for lin in range(1, len(src_text)):
            MSG.sent_msg = await MSG.sent_msg.reply_text(text=src_text[lin], quote=True)

    Messages.src_link = f"https://t.me/c/{Messages.link_p}/{MSG.sent_msg.id}"
    Messages.task_msg += f"__[{BOT.Mode.type.capitalize()} {BOT.Mode.mode.capitalize()} as {BOT.Setting.stream_upload}]({Messages.src_link})__\n\n"

    await MSG.status_msg.delete()
    img = Paths.THMB_PATH if ospath.exists(Paths.THMB_PATH) else Paths.HERO_IMAGE
    MSG.status_msg = await colab_bot.send_photo(  # type: ignore
        chat_id=OWNER,
        photo=img,
        caption=Messages.task_msg
        + Messages.status_head
        + f"\n📝 __Starting DOWNLOAD...__"
        + sysINFO(),
        reply_markup=keyboard(),
    )
    reset_status_edit_cache(MSG.status_msg.caption or "")

    await calDownSize(BOT.SOURCE)

    if not is_dir:
        await get_d_name(BOT.SOURCE[0])
    else:
        Messages.download_name = ospath.basename(BOT.SOURCE[0])

    if is_zip:
        Paths.down_path = ospath.join(Paths.down_path, Messages.download_name)
        if not ospath.exists(Paths.down_path):
            makedirs(Paths.down_path)

    BotTimes.current_time = time()

    if BOT.Mode.mode == "terabox-mirror":
        await Do_Terabox_Mirror(BOT.SOURCE, BOT.Mode.ytdl)
    elif BOT.Mode.mode == "terabox-mirror-leech":
        await Do_Terabox_Mirror_Leech(BOT.SOURCE, BOT.Mode.ytdl)
    elif BOT.Mode.mode == "dropbox-mirror":
        await Do_Dropbox_Mirror(BOT.SOURCE, BOT.Mode.ytdl, is_zip, is_unzip, is_dualzip)
    elif BOT.Mode.mode == "mirror":
        await Do_Mirror(BOT.SOURCE, BOT.Mode.ytdl, is_zip, is_unzip, is_dualzip)
    elif BOT.Mode.mode == "dropbox-mirror-leech":
        await Do_Dropbox_Mirror_Leech(BOT.SOURCE, is_dir, BOT.Mode.ytdl, is_zip, is_unzip, is_dualzip)
    else:
        await Do_Leech(BOT.SOURCE, is_dir, BOT.Mode.ytdl, is_zip, is_unzip, is_dualzip)


def _terabox_progress_callback(loop, status_message):
    last_emit = [0.0]
    current_file = [""]
    start_time = [datetime.now()]

    def _callback(progress_info):
        file_name = progress_info.get("file_name", "Unknown file")
        file_index = progress_info.get("file_index", 1)
        total_files = progress_info.get("total_files", 1)
        partseq = progress_info.get("partseq", 0)
        total_parts = progress_info.get("total_parts", 0)
        remote_path = progress_info.get("remote_path", "")
        total_bytes = progress_info.get("total_bytes", progress_info.get("size", 0))
        uploaded_bytes = progress_info.get("uploaded_bytes")

        if uploaded_bytes is None:
            if total_parts > 0 and total_bytes > 0 and partseq > 0:
                uploaded_bytes = int((partseq / total_parts) * total_bytes)
            else:
                uploaded_bytes = total_bytes

        uploaded_bytes = max(0, min(uploaded_bytes, total_bytes))

        if current_file[0] != file_name:
            current_file[0] = file_name
            start_time[0] = datetime.now()

        elapsed = max((datetime.now() - start_time[0]).total_seconds(), 1e-3)
        speed = uploaded_bytes / elapsed
        percentage = (uploaded_bytes / total_bytes * 100) if total_bytes > 0 else 0.0
        eta_seconds = ((total_bytes - uploaded_bytes) / speed) if speed > 0 else 0.0

        now = time()
        is_terminal_update = uploaded_bytes >= total_bytes and total_bytes > 0
        if not is_terminal_update and now - last_emit[0] < 5:
            return
        last_emit[0] = now

        down_msg = (
            f"<b>⬆️ UPLOADING TO TERABOX » </b>\n"
            + f"<code>{file_name}</code>\n"
            + f"<b>File:</b> {file_index}/{total_files} | <b>Chunk:</b> {partseq}/{total_parts}\n"
            + f"<code>{remote_path}</code>\n"
        )

        def _log_future_result(done_future):
            try:
                # Don't call result() immediately; check status first
                if not done_future.done():
                    return
                # Only retrieve result if no exception occurred
                if done_future.exception() is not None:
                    logging.debug(f"Terabox progress update skipped: {done_future.exception()}")
            except (asyncio.InvalidStateError, asyncio.CancelledError):
                # Ignore state errors and cancellations
                pass
            except Exception as error:
                # Silently ignore other exceptions
                logging.debug(f"Terabox progress callback error: {type(error).__name__}")

        try:
            future = asyncio.run_coroutine_threadsafe(
                status_bar(
                    down_msg=down_msg,
                    speed=f"{sizeUnit(speed)}/s",
                    percentage=percentage,
                    eta=getTime(int(eta_seconds)),
                    done=sizeUnit(uploaded_bytes),
                    left=sizeUnit(total_bytes),
                    engine="Terabox 🍑",
                ),
                loop,
            )
            future.add_done_callback(_log_future_result)
        except RuntimeError:
            # Loop is closed or invalid, silently skip this update
            logging.debug("Terabox progress callback: event loop unavailable")

    return _callback


async def _run_streaming_transfer_pipeline(
    source,
    is_ytdl: bool,
    upload_telegram: bool,
    upload_terabox: bool,
):
    source_queue = asyncio.Queue()
    queue_stop = object()
    downloaded_dirs = []
    terabox_tasks = []
    terabox_semaphore = asyncio.Semaphore(4)

    async def _on_source_complete(item):
        await source_queue.put(item)

    async def _producer():
        try:
            ok = await downloadManager(
                source,
                is_ytdl,
                on_source_complete=_on_source_complete,
                concurrent=True,
            )
            if not ok:
                raise RuntimeError("Download stage failed")
        finally:
            await source_queue.put(queue_stop)

    async def _upload_source_to_terabox(source_dir: str, source_url: str, source_index: int):
        async with terabox_semaphore:
            try:
                await upload_to_terabox(source_dir, Paths.TERABOX_FOLDER)
            except Exception as error:
                raise RuntimeError(
                    f"Terabox upload failed\nURL: {source_url}\nSource: {source_index}\nFile: {source_dir}\nError: {str(error)}"
                )

    producer_task = asyncio.create_task(_producer())

    try:
        while True:
            for pending_terabox in list(terabox_tasks):
                if pending_terabox.done() and pending_terabox.exception() is not None:
                    raise pending_terabox.exception()  # type: ignore

            item = await source_queue.get()
            if item is queue_stop:
                break

            source_dir = item.get("download_dir")
            source_url = item.get("source_url")
            source_index = item.get("source_index")

            if source_dir and ospath.exists(source_dir):
                downloaded_dirs.append(source_dir)

            if upload_terabox and source_dir:
                terabox_tasks.append(
                    asyncio.create_task(
                        _upload_source_to_terabox(source_dir, source_url, source_index)
                    )
                )

            if upload_telegram and source_dir:
                try:
                    await Leech(source_dir, not upload_terabox)
                except Exception as error:
                    latest_file = Messages.download_name or source_dir
                    raise RuntimeError(
                        f"Telegram upload failed\nURL: {source_url}\nSource: {source_index}\nFile: {latest_file}\nError: {str(error)}"
                    )

        await producer_task

        if terabox_tasks:
            done, pending = await asyncio.wait(
                set(terabox_tasks), return_when=asyncio.FIRST_EXCEPTION
            )
            first_error = None
            for completed in done:
                if completed.exception() is not None:
                    first_error = completed.exception()
                    break

            if first_error is not None:
                for pending_task in pending:
                    pending_task.cancel()
                if pending:
                    await asyncio.gather(*pending, return_exceptions=True)
                raise first_error

            if pending:
                await asyncio.gather(*pending)

    except Exception:
        if not producer_task.done():
            producer_task.cancel()
            await asyncio.gather(producer_task, return_exceptions=True)
        for pending_terabox in terabox_tasks:
            if not pending_terabox.done():
                pending_terabox.cancel()
        await asyncio.gather(*terabox_tasks, return_exceptions=True)
        raise
    finally:
        if upload_terabox:
            for source_dir in downloaded_dirs:
                if ospath.exists(source_dir):
                    shutil.rmtree(source_dir)


async def Do_Terabox_Mirror(source, is_ytdl):
    is_ok, reason = validate_terabox_credentials()
    if not is_ok:
        await cancelTask(f"Terabox Credentials Error: {reason}")
        return

    precheck_result = await asyncio.to_thread(
        precheck_terabox_upload_session,
        Paths.TERABOX_FOLDER,
    )
    if not precheck_result.get("ok"):
        await cancelTask(f"Terabox Precheck Error: {precheck_result.get('reason')}")
        return

    try:
        await _run_streaming_transfer_pipeline(
            source,
            is_ytdl,
            upload_telegram=False,
            upload_terabox=True,
        )
    except Exception as e:
        await cancelTask(f"Terabox Upload Error: {str(e)}")
        return

    await SendLogs(False)


async def Do_Terabox_Mirror_Leech(source, is_ytdl):
    is_ok, reason = validate_terabox_credentials()
    if not is_ok:
        await cancelTask(f"Terabox Credentials Error: {reason}")
        return

    precheck_result = await asyncio.to_thread(
        precheck_terabox_upload_session,
        Paths.TERABOX_FOLDER,
    )
    if not precheck_result.get("ok"):
        await cancelTask(f"Terabox Precheck Error: {precheck_result.get('reason')}")
        return

    try:
        await _run_streaming_transfer_pipeline(
            source,
            is_ytdl,
            upload_telegram=True,
            upload_terabox=True,
        )
    except Exception as e:
        await cancelTask(str(e))
        return

    await SendLogs(True)


async def Do_Leech(source, is_dir, is_ytdl, is_zip, is_unzip, is_dualzip):
    if is_dir:
        for s in source:
            if not ospath.exists(s):
                logging.error("Provided directory does not exist !")
                await cancelTask("Provided directory does not exist !")
                return
            Paths.down_path = s
            if is_zip:
                await Zip_Handler(Paths.down_path, True, False)
                await Leech(Paths.temp_zpath, True)
            elif is_unzip:
                await Unzip_Handler(Paths.down_path, False)
                await Leech(Paths.temp_unzip_path, True)
            elif is_dualzip:
                await Unzip_Handler(Paths.down_path, False)
                await Zip_Handler(Paths.temp_unzip_path, True, True)
                await Leech(Paths.temp_zpath, True)
            else:
                if ospath.isdir(s):
                    await Leech(Paths.down_path, False)
                else:
                    Transfer.total_down_size = ospath.getsize(s)
                    makedirs(Paths.temp_dirleech_path)
                    shutil.copy(s, Paths.temp_dirleech_path)
                    Messages.download_name = ospath.basename(s)
                    await Leech(Paths.temp_dirleech_path, True)
    else:
        if not (is_zip or is_unzip or is_dualzip):
            try:
                await _run_streaming_transfer_pipeline(
                    source,
                    is_ytdl,
                    upload_telegram=True,
                    upload_terabox=False,
                )
            except Exception as e:
                await cancelTask(str(e))
                return

            await SendLogs(True)
            return

        await downloadManager(source, is_ytdl)

        Transfer.total_down_size = getSize(Paths.down_path)

        # Renaming Files With Custom Name
        applyCustomName()

        # Preparing To Upload
        if is_zip:
            await Zip_Handler(Paths.down_path, True, True)
            await Leech(Paths.temp_zpath, True)
        elif is_unzip:
            await Unzip_Handler(Paths.down_path, True)
            await Leech(Paths.temp_unzip_path, True)
        elif is_dualzip:
            print("Got into un doubled zip")
            await Unzip_Handler(Paths.down_path, True)
            await Zip_Handler(Paths.temp_unzip_path, True, True)
            await Leech(Paths.temp_zpath, True)
        else:
            await Leech(Paths.down_path, True)

    await SendLogs(True)


async def Do_Mirror(source, is_ytdl, is_zip, is_unzip, is_dualzip):
    if not ospath.exists(Paths.MOUNTED_DRIVE):
        await cancelTask(
            "Google Drive is NOT MOUNTED ! Stop the Bot and Run the Google Drive Cell to Mount, then Try again !"
        )
        return

    if not ospath.exists(Paths.mirror_dir):
        makedirs(Paths.mirror_dir)

    await downloadManager(source, is_ytdl)

    Transfer.total_down_size = getSize(Paths.down_path)

    applyCustomName()

    cdt = datetime.now()
    cdt_ = cdt.strftime("Uploaded » %Y-%m-%d %H:%M:%S")
    mirror_dir_ = ospath.join(Paths.mirror_dir, cdt_)

    if is_zip:
        await Zip_Handler(Paths.down_path, True, True)
        shutil.copytree(Paths.temp_zpath, mirror_dir_)
    elif is_unzip:
        await Unzip_Handler(Paths.down_path, True)
        shutil.copytree(Paths.temp_unzip_path, mirror_dir_)
    elif is_dualzip:
        await Unzip_Handler(Paths.down_path, True)
        await Zip_Handler(Paths.temp_unzip_path, True, True)
        shutil.copytree(Paths.temp_zpath, mirror_dir_)
    else:
        shutil.copytree(Paths.down_path, mirror_dir_)

    await SendLogs(False)

async def Do_Dropbox_Mirror(source, is_ytdl, is_zip, is_unzip, is_dualzip):
    if not ospath.exists(Paths.MOUNTED_DROPBOX):
        await cancelTask(
            "Dropbox is NOT MOUNTED ! Stop the Bot and Run the Dropbox Cell to Mount, then Try again !"
        )
        return

    if not ospath.exists(Paths.dropbox_mirror_dir):
        makedirs(Paths.dropbox_mirror_dir)

    await downloadManager(source, is_ytdl)

    Transfer.total_down_size = getSize(Paths.down_path)

    applyCustomName()

    #cdt = datetime.now()
    #cdt_ = cdt.strftime("Uploaded » %Y-%m-%d %H:%M:%S")
    #mirror_dir_ = ospath.join(Paths.dropbox_mirror_dir, cdt_)
    mirror_dir_ = Paths.dropbox_mirror_dir;

    if is_zip:
        await Zip_Handler(Paths.down_path, True, True)
        shutil.copytree(Paths.temp_zpath, mirror_dir_, dirs_exist_ok=True)
    elif is_unzip:
        await Unzip_Handler(Paths.down_path, True)
        shutil.copytree(Paths.temp_unzip_path, mirror_dir_, dirs_exist_ok=True)
    elif is_dualzip:
        await Unzip_Handler(Paths.down_path, True)
        await Zip_Handler(Paths.temp_unzip_path, True, True)
        shutil.copytree(Paths.temp_zpath, mirror_dir_, dirs_exist_ok=True)
    else:
        shutil.copytree(Paths.down_path, mirror_dir_, dirs_exist_ok=True)

    await SendLogs(False)

async def Do_Dropbox_Mirror_Leech(source, is_dir, is_ytdl, is_zip, is_unzip, is_dualzip):
    if not ospath.exists(Paths.MOUNTED_DROPBOX):
        await cancelTask(
            "Dropbox is NOT MOUNTED ! Stop the Bot and Run the Dropbox Cell to Mount, then Try again !"
        )
        return

    if not ospath.exists(Paths.dropbox_mirror_dir):
        makedirs(Paths.dropbox_mirror_dir)

    mirror_dir_ = Paths.dropbox_mirror_dir;
    if is_dir:
        for s in source:
            if not ospath.exists(s):
                logging.error("Provided directory does not exist !")
                await cancelTask("Provided directory does not exist !")
                return
            Paths.down_path = s
            if is_zip:
                await Zip_Handler(Paths.down_path, True, False)
                shutil.copytree(Paths.temp_zpath, mirror_dir_, dirs_exist_ok=True)
                await Leech(Paths.temp_zpath, True)
            elif is_unzip:
                await Unzip_Handler(Paths.down_path, False)
                shutil.copytree(Paths.temp_unzip_path, mirror_dir_, dirs_exist_ok=True)
                await Leech(Paths.temp_unzip_path, True)
            elif is_dualzip:
                await Unzip_Handler(Paths.down_path, False)
                await Zip_Handler(Paths.temp_unzip_path, True, True)
                shutil.copytree(Paths.temp_zpath, mirror_dir_, dirs_exist_ok=True)
                await Leech(Paths.temp_zpath, True)
            else:
                shutil.copytree(Paths.down_path, mirror_dir_, dirs_exist_ok=True)
                if ospath.isdir(s):
                    await Leech(Paths.down_path, False)
                else:
                    Transfer.total_down_size = ospath.getsize(s)
                    makedirs(Paths.temp_dirleech_path)
                    shutil.copy(s, Paths.temp_dirleech_path)
                    Messages.download_name = ospath.basename(s)
                    await Leech(Paths.temp_dirleech_path, True)
    else:
        await downloadManager(source, is_ytdl)

        Transfer.total_down_size = getSize(Paths.down_path)

        # Renaming Files With Custom Name
        applyCustomName()

        # Preparing To Upload
        if is_zip:
            await Zip_Handler(Paths.down_path, True, True)
            shutil.copytree(Paths.temp_zpath, mirror_dir_, dirs_exist_ok=True)
            await Leech(Paths.temp_zpath, True)
        elif is_unzip:
            await Unzip_Handler(Paths.down_path, True)
            shutil.copytree(Paths.temp_unzip_path, mirror_dir_, dirs_exist_ok=True)
            await Leech(Paths.temp_unzip_path, True)
        elif is_dualzip:
            print("Got into un doubled zip")
            await Unzip_Handler(Paths.down_path, True)
            await Zip_Handler(Paths.temp_unzip_path, True, True)
            shutil.copytree(Paths.temp_zpath, mirror_dir_, dirs_exist_ok=True)
            await Leech(Paths.temp_zpath, True)
        else:
            shutil.copytree(Paths.down_path, mirror_dir_, dirs_exist_ok=True)
            await Leech(Paths.down_path, True)

    await SendLogs(True)