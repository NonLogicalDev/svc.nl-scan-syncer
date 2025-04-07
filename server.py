from typing import Any, Literal, Dict, List, Iterable

import asyncio
import os
import sqlite3
import contextlib
import logging
import sys
import time
from pathlib import Path

import quart
import aiocron

#-------------------------------------------------------------------------------
# Configuration
#-------------------------------------------------------------------------------
APP_CFG_LISTEN = os.environ.get('APP_CFG_LISTEN', '0.0.0.0:5040')
APP_CFG_LOG_LEVEL = os.environ.get('APP_CFG_LOG_LEVEL', 'DEBUG')

APP_CFG_RECONCILE_CRON = os.environ.get('APP_CFG_RECONCILE_CRON', '*/1 * * * *')
APP_CFG_DATA_DIR = Path(os.environ.get('APP_CFG_DATA_DIR', '/app/data'))

APP_CFG_SRC_DIR = Path(os.environ.get('APP_CFG_SRC_DIR', '/app/syncer/src'))

APP_CFG_DST1_DIR = Path(os.environ.get('APP_CFG_DST1_DIR', '/app/syncer/dst1'))
APP_CFG_DST1_NICKNAME = os.environ.get('APP_CFG_DST1_NICKNAME', 'dst1')

APP_CFG_DST2_DIR = Path(os.environ.get('APP_CFG_DST2_DIR', '/app/syncer/dst2'))
APP_CFG_DST2_NICKNAME = os.environ.get('APP_CFG_DST2_NICKNAME', 'dst2')

#-------------------------------------------------------------------------------

logging.basicConfig(
    level=APP_CFG_LOG_LEVEL,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)  # Log to stdout
    ]
)
logger = logging.getLogger(__name__)

#-------------------------------------------------------------------------------

class SyncWorker:
    def __init__(self):
        self.data_dir = APP_CFG_DATA_DIR
        self.src_dir = APP_CFG_SRC_DIR
        self.dst1_dir = APP_CFG_DST1_DIR
        self.dst2_dir = APP_CFG_DST2_DIR

        self.init_db()

        for dir in [self.data_dir, self.src_dir, self.dst1_dir, self.dst2_dir]:
            Path(dir).mkdir(parents=True, exist_ok=True)
    
    @contextlib.contextmanager
    def connect_db(self):
        conn = sqlite3.connect(self.data_dir / 'syncer.db')
        conn.row_factory = sqlite3.Row
        yield conn
        conn.close()
    
    def init_db(self):
        with self.connect_db() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS file_state (
                    file_path TEXT PRIMARY KEY,
                    sync_state TEXT,

                    sync_dst1_state TEXT,
                    sync_dst1_error TEXT,
                    sync_dst1_updated_at INTEGER,

                    sync_dst2_state TEXT,
                    sync_dst2_error TEXT,
                    sync_dst2_updated_at INTEGER,

                    first_seen_at INTEGER,
                    updated_at INTEGER
                )
            """)
            conn.commit()
    
    def db_init_file(self, conn: sqlite3.Connection, file_path: Path):
        ts_now = int(time.time())

        file_state = conn.execute("""
            SELECT 1 FROM file_state WHERE file_path = ?
        """, (str(file_path),)).fetchone()
        if not file_state:
            # insert if not exists  
            conn.execute("""
                INSERT INTO file_state (file_path, sync_state, sync_dst1_state, sync_dst2_state, first_seen_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (str(file_path), 'pending', 'pending', 'pending', ts_now, ts_now))
    
    def db_update_file_dst_state(self, conn: sqlite3.Connection, file_path:Path, dst_selector: Literal['dst1', 'dst2'], state, error=None):
        ts_now = int(time.time())
        self.db_init_file(conn, file_path)

        # Update the state
        conn.execute(f"""
            UPDATE file_state SET
                sync_{dst_selector}_state = ?,
                sync_{dst_selector}_error = ?,
                sync_{dst_selector}_updated_at = ?
            WHERE file_path = ?
        """, (state, error, ts_now, str(file_path)))
    
    def db_update_file_sync_state(self, conn: sqlite3.Connection, file_path:Path, state):
        ts_now = int(time.time())
        self.db_init_file(conn, file_path)

        # Update the state
        conn.execute(f"""
            UPDATE file_state SET
                sync_state = ?,
                updated_at = ?
            WHERE file_path = ?
        """, (state, ts_now, str(file_path)))
    
    def get_file_status(self, conn: sqlite3.Connection, file_path: Path):
        # Fetch all fields of the file_state table
        return conn.execute("""
            SELECT * FROM file_state WHERE file_path = ?
        """, (str(file_path),)).fetchone()
    
    def get_all_file_status(self, conn: sqlite3.Connection):
        return conn.execute("""
            SELECT * FROM file_state
        """).fetchall()
    
    async def sync_run(self):
        # Get all files from the src_dir
        files = list(self.src_dir.glob('**/*'))
        with self.connect_db() as conn:
            for file in files:
                self.db_init_file(conn, file)

                # If Dst1 is not synced, sync it
                file_status = self.get_file_status(conn, file)
                if file_status['sync_state'] != 'synced':
                    logger.info(f"{file} :: Processing because it is not synced")
                else:
                    logger.info(f"{file} :: Skipping because it is already synced")
                    continue

                if file_status['sync_dst1_state'] != 'synced':
                    try:    
                        await self.sync_file(file, 'dst1')
                        self.db_update_file_dst_state(conn, file, 'dst1', 'synced')
                        logger.info(f"{file} :: Done Syncing (dst1)")
                    except Exception as e:
                        logger.error(f"{file} :: Error syncing file (dst1): {e}")
                        self.db_update_file_dst_state(conn, file, 'dst1', 'error', str(e))

                # If Dst2 is not synced, sync it
                file_status = self.get_file_status(conn, file)
                if file_status['sync_dst2_state'] != 'synced':
                    try:
                        await self.sync_file(file, 'dst2')
                        self.db_update_file_dst_state(conn, file, 'dst2', 'synced')
                        logger.info(f"{file} :: Done Syncing (dst2)")
                    except Exception as e:
                        logger.error(f"{file} :: Error syncing file (dst2): {e}")
                        self.db_update_file_dst_state(conn, file, 'dst2', 'error', str(e))

                file_status = self.get_file_status(conn, file)
                if file_status['sync_dst1_state'] == 'synced' and file_status['sync_dst2_state'] == 'synced':
                    # Delete the file from the src_dir
                    file.unlink()
                    self.db_update_file_sync_state(conn, file, 'synced')
                    logger.info(f"{file} :: Done Syncing, deleted (src)")

                conn.commit()
    
    async def sync_file(self, file, dst_selector):
        dst_dir = self.dst1_dir if dst_selector == 'dst1' else self.dst2_dir
        target_path = dst_dir / file.relative_to(self.src_dir)

        # Shell out to rsync
        cmd = ['rsync', '-av', file, target_path]

        proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, shell=False)
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            raise Exception(f"rsync failed with return code {proc.returncode}: {stderr.decode()}")

            
#-------------------------------------------------------------------------------
# Quart App
#-------------------------------------------------------------------------------


app = quart.Quart(__name__)
app_loop = asyncio.new_event_loop()
sync_state = SyncWorker()

@app.before_serving
async def app_before_serving():
    logger.info("Starting Quart app")

# Run every 1 minute
@aiocron.crontab(APP_CFG_RECONCILE_CRON, loop=app_loop)
async def sync_cron():
    logger.info("Running sync cron")
    await sync_state.sync_run()

@app.route('/')
async def app_index():
    return "Hello from py-doc-syncer!"

@app.route('/reset', methods=['POST'])
async def app_post_reset():
    # Drop all tables
    with sync_state.connect_db() as conn:
        conn.execute("DROP TABLE IF EXISTS file_state")
        conn.commit()
    
    # Recreate the sync_state
    sync_state.init_db()
    return "RESET OK"


@app.route('/status', methods=['GET'])
async def app_get_status():
    # One line per file
    with sync_state.connect_db() as conn:
        lines = []
        for i, file in enumerate(sync_state.get_all_file_status(conn)):
            file_rel = Path(file['file_path']).relative_to(sync_state.src_dir)
            if i > 0:
                lines.append("")

            lines.append(f"{file['file_path']}: {file['sync_state']}")

            # Get Date representation
            dst1_updated_at = file['sync_dst1_updated_at']
            dst1_updated_at_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(dst1_updated_at))
            lines.append(f"  {APP_CFG_DST1_NICKNAME}: {file['sync_dst1_state']} ({dst1_updated_at_str})")
            if file['sync_dst1_error']:
                lines.append(f"    error: {file['sync_dst1_error']}")

            dst2_updated_at = file['sync_dst2_updated_at']      
            dst2_updated_at_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(dst2_updated_at))
            lines.append(f"  {APP_CFG_DST2_NICKNAME}: {file['sync_dst2_state']} ({dst2_updated_at_str})")
            if file['sync_dst2_error']:
                lines.append(f"    error: {file['sync_dst2_error']}")
            

        # return text/plain
        return quart.Response("\n".join(lines), content_type='text/plain')


@app.route('/trigger', methods=['POST'])
async def app_post_trigger():
    # Trigger the sync
    await sync_cron.run()
    return "TRIGGERED"

#-------------------------------------------------------------------------------
# Main
#-------------------------------------------------------------------------------

if __name__ == "__main__":
    host, port = APP_CFG_LISTEN.split(':')
    logger.info(f"Listening on {host}:{port}")
    app.run(host=host, port=int(port), loop=app_loop)