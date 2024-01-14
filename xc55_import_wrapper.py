#!/usr/bin/python3
import os
from dataclasses import dataclass
from io import TextIOWrapper
from pathlib import Path, PurePosixPath
from mmap import mmap, PROT_READ
from shutil import which
from re import compile, Pattern
from fileinput import input, FileInput
from subprocess import run, Popen, CalledProcessError, STDOUT, PIPE, DEVNULL, TimeoutExpired, SubprocessError
from time import sleep
from datetime import datetime


# env vars
# XC_STORE_PATH
# IMPORT_PATH
# PHP_CLI - optional
# ITEMS_PER_WORKER - optional
@dataclass(frozen=True)
class TaskStatus:
    NEW = "NEW"
    CREATED = "CREATED"
    STARTED = "STARTED"
    COMPLETED = "COMPLETED"
    ERROR = "ERROR"


@dataclass
class Config:
    store_path: PurePosixPath
    import_path: PurePosixPath
    php_cmd: str
    worker_cmd: [str]
    import_cmd: [str]
    items_per_worker: int
    import_timeout: int = 3 * 3600  # 3h
    background_jobs: bool = False
    recursive: bool = False
    move_completed: bool = False
    verbose: bool = False
    report_to_file: PurePosixPath | None = None
    # report_to_email: bool = False


def init() -> Config:
    store_path = _normalize_path(path=os.environ.get("XC_STORE_PATH", "/var/www/html/"))
    import_path = _normalize_path(path=os.environ.get("IMPORT_PATH", "/var/www/html/var/import/"))
    if not Path(store_path, ".env.local").is_file():
        raise ValueError(f"Store path is not valid: {store_path}")
    if not Path(import_path).is_dir():
        raise ValueError(f"Import path is not valid: {import_path}")
    php_cmd = os.environ.get("PHP_CLI", which('php'))
    params = {
        'items_per_worker': int(os.environ.get("ITEMS_PER_WORKER", 5000)),
        'php_cmd': php_cmd,
        'worker_cmd': [php_cmd, f"{store_path}/bin/console", "messenger:consume", "async"],
        'import_cmd': [php_cmd, f"{store_path}/bin/console", "utils:import"],
        'report_to_file': PurePosixPath(store_path, "var/log/", f"import-{datetime.now().strftime('%Y-%m-%d')}.log"),
        'background_jobs': _are_background_jobs_enabled(store_path)
    }
    return Config(store_path=store_path, import_path=import_path, **params)


def do_report(cfg: Config, rep: [str]):
    if cfg.report_to_file is not None:
        with open(file=cfg.report_to_file, mode='ta') as f:
            f.write('\n'.join(rep))
            f.write(''.join(['\n', '-'*16, '\n']))


def _normalize_path(path: PurePosixPath | str, cfg: Config = None) -> PurePosixPath:
    p = PurePosixPath(os.path.normpath(path))
    return p if cfg is None or p.is_absolute() else PurePosixPath(cfg.store_path, p)


def _get_lines_number(path: PurePosixPath) -> int:
    lines = 0
    f = open(file=path, mode='tr')
    buf = mmap(fileno=f.fileno(), length=0, prot=PROT_READ)
    while buf.readline():
        lines += 1
    f.close()
    return lines


def _find_in_store_config_by_pattern(store_path: PurePosixPath, pattern: Pattern[str]) -> {str: str}:
    def _grep_by_pattern(fi: FileInput[str], p: Pattern[str]) -> [(str, str)]:
        return [(m.group(1).lower(), m.group(2).lower()) for s in fi if (m := p.search(s)) is not None]
    result = []
    for file_name in [f"{store_path}/.env", f"{store_path}/.env.local"]:
        try:
            with input(file_name) as file:
                result.extend(_grep_by_pattern(fi=file, p=pattern))
        except OSError:
            raise ValueError(f"Cannot read file: {file_name}")
    return dict(result)


def _are_background_jobs_enabled(store_path: PurePosixPath) -> bool:
    pattern = compile(r'(?mi)^(BACKGROUND_JOBS_ENABLED)\s*=\s*(true|false).*$')
    result = _find_in_store_config_by_pattern(store_path=store_path, pattern=pattern)
    return result.get('background_jobs_enabled', 'false') == 'true'


class Task:
    status: str = TaskStatus.NEW
    status_message: str = ""
    file: PurePosixPath
    lockfile: PurePosixPath
    config: Config
    workers_qty: int = 0
    workers: {int: Popen} = {}
    import_cmd: [str] = []
    report: [str] = []

    def __init__(self, cfg: Config):
        self.config = cfg

    def init(self, file: PurePosixPath) -> bool:
        self.report.clear()
        self.file = file
        self.lockfile = PurePosixPath(f"{self.file}.lock")
        if not (p := Path(self.file)).is_file() or p.stat().st_size == 0:
            self._error(f"File not found or empty: {self.file}")
            return False
        self.workers_qty = self._get_qty_of_workers() if self.config.background_jobs else 0
        self.import_cmd = [*self.config.import_cmd]
        if self.config.background_jobs:
            self.import_cmd.extend(['-w', 'true'])
        self.import_cmd.append(self.file)
        self.status = TaskStatus.CREATED
        self.status_message = f"Task for file {self.file} has been created"
        self.report.append(f"{datetime.now()}: Status: {self.status} - {self.status_message}")
        return True

    def start(self) -> bool:
        if self.status is not TaskStatus.CREATED:
            return False
        if self.config.background_jobs:
            self._start_workers()
        if not self._lock_file():
            self._error(f"File already locked: {self.file}")
            self._stop_workers()
            return False
        self.status = TaskStatus.STARTED
        self.status_message = f"Task for file {self.file} has been started"
        self.report.append(f"{datetime.now()}: Status: {self.status} - {self.status_message}")
        if not self._import():
            self._error(f"Error occurred during the import task for file {self.file}", unlock=True)
            self._stop_workers()
            return False
        self._stop_workers()
        if not self._unlock_file():
            self._error(f"Task for file {self.file} has been completed, but the file was not locked")
            return False
        self._success(f"Task for file {self.file} has been completed")
        self._remove_file()
        return True

    def get_report(self) -> str:
        return '\n'.join(self.report)

    def _error(self, msg: str, unlock: bool = False) -> None:
        self.status = TaskStatus.ERROR
        self.status_message = msg
        if unlock:
            self._unlock_file()
        self.report.append(f"{datetime.now()}: Status: {self.status} - {self.status_message}")

    def _success(self, msg: str):
        self.status = TaskStatus.COMPLETED
        self.status_message = msg
        self.report.append(f"{datetime.now()}: Status: {self.status} - {self.status_message}")

    def _start_worker(self) -> int | None:
        try:
            process = Popen(self.config.worker_cmd, stdout=DEVNULL, stderr=DEVNULL, text=True, shell=False)
            pid = process.pid
            self.workers[pid] = process
            return pid
        except OSError | ValueError | CalledProcessError | SubprocessError as e:
            self._error(f"Cannot run worker: {e}")
            return None

    def _stop_worker(self, pid: int):
        self.workers[pid].terminate()
        sleep(2)
        if self.workers[pid].poll() is None:
            self.workers[pid].kill()
            if (p := Path(f"{self.config.store_path}/var/workers/{pid}")).is_file():
                p.unlink()
        del self.workers[pid]

    def _start_workers(self):
        for _ in range(self.workers_qty):
            if (pid := self._start_worker()) is not None:
                self.report.append(f"Worker {pid} started")

    def _stop_workers(self):
        pids = [*self.workers.keys()]
        for pid in pids:
            self._stop_worker(pid)
            self.report.append(f"Worker {pid} stopped")

    def _import(self) -> bool:
        try:
            result = run(self.import_cmd, stdout=PIPE, stderr=STDOUT,
                         timeout=self.config.import_timeout, check=True, text=True, shell=False).stdout.strip()
            self.report.append(f"{datetime.now()}: Import output\n{result}")
            return True
        except OSError | ValueError | TimeoutExpired | CalledProcessError | TextIOWrapper as e:
            self._error(f"Cannot run import: {e}")
            return False

    def _get_qty_of_workers(self) -> int:
        offset = 0.05
        num_of_lines = _get_lines_number(self.file)
        num_of_workers, remainder = divmod(num_of_lines, self.config.items_per_worker)
        offset_lines = int(num_of_lines * offset)
        return num_of_workers if offset_lines > remainder else num_of_workers + 1

    def _lock_file(self) -> bool:
        lock = Path(self.lockfile)
        if lock.is_file():
            return False
        if len(self.workers) > 0:
            lock.write_text('\n'.join([str(pid) for pid in self.workers.keys()]))
        else:
            lock.touch()
        return True

    def _unlock_file(self) -> bool:
        lock = Path(self.lockfile)
        if lock.is_file():
            lock.unlink()
            return True
        return False

    def _remove_file(self):
        Path(self.file).unlink(missing_ok=True)
        self.report.append(f"{datetime.now()}: File removed: {self.file}")


if __name__ == '__main__':
    report = []
    config = init()
    import_task = Task(config)
    files_pattern = '**/*.csv' if config.recursive else '*.csv'
    files_list = [PurePosixPath(f) for f in Path(config.import_path).glob(files_pattern)]
    for f in files_list:
        if import_task.init(f):
            import_task.start()
        report.append(import_task.get_report())
    do_report(cfg=config, rep=report)
