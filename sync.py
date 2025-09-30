import subprocess
from pathlib import Path
from typing import Optional

import typer
import yaml

from sync_log import Logger
from ui import CursorTool, UITool, blue_block, red_block, yellow_block
from utils import get_lsync_dir, popen_with_error_check

logger = Logger()

app = typer.Typer()

LSYNC_DIR = get_lsync_dir()

# TODO: move this into config file
SYNC_DIRS = ["common_sync", "sglang", "docker_workspace"]
DEFAULT_CONFIG = f"{LSYNC_DIR}/lsync_config.yaml"
RSYNCIGNORE = f"{LSYNC_DIR}/.rsyncignore"


def _sync_command(
    remote_dir: str,
    local_dir: str,
    delete: bool = False,
    back: bool = False,
    git_repo: bool = False,
    git_ignore: Optional[str] = None,
):
    if not back:
        src_dir, dst_dir = local_dir, remote_dir
    else:
        src_dir, dst_dir = remote_dir, local_dir

    rsync_cmd = [
        "rsync",
        "-ah",
        "--delete" if delete else "",
        "--info=progress2",
        f"--exclude-from={git_ignore}" if git_ignore else "",
        f"--exclude-from={RSYNCIGNORE}" if not back else "",
        "--exclude=.git" if not git_repo else "",
        src_dir,
        dst_dir,
    ]
    # remove empty strings
    rsync_cmd = [cmd for cmd in rsync_cmd if cmd]
    typer.echo(f"Executing: \x1b[42m{' '.join(rsync_cmd)}\x1b[0m")

    return rsync_cmd


class SyncTool:
    def __init__(
        self,
        server_config: dict,
        file_or_path: Optional[str],
        master: Optional[str],
        delete: bool,
        back: bool,
        git_repo: bool,
    ):
        self.server_config = server_config
        self.hosts = self.server_config["hosts"]
        self.ancestor_to_sync = self.find_ancestor_to_sync()

        if file_or_path is None:
            self.local_dir = self.ancestor_to_sync
            self.remote_dir = Path(self.server_config["base_dir"]) / self.local_dir.name
        else:
            self.local_dir = Path.cwd() / file_or_path
            relative_path = self.local_dir.relative_to(self.ancestor_to_sync.parent)
            self.remote_dir = Path(self.server_config["base_dir"]) / relative_path

        # arguments
        self.master = master
        self.delete = delete
        self.back = back
        self.git_repo = git_repo
        self.git_ignore = self._probe_gitignore()

        self.__post_init__()

        CursorTool.clear_screen()

        # Info
        if self.delete:
            typer.echo(
                f"{yellow_block('#'*28)}\n"
                f"{yellow_block('# Delete option is enabled #')}\n"
                f"{yellow_block('#'*28)}"
            )

        if self.back:
            typer.echo(
                f"{red_block('#'*28)}\n"
                f"{red_block('# Back option is enabled #')}\n"
                f"{red_block('#'*28)}"
            )

        logger.print_last_log()

        src, dst = ("macbook", self.hosts) if not self.back else (self.hosts, "macbook")
        relative_path = self.local_dir.relative_to(self.ancestor_to_sync.parent)
        typer.echo(
            f"Syncing folder {blue_block(relative_path)} from "
            f"{blue_block(src)} -> {blue_block(dst)} "
        )

    def __post_init__(self):
        if not isinstance(self.hosts, list):
            self.hosts = [self.hosts]

        if len(self.hosts) > 1 and self.back:
            if self.master is None:
                raise typer.Exit(f"master must be set when syncing back")
            self.hosts = [h for h in self.hosts if h == self.master]

    def find_ancestor_to_sync(self) -> Path:
        d = Path.cwd()
        while d.as_posix() != "/":
            if d.name in SYNC_DIRS:
                return d
            d = d.parent
        raise typer.Exit(f"No ancestor directory in {SYNC_DIRS} found in {Path.cwd()}")

    def _probe_gitignore(self) -> Optional[str]:
        gitignore_file = self.local_dir / ".gitignore"
        return gitignore_file.as_posix() if gitignore_file.exists() else None

    def _ui_thread(self, rsync_procs: list[subprocess.Popen]):
        with UITool.ui_tool(len(rsync_procs)) as ui_tool:
            while not all(p.poll() is not None for p in rsync_procs):
                for i, p in enumerate(rsync_procs):
                    if p.stdout and (char := p.stdout.read(1)):
                        ui_tool.update_char(i, char)

    def sync(self):
        rsync_cmds = []
        for host in self.hosts:
            # adding trailing slash to sync the content of the directory
            is_folder = "/" if self.local_dir.is_dir() else ""
            rsync_cmds.append(
                _sync_command(
                    f"{host}:{self.remote_dir.as_posix()}{is_folder}",
                    f"{self.local_dir.as_posix()}{is_folder}",
                    self.delete,
                    self.back,
                    self.git_repo,
                    self._probe_gitignore(),
                )
            )

        input("Press Enter to continue...")
        CursorTool.clear_screen()
        relative_path = self.local_dir.relative_to(self.ancestor_to_sync.parent)
        typer.echo(
            f"Syncing local folder {blue_block(relative_path)} with remote hosts {blue_block(self.hosts)}"
            f"\n(delete={self.delete})"
            f"\n(back={self.back})"
            f"\n(git_repo={self.git_repo})"
            f"\n===================================================================="
        )

        rsync_procs: list[subprocess.Popen] = []
        for cmd in rsync_cmds:
            rsync_procs.append(popen_with_error_check(cmd))

        self._ui_thread(rsync_procs)

        for rsync_proc in rsync_procs:
            rsync_proc.wait()

        logger.log_one(
            path=self.local_dir.relative_to(self.ancestor_to_sync.parent),
            hosts=self.hosts,
            back=self.back,
            delete=self.delete,
            git_repo=self.git_repo,
        )

        logger.print_last_log()


@app.command()
def sync(
    server: str = typer.Option(..., "--server", "-n"),
    file_or_path: Optional[str] = typer.Option(None, "--file-or-path", "-f"),
    master: Optional[str] = typer.Option(None, "--master", "-m"),
    delete: bool = typer.Option(False, "--delete", "-d"),
    back: bool = typer.Option(False, "--back", help="sync from remote to local"),
    git_repo: bool = typer.Option(False, "--git", "-g", help="sync git repo"),
    config: str = typer.Option(DEFAULT_CONFIG, "--config"),
):
    # read yaml from config
    with open(config, "r") as f:
        config_dict = yaml.safe_load(f)

    if server not in config_dict:
        raise typer.Exit(f"Invalid server(cluster) name: {server}")

    sync_tool = SyncTool(
        config_dict[server],
        file_or_path=file_or_path,
        master=master,
        delete=delete,
        back=back,
        git_repo=git_repo,
    )

    sync_tool.sync()


if __name__ == "__main__":
    app()
