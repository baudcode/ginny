import io
import logging
from pathlib import Path
from typing import Any, List, Optional, Union

from .base import Task
from .utils import logger  # noqa

try:
    import paramiko
    from paramiko.client import AutoAddPolicy, SSHClient
except:
    logger.warning("SSH Tasks cannot be used because 'paramiko' is not installed. <pip install paramiko>")


def get_client(ip: str, username: str, pem: Optional[Union[str, Path]] = None):

    client = SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(AutoAddPolicy())

    if pem is not None:
        if isinstance(pem, Path):
            logger.info(f"connecting via pem {pem}")
            private_key = paramiko.RSAKey.from_private_key(io.StringIO(pem))
            client.connect(ip, username=username, pkey=private_key)
        else:
            logger.info(f"connecting via pem path={pem}")
            client.connect(ip, username='ubuntu', key_filename=pem)
    else:
        client.connect(ip, username=username)

    return client


def upload_file(client: "SSHClient", local_path: Union[str, Path], remote_path: Union[str, Path]):
    ftp_client = client.open_sftp()

    ftp_client.put(
        str(Path(local_path).absolute()),
        str(Path(remote_path).absolute())
    )

    ftp_client.close()


def download_file(client: "SSHClient", remote_path: Union[str, Path], local_path: Union[str, Path]):
    ftp_client = client.open_sftp()

    ftp_client.put(
        str(Path(remote_path).absolute()),
        str(Path(local_path).absolute())
    )

    ftp_client.close()



class SSHCommandTask(Task):

    ip: str
    username: str
    cmd: List[str]
    pem: Union[str, Path] = None

    def run(self):

        logging.info(f"=> executing {self.cmd}")

        client = ssh.get_client(self.ip, self.username, pem=self.pem)
        stdin, stdout, stderr = client.exec_command(" ".join(self.cmd))

        stderr = stderr.readlines()

        if len(stderr) > 0:
            raise Exception(f"failed to execute {self.cmd} - stderr: {stderr}")

        out = stdout.readlines()

        with self.target().open('w') as writer:
            writer.writelines(out)

        return out

    def target(self):
        return self._create_simple_local_target()
