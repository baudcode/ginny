from pathlib import Path
from typing import List, Union, Optional, Any
from .utils import logger  # noqa
import io

try:
    import paramiko
    from paramiko.client import SSHClient, AutoAddPolicy
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
