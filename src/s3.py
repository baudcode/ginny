import codecs
from collections import namedtuple
import io
from pathlib import Path
import tqdm
import os
from dataclasses import dataclass
import logging
from typing import Any, Dict, List, Tuple, Union, Optional
import boto3
import botocore
import io
from PIL import Image
import numpy as np
from typing import Protocol
import functools
import ujson as json
import pickle
from timeit import default_timer
from .utils import logger

get_var = os.getenv


class StreamingBody(Protocol):
    def read(self) -> bytes:
        pass


def seekable_stream(stream: StreamingBody):
    # StreamingBody
    return io.BytesIO(stream.read())


class S3ReadWrite:

    def __init__(self, s3file: "S3File", mode: str, show_tq: bool = False, seekable: bool = False):
        """ Stream needs to be seekable for reading numpy arrays"""

        self.file = s3file
        self.mode = mode
        self.show_tq = show_tq
        self.seekable = seekable

        if mode == 'r':
            byte_stream = S3().get_file_stream(self.file.path, self.file.bucket)
            StreamReader = codecs.getreader('utf-8')
            self.stream = StreamReader(byte_stream)

        elif mode == 'rb':
            self.stream = S3().get_file_stream(self.file.path, self.file.bucket)
            if self.seekable:
                self.stream = seekable_stream(self.stream)

        elif mode == 'wb':
            self.stream = io.BytesIO()
        elif mode == 'w':
            self.stream = io.StringIO()
        else:
            raise Exception(f"mode {mode} not supported")

    def __enter__(self):
        return self.stream

    def __exit__(self, type, value, traceback):
        # persist the stream
        if self.mode == 'w' or self.mode == 'wb':
            num_bytes = self.stream.tell()
            self.stream.seek(0)

            if isinstance(self.stream, io.StringIO):
                data = self.stream.getvalue()
                bytestream = io.BytesIO(bytes(data, 'utf-8'))
                S3().write_stream(bytestream, self.file.path, self.file.bucket, num_bytes=num_bytes, show_tq=self.show_tq)
            else:
                S3().write_stream(self.stream, self.file.path, self.file.bucket, num_bytes=num_bytes, show_tq=self.show_tq)

        self.stream.close()

    def read(self, size: int = -1) -> bytes:
        if not hasattr(self.stream, "read"):
            self.stream = seekable_stream(self.stream)
        else:
            return self.stream.read(size)


S3Obj = namedtuple('S3Obj', ['key', 'mtime', 'size', 'ETag'])


@dataclass
class S3File:
    bucket: str
    path: str

    def upload(self, local_path: Union[str, Path], progress: bool = False) -> bool:
        if isinstance(local_path, Path):
            local_path = str(local_path.absolute())

        S3().upload_file(local_path, self.path, self.bucket, progress=progress)
        return self.exists()

    def download(self, path: Union[str, Path]):
        if isinstance(path, Path):
            path = str(path.absolute())

        S3().download_file(self.path, path, self.bucket)

    def unlink(self):
        return self.rmtree()

    @property
    def size(self) -> Optional[float]:
        """ File size via HEAD method. Might return invalid size incase of encryption
            returns: float (bytes) or None, if path is not found
        """
        response = S3().s3_client.head_object(Bucket=self.bucket, Key=self.path)
        if "ContentLength" not in response:
            return None
        return response['ContentLength']

    @property
    def parent(self):
        from pathlib import Path
        return S3File(path=str(Path(self.path).parent), bucket=self.bucket)

    @property
    def uri(self):
        return "s3://%s/%s" % (self.bucket, self.path)

    @classmethod
    def from_uri(cls, uri: str):
        bucket, path = S3.split_uri(uri)
        return S3File(bucket, path)

    def exists(self):
        return S3().file_exists(self.path, self.bucket) or S3().directory_exists(self.path, self.bucket)

    def open(self, mode: str, show_tq: bool = False, seekable: bool = False):
        return S3ReadWrite(self, mode, show_tq, seekable=seekable)

    def walk(self) -> List["S3File"]:
        paths = S3().list_directory(self.path, self.bucket)
        return list(map(lambda path: S3File(self.bucket, path), paths))

    def listdir(self, limit: int = -1):
        return list(map(lambda x: os.path.basename(x[:-1]), filter(lambda x: x.endswith("/"), self.list_prefixes(limit=limit))))

    def list_prefixes(self, limit: int = -1):
        return list(S3().list_prefixes_v2(self.path, self.bucket, limit=limit))

    def _head(self):
        return S3().s3_client.head_object(Bucket=self.bucket, Key=self.path)

    def content_length(self) -> float:
        return self._head()['ContentLength']

    def join(self, *args):
        f = self
        for a in args:
            f = f / a
        return f

    def __len__(self):
        return self.content_length()

    def __truediv__(self, other: str):
        new_path = os.path.join(self.path, other)
        return S3File(self.bucket, new_path)

    def rmtree(self):
        if not self.exists():
            return False

        S3().delete_directory(self.path, self.bucket)
        return True

    def mkdir(self, *args, **kwargs):
        """ ghost method because s3 directories don't need to be created """
        pass

    def read(self) -> bytes:
        """ make it behave like a stream """
        with self.open('rb') as reader:
            data = reader.read()

        return data

    def write_image(self, image: Union[np.ndarray, Image.Image], format: str = "JPEG", show_tq: bool = False):
        S3().write_image(image, self.path, self.bucket, format=format, show_tq=show_tq)

    def read_image(self, pil: bool = False) -> Union[np.ndarray, Image.Image]:
        return S3().read_image(self.path, self.bucket, pil=pil)

    def read_numpy(self):
        compressed = self.path.lower().endswith(".npz")

        with self.open("rb", seekable=True) as stream:
            if compressed:
                descriptor = np.load(stream)
                data = descriptor['data']
                descriptor.close()
            else:
                data = np.load(stream)

        return data

    def __check_numpy_file_ending(self, compressed: bool):
        if compressed:
            assert (self.path.endswith(".npz")), f"{self.path} has to end with .npz"
        else:
            assert (self.path.endswith(".npy")), f"{self.path} has to end with .npy"

    def write_arrays(self, data: Dict[str, np.ndarray], compressed: bool = True, show_tq: bool = False):
        self.__check_numpy_file_ending(compressed)
        with self.open('wb', show_tq=show_tq) as writer:
            if compressed:
                np.savez_compressed(writer, **data)
            else:
                np.save(writer, data)

    def read_arrays(self, compressed: bool = True, show_tq: bool = False) -> Dict[str, np.ndarray]:
        with self.open("rb", show_tq=show_tq, seekable=True) as stream:
            if compressed:
                data = np.load(stream, allow_pickle=True)
            else:
                data = np.load(stream, allow_pickle=True)

            if isinstance(data, np.ndarray):
                return data.tolist()  # returns dict :D
            else:
                keys = list(data.keys())
                ret = {d: data[d] for d in keys}
                return ret

    def write_numpy(self, data: np.ndarray, compressed: bool = True, show_tq: bool = False):
        # ensure correct line ending
        self.__check_numpy_file_ending(compressed)

        with self.open('wb', show_tq=show_tq) as writer:
            if compressed:
                np.savez_compressed(writer, data=data)
            else:
                np.save(writer, data)

    def read_text(self, encoding='utf-8'):
        return str(self.read(), encoding)

    def write_text(self, text: str):
        with self.open("w") as writer:
            writer.write(text)

    def copy(self, dst: "S3File", check_exists: bool = False) -> Optional["S3File"]:
        if S3().copy(self.bucket, self.path, dst.path, dst.bucket, check_exists=check_exists):
            return dst
        else:
            return None

    def write_json(self, json_data: dict):
        data = bytes(json.dumps(json_data), 'utf-8')
        S3().write_bytes(data, self.path, self.bucket)

    def read_json(self) -> dict:
        with self.open("r") as reader:
            data = reader.read()

        return json.loads(data)

    def read_dict(self) -> dict:
        with self.open("r") as reader:
            data = reader.read()
        return json.loads(data)

    """ csv """

    def read_csv(self, **kwargs):
        import pandas as pd
        with self.open("rb") as reader:
            df = pd.read_csv(reader, **kwargs)
        return df

    def write_csv(self, df, index: bool = False, **kwargs):
        import pandas as pd
        with self.open("wb") as writer:
            df.to_csv(writer, index=index, **kwargs)
        return df

    """ pickle """

    def read_pickle(self, **kwargs) -> Any:
        with self.open("rb", seekable=True) as reader:
            data = pickle.load(reader, **kwargs)
        return data

    def write_pickle(self, obj: Any) -> Any:
        with self.open("wb", seekable=True) as writer:
            data = pickle.dump(obj, writer)
        return data

    """ publicity functions"""

    def get_public_url(self):
        location = S3().s3_client.get_bucket_location(Bucket=self.bucket)['LocationConstraint']
        return "https://s3-%s.amazonaws.com/%s/%s" % (location, self.bucket, self.path)

    def make_public(self) -> bool:
        response = S3().s3_client.put_object_acl(
            ACL="public-read", Bucket=self.bucket, Key=self.path
        )
        return response['ResponseMetadata']['HTTPStatusCode'] == 200

    def is_publicly_accessible(self):
        grants = self._get_grants()

        def has_read_rights(g):
            grantee = g['Grantee']
            permission = g['Permission']

            if permission == "READ" and "URI" in grantee and grantee['URI'] == "http://acs.amazonaws.com/groups/global/AllUsers":
                return True

            return False

        return any(has_read_rights(g) for g in grants)

    def _get_grants(self):
        return S3().s3_client.get_object_acl(
            Bucket=self.bucket,
            Key=self.path,
        )['Grants']

    def meta(self) -> Optional[S3Obj]:

        results = list(S3().walk_v2(self.path, self.bucket, limit=1))
        if len(results) == 0:
            return None
        r: S3Obj = results[0]
        if r.key != self.path:
            return None

        return r


@dataclass
class S3Config:
    aws_secret_access_key: str
    aws_access_key_id: str

    region: str
    bucket: str

    profile: Optional[str]
    endpoint_url: Optional[str]


def get_config() -> S3Config:
    aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    aws_secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"]
    bucket = os.environ["S3_BUCKET"]

    s3_region = os.getenv("S3_REGION", "eu-west-1")

    return S3Config(**{
        "aws_secret_access_key": aws_secret_access_key,
        "region": s3_region,
        "aws_access_key_id": aws_access_key_id,
        "bucket": bucket,
        "profile": os.environ.get("AWS_PROFILE"),
        "endpoint_url": os.getenv("ENDPOINT_URL")
    })


class S3(object):
    _instace: Optional["S3"] = None

    MAX_SESSION_LENGTH = 60 * 60 * 11

    def __init__(self, *args, **kwargs):
        self.config = get_config()

        # check if init of this class was already called once
        has_session = hasattr(self, "start") and hasattr(self, "s3")

        if has_session and not self._session_exired():
            # logger.debug("reusing active session")
            pass
        else:
            self.invalidate_session()

    def invalidate_session(self):
        """ invalidates current session and recreates necessary properties of the class"""

        args = dict(endpoint_url=self.config.endpoint_url)
        logger.info("invalidate s3 session")

        if self.config.profile is None:
            access_key_id = self.config.aws_access_key_id
            logger.info(f"start s3 session using access key {access_key_id}")
            session = boto3
            args = dict(
                region_name=self.config.region,
                aws_access_key_id=access_key_id,
                aws_secret_access_key=self.config.aws_secret_access_key,

            )
        else:
            profile = self.config.profile
            logger.info(f"start s3 session using profile {profile}")
            session = boto3.session.Session(profile_name=profile)

        self.s3_client = session.client('s3', **args)
        self.s3 = session.resource('s3', **args)
        self.transfer = boto3.s3.transfer.S3Transfer(client=self.s3_client)

        # set the start of the new session
        self.start: float = default_timer()

    def _session_exired(self):
        total_seconds = default_timer() - self.start
        return total_seconds > self.MAX_SESSION_LENGTH

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, "_instance"):
            cls._instance = super().__new__(cls)

        return cls._instance

    @staticmethod
    def split_uri(uri: str) -> Tuple[str, str]:
        # format: s3://my-bucket/my-path
        assert (uri.startswith("s3://")), "uri has to start with s3://"
        start = uri[5:]
        split = start.split("/")
        bucket = split[0]
        path = "/".join(split[1:])
        return bucket, path

    @staticmethod
    def make_uri(bucket: str, path: str) -> str:
        return f"s3://{bucket}/{path}"

    @staticmethod
    def join_uri(uri: str, filename: str) -> str:
        s3_bucket, s3_path = S3.split_uri(uri)
        return S3.make_uri(s3_bucket, os.path.join(s3_path, filename))

    @property
    def default_bucket(self):
        return self.config.bucket

    @property
    def default_region(self):
        return self.config.region

    def copy(self, src_bucket: str, src_key: str, dst_key: str, dst_bucket: Optional[str] = None, check_exists: bool = False) -> bool:
        # returns whether or not the file was copied

        if dst_bucket == None:
            dst_bucket = src_bucket

        if check_exists and self.file_exists(dst_key, dst_bucket):
            return False

        return self.s3.Object(dst_bucket, dst_key).copy_from(CopySource={"Bucket": src_bucket, "Key": src_key})

    def file_exists(self, s3_path, bucket_name):
        # directory exists works as well for files
        return self.directory_exists(s3_path, bucket_name)

    def file_exists_head(self, s3_path: str, bucket_name: str):
        try:
            self.s3_client.head_object(Bucket=bucket_name, Key=s3_path)
            return True
        except botocore.errorfactory.ClientError as c:
            if "Error" in c.response and "Code" in c.response["Error"]:
                if c.response['Error']["Code"] == "403":
                    logger.error(f"403 Forbidden on s3://{bucket_name}/{s3_path}")

            return False

    def list_prefixes(self, s3_path: str, s3_bucket: str, limit: int = 1000) -> List[str]:
        prefix = s3_path[1:] if s3_path.startswith("/") else s3_path
        if not prefix.endswith("/"):
            prefix = prefix + "/"

        result = self.s3_client.list_objects(Bucket=s3_bucket, Delimiter="/", Prefix=prefix, MaxKeys=limit)
        result_prefixes = result.get("CommonPrefixes")

        if result_prefixes == None:
            return []

        return [o.get("Prefix") for o in result_prefixes]

    def list_prefixes_v2(self, s3_path: str, s3_bucket: str, limit: int = -1):
        prefix = s3_path[1:] if s3_path.startswith("/") else s3_path
        if not prefix.endswith("/"):
            prefix = prefix + "/"

        bucket = self.s3.Bucket(s3_bucket)
        paginator = bucket.meta.client.get_paginator('list_objects')
        num = 0
        args = dict(Bucket=bucket.name, Delimiter="/", Prefix=prefix)
        if limit > 0 and limit < 1000:
            args['MaxKeys'] = limit

        for result in paginator.paginate(**args):
            result_prefixes = result.get("CommonPrefixes")

            if result_prefixes == None:
                return []

            for r in result_prefixes:
                num += 1
                yield r.get("Prefix")

                if limit > 0 and num == limit:
                    break

            if limit > 0 and num == limit:
                break

    def directory_exists(self, s3_path, bucket_name) -> bool:

        try:
            result = self.s3_client.list_objects(Bucket=bucket_name, Prefix=s3_path, MaxKeys=1)
            if "Contents" in result:
                return True
            else:
                return False
        except botocore.errorfactory.ClientError as c:
            return False

    def upload_file(self, local_path, s3_path, bucket_name, progress: bool = False):
        if progress:
            with open(local_path, "rb") as reader:
                self._write_stream_with_progress(reader, s3_path, bucket_name, num_bytes=os.path.getsize(local_path), show_tq=True)
        else:
            self.transfer.upload_file(local_path, bucket_name, s3_path)

        logger.debug('S3: uploaded ' + local_path + ' to ' + s3_path)

    def download_file(self, s3_path, local_path, bucket_name, debug=True):
        """
        dir_parts = local_path.split('/')[:-1]
        act_dir = '/'
        for i in range(len(dir_parts)):
            act_dir = os.path.join(act_dir, dir_parts[i])
            if not os.path.exists(act_dir):
                os.makedirs(act_dir)
        """

        self.transfer.download_file(bucket_name, s3_path, local_path)
        if debug:
            logger.debug('S3: downloaded %s in bucket %s to %s' % (s3_path, bucket_name, local_path))

    def list_directory(self, s3_directory, bucket_name):
        """Get a list of all keys in an S3 bucket."""
        keys = []
        kwargs = {'Bucket': bucket_name, 'Prefix': s3_directory}
        while True:
            resp = self.s3_client.list_objects_v2(**kwargs)
            if "Contents" not in resp:
                break
            for obj in resp['Contents']:
                keys.append(obj['Key'])
            try:
                kwargs['ContinuationToken'] = resp['NextContinuationToken']
            except KeyError:
                break
        return keys

    def walk(self, s3_path: str, bucket_name: str):
        kwargs = {'Bucket': bucket_name, 'Prefix': s3_path}

        keys = []
        while True:
            resp = self.s3_client.list_objects_v2(**kwargs)
            if "Contents" not in resp:
                break

            for obj in resp['Contents']:
                keys.append(obj['Key'])
                # yield S3Obj(f['Key'], f['LastModified'], f['Size'], f['ETag'])
            try:
                kwargs['ContinuationToken'] = resp['NextContinuationToken']
            except KeyError:
                break
        return keys

    def walk_v2(self, s3_directory: str, bucket_name: str, limit: int = -1):
        # [S3Obj(f['Key'], f['LastModified'], f['Size'], f['ETag']) for f in resp['Contents']]
        kwargs = {'Bucket': bucket_name, 'Prefix': s3_directory}
        if limit > 0 and limit < 1000:
            kwargs['MaxKeys'] = limit

        num = 0

        while limit < 0 or num < limit:

            resp = self.s3_client.list_objects_v2(**kwargs)
            if "Contents" not in resp:
                break

            for f in resp['Contents']:
                num += 1
                yield S3Obj(f['Key'], f['LastModified'], f['Size'], f['ETag'])
                if limit > 0 and num == limit:
                    break

            try:
                kwargs['ContinuationToken'] = resp['NextContinuationToken']
            except KeyError:
                break

    def download_directory(self, s3_directory, local_directory, bucket_name):
        s3_list = self.list_directory(s3_directory, bucket_name)

        with tqdm.tqdm(total=len(s3_list), desc='downloading files from s3') as tq:
            for s3_url in s3_list:
                s3_dir = os.path.dirname(s3_url).replace(s3_directory, '')
                if len(s3_dir) > 0 and s3_dir[0] == '/':
                    s3_dir = s3_dir[1:]
                local_dir = os.path.join(local_directory, s3_dir)
                if not os.path.isdir(local_dir):
                    os.makedirs(local_dir)
                local_fn = os.path.join(local_dir, s3_url.split('/')[-1])
                try:
                    self.download_file(s3_url, local_fn, bucket_name, debug=False)
                    tq.set_postfix(file=s3_url.encode('ascii', "ignore"))
                except Exception as e:
                    logger.error('[S3] not downloading %s to %s (bucket=%s)' % (s3_url, local_fn, bucket_name))
                tq.update(1)

    def read_image(self, s3_path, bucket_name, pil: bool = False) -> np.ndarray:
        from PIL import Image

        obj = self.s3.Bucket(bucket_name).Object(s3_path)
        response = obj.get()
        stream = response['Body']
        image = Image.open(stream)
        if pil:
            return image

        return np.array(image)

    def read_bytes(self, s3_path, bucket_name):
        obj = self.s3.Bucket(bucket_name).Object(s3_path)
        response = obj.get()
        stream = response['Body']
        return stream.read()

    def get_file_stream(self, s3_path, bucket_name):
        obj = self.s3.Bucket(bucket_name).Object(s3_path)
        response = obj.get()
        stream = response['Body']
        return stream

    def write_bytes(self, bytes, s3_path, bucket_name, show_tq: bool = False):
        return self._write_stream_with_progress(io.BytesIO(bytes), s3_path, bucket_name, num_bytes=len(bytes), show_tq=show_tq)

    def _write_progress(self, num_bytes: int, tq):
        tq.update(num_bytes)

    def _write_stream_with_progress(self, stream, s3_path: str, bucket_name: str, num_bytes: int = None, show_tq: bool = True):
        total = None if num_bytes == None else num_bytes
        bar_format = "{desc}: {percentage:.1f}%|{bar}| {n:.2f}/{total:.2f} [{elapsed}<{remaining}]"
        unit_scale = 1. / (1024 * 1024)

        if show_tq and total != None:
            with tqdm.tqdm(desc=f"uploading {self.make_uri(bucket_name, s3_path)}", unit='MB', bar_format=bar_format, total=total, unit_scale=unit_scale) as tq:
                return self.s3.Bucket(bucket_name).upload_fileobj(stream, s3_path, Callback=lambda x, tq=tq: self._write_progress(x, tq))
        else:
            return self.s3.Bucket(bucket_name).upload_fileobj(stream, s3_path)

    def write_stream(self, stream, s3_path, bucket_name, num_bytes: int = None, show_tq: bool = True):
        return self._write_stream_with_progress(stream, s3_path, bucket_name, num_bytes=num_bytes, show_tq=show_tq)

    def write_url(self, file_url: str, s3_path: str, bucket: str, show_tq: bool = False):
        # writes url stream to s3
        import requests

        with requests.get(file_url, stream=True) as r:
            content_length = r.headers.get('Content-Length', None)
            S3().write_stream(r.raw, s3_path, bucket, show_tq=False)

    def write_image(self, image: Union[np.ndarray, Image.Image], s3_path, bucket_name, format='PNG', show_tq: bool = False):
        stream = io.BytesIO()

        if not isinstance(image, Image.Image):
            image = Image.fromarray(image)
        else:
            if image.format != None:
                logger.debug(f"using pil image format {image.format}")
                format = image.format

        image.save(stream, format=format, quality=100)

        # get the number of bytes to write by the current stream position
        num_bytes = stream.tell()

        # seek to the start to write the whole file
        stream.seek(0)

        self.write_stream(stream, s3_path, bucket_name, num_bytes=num_bytes, show_tq=show_tq)

    def delete_directory(self, s3_directory, bucket_name):
        s3_list = self.list_directory(s3_directory, bucket_name)
        with tqdm.tqdm(total=len(s3_list), desc='deleting files from s3') as tq:
            for path in s3_list:
                self.delete_file(path, bucket_name, debug=False)
                tq.set_postfix(file=path.encode('ascii', "ignore"))
                tq.update(1)

    def delete_file(self, s3_path, bucket_name, debug=True) -> bool:
        try:
            self.s3.Object(bucket_name, s3_path).delete()
            if debug:
                logger.debug("S3: deleted %s from bucket %s" % (s3_path, bucket_name))
            return True
        except Exception as e:
            return False

    def create_bucket(self, bucket_name):
        """ https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.create_bucket """

        self.s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={
                                     'LocationConstraint': 'eu-central-1'})

    def guess_media_type(self, ext) -> Optional[str]:
        m = {
            "png": "image/png",
            "jpg": "image/jpeg",
            "jpeg": "image/jpeg",
            "pdf": "application/pdf",
            "xml": "application/xml",
            "zip": "application/zip",
            "ppt": "application/vnd.ms-powerpoint",
            "xls": "application/vnd.ms-excel",
            "doc": "application/msword",
            "pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
            "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "odt": "application/vnd.oasis.opendocument.text",
            "csv": "text/csv",
            "html": "text/html",
            "txt": "text/plain",
            "dxf": "image/vnd.dxf",
            "svg": "image/svg+xml",
            "json": "application/json"
        }
        if ext in m:
            return m[ext]
        else:
            return None


if __name__ == '__main__':
    _test()
