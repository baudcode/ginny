import tqdm
import os
from typing import List, Tuple, Dict
import hashlib
import logging
import multiprocessing
import requests


def get_files(directory, extensions=None) -> List[str]:
    files = []
    extensions = [ext.lower() for ext in extensions] if extensions else None
    for root, _, filenames in sorted(os.walk(directory)):
        files += [os.path.join(root, name) for name in sorted(filenames)
                  if extensions is None or any([name.lower().endswith("." + ext) for ext in extensions])]

    return sorted(files)


def get_logger(name, level=logging.DEBUG, fh_level=logging.DEBUG, enable_file_logging=False, root_dir=''):

    logger = logging.getLogger(name)
    logger.propagate = False
    if not logger.handlers or len(logger.handlers) == 0:
        logger.setLevel(level)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s')
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    if enable_file_logging and not any([isinstance(handler, logging.FileHandler) for handler in logger.handlers]):
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s')
        fh = logging.FileHandler(os.path.join(root_dir, '%s.log' % name))
        fh.setLevel(fh_level)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    return logger


def batching(args, batch_size: int):
    for i in range(0, len(args), batch_size):
        yield args[i:(i + batch_size)]


def parallize(f, args, n_processes: int = None, desc: str = 'parallize_v3', tq=None, postfix={}, batch_size: int = 64):
    if n_processes == None:
        n_processes = multiprocessing.cpu_count()

    with multiprocessing.Pool(n_processes) as pool:
        total = len(args)
        results = []

        if total == 0:
            return []

        if (type(args[0]) == list or type(args[0]) == tuple) and len(args[0]) > 1:
            desc = '[batching] ' + desc
            results = []

            if tq is None:
                with tqdm.tqdm(total=total, desc=desc) as tq:
                    for batch in batching(args, batch_size):
                        results.extend(pool.starmap(f, batch))
                        tq.update(len(batch))

            else:
                for batch in batching(args, batch_size):
                    results.extend(pool.starmap(f, batch))
                    tq.set_postfix(done="%d/%d" % (len(results) + 1, total), desc=desc, **postfix)

        else:
            if tq is None:
                for r in tqdm.tqdm(
                        pool.imap(f, args), desc=desc, total=total):
                    results.append(r)
            else:
                for i, r in enumerate(pool.imap(f, args)):
                    tq.set_postfix(done="%d/%d" % (i + 1, total), desc=desc, **postfix)
                    results.append(r)

    return results


logger = get_logger("ginny")


def download(url: str, destination: str, auth: Tuple = None, headers: Dict[str, str] = None):
    r = requests.get(url, auth=auth, headers=headers)
    if r.status_code != 200:
        logger.error("url: %s | status code: %d" % (url, r.status_code))
        raise ValueError("status code == %d" % r.status_code)

    with open(destination, 'wb') as w:
        w.write(r.content)


def encode(data: str):
    m = hashlib.sha256()
    m.update(bytes(data, 'utf-8'))
    return m.hexdigest()
