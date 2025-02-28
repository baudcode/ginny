""" pip install markdown """

try:
    import markdown
except ImportError:
    pass

import dataclasses
import datetime
import importlib
from multiprocessing import Lock, Manager
from pathlib import Path
from typing import List, Optional, Tuple, Union

import numpy as np
from PIL import Image

try:
    import pandas as pd
except ImportError:
    pass

from src.utils import logger


def module_exists(module_name):
    return importlib.util.find_spec(module_name) is not None

class Markdown:
    def __init__(self, text):
        self.text = text

    def __str__(self):
        # check if markdown_imported is defined
        # if not, return the text as is
        if not module_exists("markdown"):
            logger.warning("text will not be rendered as markdown because the module 'markdown' is not installed")
            return self.text
        return markdown.markdown(self.text)

def image2html(img: Image.Image):
    import base64
    from io import BytesIO

    buffered = BytesIO()
    img.save(buffered, format="JPEG")
    img_str = base64.b64encode(buffered.getvalue()).decode("utf-8")
    return f'<img src="data:image/jpeg;base64,{img_str}"/>'

Loggable = Union["pd.DataFrame", np.ndarray, Image.Image, Markdown, Path, datetime.date, datetime.datetime]

@dataclasses.dataclass
class LogContext:
    task: "Task"
    ts: datetime.datetime = dataclasses.field(default_factory=datetime.datetime.now)
    from_file: Optional[Path] = None

class Log:
    """ a singleton class for logging custom objects like pandas dataframes, numpy arrays, images to html file """
    _instance = None
    _loglist: List[Tuple[Loggable, Optional[LogContext]]]
    _lock = Lock()

    def __new__(cls, *args, **kwargs):
        with cls._lock:
            if not cls._instance:
                manager = Manager()
                cls._instance = super(Log, cls).__new__(cls)
                cls._instance._loglist = manager.list()
        return cls._instance

    def _log(self, obj: Loggable, context: Optional[LogContext] = None):
        with self._lock:
            self._loglist.append((obj, context))

    def log(self, obj: Loggable, task: Optional["Task"] = None):

        if isinstance(obj, Path):
            self.add_file(obj, task=task)
        else:
            context = LogContext(task=task)
            self._log(obj, context=context)
    
    def add_file(self, path: Union[str, Path], task: Optional["Task"] = None):
        # depending on the file extension, we can load the file as a pandas dataframe or an image, markdown, etc.
        ext = Path(path).suffix.lower()
        context = LogContext(task=task, from_file=Path(path))

        print("context for logging to file: ", context)

        if ext in [".csv", ".xlsx"]:
            self._log(pd.read_csv(path), context=context)
        if ext in ['parquet']:
            self._log(pd.read_parquet(path), context=context)
        elif ext in [".jpg", ".jpeg", ".png"]:
            self._log(Image.open(path), context=context)
        elif ext in [".md"]:
            with open(path, "r") as f:
                self._log(Markdown(f.read()), context=context)
        elif ext in [".npy", "np"]:
            self._log(np.load(path), context=context)
        else:
            logger.warning(f"file extension {ext} unknwon for {path}. reverting to plain text")
            with open(path, "r") as f:
                self._log(f.read(), context=context)

    def save(self, filename):
        html_elements = []
        logger.debug(f"writing html result to {filename}")
        # lets group log elements by task
        
        # create groups
        groups = {}
        group_order = []

        for (obj, context) in self._loglist:
            if context.task not in groups:
                groups[context.task] = []

            groups[context.task].append((obj, context))

            if context not in group_order:
                group_order.append(context.task)

        with open(filename, "w") as f:
            for task in group_order:
                if task is not None:
                    html_elements.append(f"<h1>{str(task)}</h1>")

                for (obj, context) in groups[task]:
                    if isinstance(obj, pd.DataFrame):
                        elem = obj.to_html()
                    elif isinstance(obj, np.ndarray):
                        elem = obj.__repr__()
                    elif isinstance(obj, Image.Image):
                        # save image as html (base64 encoded)
                        elem = image2html(obj)
                    elif isinstance(obj, Markdown):
                        elem = str(obj)
                    elif isinstance(obj, datetime.date) or isinstance(obj, datetime.datetime):
                        elem = f"<p>{obj.isoformat()}</p>"
                    else:
                        elem = str(obj)

                    underline = context.ts.isoformat()
                    if context.from_file:
                        underline += f" from file {context.from_file}"

                    elem_div_with_ts = f"""
                    <div>
                    {elem}
                    <p style="margin-top: 10px; font-style: italic">{underline}</p>
                    </div>
                    """
                    html_elements.append(elem_div_with_ts)

            # create a complete html file with all the elements with are arranged vertically using flex box with padding
            f.write(f"""
            <html>
            <head>
            <style>
            .container {{
                display: flex;
                flex-direction: column;
                padding: 20px;
                gap: 20px;
            }}
            </style>
            </head>
            <body>
            <div class="container">
            {''.join(html_elements)}
            </div>
            </body>
            </html>
            """)
