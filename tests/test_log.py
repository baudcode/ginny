from datetime import date, datetime

import numpy as np
import pandas as pd
from PIL import Image

from src.log import Log, Markdown


def test_all_options():
    log = Log()
    log.log(pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}))
    log.log(np.array([[1, 2, 3], [4, 5, 6]]))
    log.log(Image.new("RGB", (100, 100), (255, 0, 0)))
    log.log(Markdown("# Hello World"))
    log.log(date.today())
    log.log(datetime.now())
    log.save("test.html")



def test_with_multiple_tasks_as_context():
    log = Log()
    log.log(pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}), task=1)
    log.log(np.array([[1, 2, 3], [4, 5, 6]]), task=2)
    log.log(Image.new("RGB", (100, 100), (255, 0, 0)), task=3)
    log.log(Markdown("# Hello World"), task=4)
    log.log(date.today(), task=5)
    log.log(datetime.now(), task=6)
    log.save("test.html")