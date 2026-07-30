"""
worker sub-package: executor and loop layers.
"""

import signal

from . import executor
from . import loop

from .executor import execute_job
from .loop import worker_main_loop
