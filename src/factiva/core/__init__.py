"""
    Implement core capabilities for classes.
"""
__all__ = [
    'const', 'dicts', 'auth', 'UserKey', 'APIKeyUser', 'StreamUser',
    'StreamResponse', 'SnapshotFiles', 'factiva_logger', 'get_factiva_logger'
]

from .__version__ import __version__
from .apikeyuser import APIKeyUser
from .auth.userkey import UserKey
from .stream_response import StreamResponse
from .streamuser import StreamUser
from .tools.files import SnapshotFiles
from .log import factiva_logger, get_factiva_logger

version = __version__
