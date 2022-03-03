"""
    Init file for news module.
"""
__all__ = [
    'snapshot', 'stream', 'bulknews', 'taxonomy', 'Snapshot', 'SnapshotQuery',
    'SnapshotFiles', 'Stream', 'Listener', 'Subscription', 'Taxonomy',
    'SnapshotFiles', 'ListenerTools','factiva_logger', 'get_factiva_logger'
]

from factiva.core import SnapshotFiles, factiva_logger, get_factiva_logger

from .__version__ import __version__
from .snapshot import Snapshot, SnapshotFiles, SnapshotQuery
from .stream import Listener, Stream, Subscription
from .taxonomy.taxonomy import Taxonomy
from .tools import ListenerTools

version = __version__
