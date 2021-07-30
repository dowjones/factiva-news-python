
"""
    Init file for news module.
"""
__all__ = ['snapshot', 'stream', 'bulknews', 'taxonomy',
    'Snapshot', 'SnapshotQuery', 'SnapshotFiles', 'Stream',
    'Listener', 'Subscription', 'Taxonomy', 'SnapshotFiles']

from .__version__ import __version__
from .snapshot import (Snapshot, SnapshotQuery, SnapshotFiles)
from .stream import (Stream, Listener, Subscription)
from .taxonomy.taxonomy import Taxonomy
from factiva.core import SnapshotFiles

version = __version__
