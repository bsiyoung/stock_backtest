import os
from datamgr.datamgr import DataManager


class BackTest:
    def __init__(self, root_dir='.'):
        self.root = root_dir
        self.dm = DataManager(root_dir=os.path.join(self.root, 'datamgr'))