"""Client Library for Google Cloud Storage."""




from cloudstorage_api import *
from errors import *
from storage_api import *


try:
  from google.appengine.ext.cloudstorage.common import CSFileStat
  from google.appengine.ext.cloudstorage.common import GCSFileStat
except ImportError:
  from ..common import CSFileStat
  from ..common import GCSFileStat
