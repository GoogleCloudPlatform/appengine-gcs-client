"""Client Library for Google Cloud Storage."""




from cloudstorage_api import *
from errors import *
from storage_api import *


try:
  from .common import CSFileStat
  from .common import GCSFileStat
  from .common import validate_bucket_name
  from .common import validate_bucket_path
  from .common import validate_file_path
except ImportError:
  from google.appengine.ext.cloudstorage.common import CSFileStat
  from google.appengine.ext.cloudstorage.common import GCSFileStat
  from google.appengine.ext.cloudstorage.common import validate_bucket_name
  from google.appengine.ext.cloudstorage.common import validate_bucket_path
  from google.appengine.ext.cloudstorage.common import validate_file_path
