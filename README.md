| :boom: ALERT!!             |
|:---------------------------|
| ![status: inactive](https://img.shields.io/badge/status-inactive-red.svg) [![unstable](http://badges.github.io/stability-badges/dist/unstable.svg)](http://github.com/badges/stability-badges) This project is no longer actively developed or maintained. |

**TL;DR:** To store files from
[App Engine](https://cloud.google.com/appengine) to [Cloud Storage](https://cloud.google.com/storage), please use the [Cloud Storage client libraries](https://cloud.google.com/storage/docs/reference/libraries) available in different languages. Each one will have its own open source repo and documentation. Those libraries can connect your app to Cloud Storage, usable outside of App Engine and any other Google Cloud compute platform.


# Google App Engine custom Cloud Storage client libraries

The libraries in this repository replaced the original [App Engine Files API deprecated in Jun 2013](https://cloudplatform.googleblog.com/2013/06/google-app-engine-181-released.html) and represented the original client libraries for Cloud Storage created just for App Engine users. Since then, the Google Cloud team [launched the Cloud Storage client libraries in Dec 2016](https://cloud.google.com/blog/products/gcp/announcing-new-google-cloud-client) (linked above) which obsolete these libraries, which are no longer recommended. For archival purposes, here is a link to the documentation for the [Python version](https://cloud.google.com/appengine/docs/standard/python/googlecloudstorageclient/setting-up-cloud-storage#downloading_the_client_library) of this library. It will be removed at some point in the near future. (The Java documentation was already removed in 2021.)
