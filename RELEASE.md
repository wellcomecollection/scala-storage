RELEASE_TYPE: patch

Fix a bug in S3Transfer where we weren't closing streams correctly, and the SDK would warn that you hadn't read all the bytes from an S3AbortableInputStream.