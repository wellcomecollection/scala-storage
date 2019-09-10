RELEASE_TYPE: patch

Try catching the "Read timed out" exception inside S3StreamStore again, and log errors we don't recognise.