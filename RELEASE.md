RELEASE_TYPE: minor

-   Remove the codec, decoder and encoder for `java.io.InputStream`.

-   The decoder is more relaxed about the sort of InputStream it takes as input.
    You get reduced error checking if you don't pass a length, but that's all.

-   Add the initial `Store` and `StreamingStore` implementations.

-   Separate our the notion of `HasLength` and `HasMetadata` for instances of InputStream.