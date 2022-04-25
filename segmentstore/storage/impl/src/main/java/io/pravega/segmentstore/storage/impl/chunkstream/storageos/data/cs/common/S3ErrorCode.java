package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common;

// https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html#ErrorCodeList
public enum S3ErrorCode {
    BucketAlreadyExists(409),
    EntityTooLarge(400),
    InternalError(500),
    InvalidArgument(400),
    InvalidBucketName(400),
    InvalidRange(416),
    InvalidRequest(400),
    MethodNotAllowed(405),
    MissingContentLength(411),
    NoSuchBucket(404),
    NoSuchKey(404),
    NotImplemented(501),
    OperationAborted(409),
    RequestTimeout(400),
    BadDigest(400),
    ;

    private final int httpCode;

    S3ErrorCode(int httpCode) {
        this.httpCode = httpCode;
    }

    public int httpCode() {
        return this.httpCode;
    }
}
