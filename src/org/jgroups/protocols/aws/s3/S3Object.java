package org.jgroups.protocols.aws.s3;

import java.util.Map;

public class S3Object {

    public byte[] data;

    /**
     * A Map from String to List of Strings representing the object's metadata
     */
    public Map metadata;

    public S3Object(byte[] data, Map metadata) {
        this.data=data;
        this.metadata=metadata;
    }
}
