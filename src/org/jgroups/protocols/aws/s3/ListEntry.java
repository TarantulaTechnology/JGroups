package org.jgroups.protocols.aws.s3;

import java.util.Date;


public class ListEntry {
    /**
     * The name of the object
     */
    public String key;

    /**
     * The date at which the object was last modified.
     */
    public Date lastModified;

    /**
     * The object's ETag, which can be used for conditional GETs.
     */
    public String eTag;

    /**
     * The size of the object in bytes.
     */
    public long size;

    /**
     * The object's storage class
     */
    public String storageClass;

    /**
     * The object's owner
     */
    public Owner owner;

    public String toString() {
        return key;
    }
}
