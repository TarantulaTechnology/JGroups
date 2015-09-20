package org.jgroups.protocols.aws.s3;

import java.util.Date;

public class Bucket {
    /**
     * The name of the bucket.
     */
    public String name;

    /**
     * The bucket's creation date.
     */
    public Date creationDate;

    public Bucket() {
        this.name=null;
        this.creationDate=null;
    }

    public Bucket(String name, Date creationDate) {
        this.name=name;
        this.creationDate=creationDate;
    }

    public String toString() {
        return this.name;
    }
}