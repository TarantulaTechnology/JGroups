package org.jgroups.protocols.aws.s3;

import java.net.MalformedURLException;
import java.net.URL;
/**
 * Utility class to parse S3 pre-signed URLs
 */
public class PreSignedUrlParser {
    String bucket = "";
    String prefix = "";

    public PreSignedUrlParser(String preSignedUrl) {
        try {
            URL url = new URL(preSignedUrl);
            this.bucket = parseBucketFromHost(url.getHost());
            String path = url.getPath();
            String[] pathParts = path.split("/");
            
            if (pathParts.length < 2) {
                throw new IllegalArgumentException("pre-signed url " + preSignedUrl + " must point to a file within a bucket");
            }
            if (pathParts.length > 3) {
                throw new IllegalArgumentException("pre-signed url " + preSignedUrl + " may only have only subdirectory under a bucket");
            }
            if (pathParts.length > 2) {
                this.prefix = pathParts[1];
            }
        } catch (MalformedURLException ex) {
            throw new IllegalArgumentException("pre-signed url " + preSignedUrl + " is not a valid url");
        }
    }

    private String parseBucketFromHost(String host) {
        int s3Index = host.lastIndexOf(".s3.");
        if (s3Index > 0) {
            host = host.substring(0, s3Index);
        }
        return host;
    }

    public String getBucket() {
        return bucket;
    }
    
    public String getPrefix() {
        return prefix;
    }
}

