package org.jgroups.protocols.aws.s3;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;


public abstract class CallingFormat {

    protected static CallingFormat pathCallingFormat=new PathCallingFormat();
    protected static CallingFormat subdomainCallingFormat=new SubdomainCallingFormat();
    protected static CallingFormat vanityCallingFormat=new VanityCallingFormat();

    public abstract boolean supportsLocatedBuckets();

    public abstract String getEndpoint(String server, int port, String bucket);

    public abstract String getPathBase(String bucket, String key);

    public abstract URL getURL(boolean isSecure, String server, int port, String bucket, String key, Map pathArgs)
            throws MalformedURLException;

    public static CallingFormat getPathCallingFormat() {
        return pathCallingFormat;
    }

    public static CallingFormat getSubdomainCallingFormat() {
        return subdomainCallingFormat;
    }

    public static CallingFormat getVanityCallingFormat() {
        return vanityCallingFormat;
    }

    private static class PathCallingFormat extends CallingFormat {
        public boolean supportsLocatedBuckets() {
            return false;
        }

        public String getPathBase(String bucket, String key) {
            return isBucketSpecified(bucket)? "/" + bucket + "/" + key : "/";
        }

        public String getEndpoint(String server, int port, String bucket) {
            return server + ":" + port;
        }

        public URL getURL(boolean isSecure, String server, int port, String bucket, String key, Map pathArgs)
                throws MalformedURLException {
            String pathBase=isBucketSpecified(bucket)? "/" + bucket + "/" + key : "/";
            String pathArguments=Utils.convertPathArgsHashToString(pathArgs);
            return new URL(isSecure? "https" : "http", server, port, pathBase + pathArguments);
        }

        private static boolean isBucketSpecified(String bucket) {
            return bucket != null && bucket.length() != 0;
        }
    }

    private static class SubdomainCallingFormat extends CallingFormat {
        public boolean supportsLocatedBuckets() {
            return true;
        }

        public String getServer(String server, String bucket) {
            return bucket + "." + server;
        }

        public String getEndpoint(String server, int port, String bucket) {
            return getServer(server, bucket) + ":" + port;
        }

        public String getPathBase(String bucket, String key) {
            return "/" + key;
        }

        public URL getURL(boolean isSecure, String server, int port, String bucket, String key, Map pathArgs)
                throws MalformedURLException {
            if(bucket == null || bucket.length() == 0) {
                //The bucket is null, this is listAllBuckets request
                String pathArguments=Utils.convertPathArgsHashToString(pathArgs);
                return new URL(isSecure? "https" : "http", server, port, "/" + pathArguments);
            }
            else {
                String serverToUse=getServer(server, bucket);
                String pathBase=getPathBase(bucket, key);
                String pathArguments=Utils.convertPathArgsHashToString(pathArgs);
                return new URL(isSecure? "https" : "http", serverToUse, port, pathBase + pathArguments);
            }
        }
    }

    private static class VanityCallingFormat extends SubdomainCallingFormat {
        public String getServer(String server, String bucket) {
            return bucket;
        }
    }
}

