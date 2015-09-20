package org.jgroups.protocols.aws.s3;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;


public class GetResponse extends Response {
    public S3Object object;

    /**
     * Pulls a representation of an S3Object out of the HttpURLConnection response.
     */
    public GetResponse(HttpURLConnection connection) throws IOException {
        super(connection);
        if(connection.getResponseCode() < 400) {
            Map metadata=extractMetadata(connection);
            byte[] body=slurpInputStream(connection.getInputStream());
            this.object=new S3Object(body, metadata);
        }
    }

    /**
     * Examines the response's header fields and returns a Map from String to List of Strings
     * representing the object's metadata.
     */
    private static Map extractMetadata(HttpURLConnection connection) {
        TreeMap metadata=new TreeMap();
        Map headers=connection.getHeaderFields();
        for(Iterator i=headers.keySet().iterator(); i.hasNext();) {
            String key=(String)i.next();
            if(key == null) continue;
            if(key.startsWith(Utils.METADATA_PREFIX)) {
                metadata.put(key.substring(Utils.METADATA_PREFIX.length()), headers.get(key));
            }
        }

        return metadata;
    }

    /**
     * Read the input stream and dump it all into a big byte array
     */
    public static byte[] slurpInputStream(InputStream stream) throws IOException {
        final int chunkSize=2048;
        byte[] buf=new byte[chunkSize];
        ByteArrayOutputStream byteStream=new ByteArrayOutputStream(chunkSize);
        int count;

        while((count=stream.read(buf)) != -1) byteStream.write(buf, 0, count);

        return byteStream.toByteArray();
    }
}

