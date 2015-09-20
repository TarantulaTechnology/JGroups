package org.jgroups.protocols.aws.s3;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.List;

import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

public class ListBucketResponse extends Response {

    /**
     * The name of the bucket being listed.  Null if request fails.
     */
    public String name=null;

    /**
     * The prefix echoed back from the request.  Null if request fails.
     */
    public String prefix=null;

    /**
     * The marker echoed back from the request.  Null if request fails.
     */
    public String marker=null;

    /**
     * The delimiter echoed back from the request.  Null if not specified in
     * the request, or if it fails.
     */
    public String delimiter=null;

    /**
     * The maxKeys echoed back from the request if specified.  0 if request fails.
     */
    public int maxKeys=0;

    /**
     * Indicates if there are more results to the list.  True if the current
     * list results have been truncated.  false if request fails.
     */
    public boolean isTruncated=false;

    /**
     * Indicates what to use as a marker for subsequent list requests in the event
     * that the results are truncated.  Present only when a delimiter is specified.
     * Null if request fails.
     */
    public String nextMarker=null;

    /**
     * A List of ListEntry objects representing the objects in the given bucket.
     * Null if the request fails.
     */
    public List entries=null;

    /**
     * A List of CommonPrefixEntry objects representing the common prefixes of the
     * keys that matched up to the delimiter.  Null if the request fails.
     */
    public List commonPrefixEntries=null;

    public ListBucketResponse(HttpURLConnection connection) throws IOException {
        super(connection);
        if(connection.getResponseCode() < 400) {
            try {
                XMLReader xr=Utils.createXMLReader();
                ListBucketHandler handler=new ListBucketHandler();
                xr.setContentHandler(handler);
                xr.setErrorHandler(handler);

                xr.parse(new InputSource(connection.getInputStream()));

                this.name=handler.getName();
                this.prefix=handler.getPrefix();
                this.marker=handler.getMarker();
                this.delimiter=handler.getDelimiter();
                this.maxKeys=handler.getMaxKeys();
                this.isTruncated=handler.getIsTruncated();
                this.nextMarker=handler.getNextMarker();
                this.entries=handler.getKeyEntries();
                this.commonPrefixEntries=handler.getCommonPrefixEntries();

            }
            catch(SAXException e) {
                throw new RuntimeException("Unexpected error parsing ListBucket xml", e);
            }
        }
    }

}