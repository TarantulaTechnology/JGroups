package org.jgroups.protocols.aws.s3;

import java.io.IOException;
import java.net.HttpURLConnection;

import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

public class LocationResponse extends Response {
    String location;

    /**
     * Parse the response to a ?location query.
     */
    public LocationResponse(HttpURLConnection connection) throws IOException {
        super(connection);
        if(connection.getResponseCode() < 400) {
            try {
                XMLReader xr=Utils.createXMLReader();
                ;
                LocationResponseHandler handler=new LocationResponseHandler();
                xr.setContentHandler(handler);
                xr.setErrorHandler(handler);

                xr.parse(new InputSource(connection.getInputStream()));
                this.location=handler.loc;
            }
            catch(SAXException e) {
                throw new RuntimeException("Unexpected error parsing ListAllMyBuckets xml", e);
            }
        }
        else {
            this.location="<error>";
        }
    }

    /**
     * Report the location-constraint for a bucket.
     * A value of null indicates an error;
     * the empty string indicates no constraint;
     * and any other value is an actual location constraint value.
     */
    public String getLocation() {
        return location;
    }

    /**
     * Helper class to parse LocationConstraint response XML
     */
    static class LocationResponseHandler extends DefaultHandler {
        String loc=null;
        private StringBuffer currText=null;

        public void startDocument() {
        }

        public void startElement(String uri, String name, String qName, Attributes attrs) {
            if(name.equals("LocationConstraint")) {
                this.currText=new StringBuffer();
            }
        }

        public void endElement(String uri, String name, String qName) {
            if(name.equals("LocationConstraint")) {
                loc=this.currText.toString();
                this.currText=null;
            }
        }

        public void characters(char ch[], int start, int length) {
            if(currText != null)
                this.currText.append(ch, start, length);
        }
    }
}
