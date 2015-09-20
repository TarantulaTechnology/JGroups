package org.jgroups.protocols.aws.s3;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.SimpleTimeZone;

import org.xml.sax.Attributes;
import org.xml.sax.helpers.DefaultHandler;

public class ListBucketHandler extends DefaultHandler {

    private String name=null;
    private String prefix=null;
    private String marker=null;
    private String delimiter=null;
    private int maxKeys=0;
    private boolean isTruncated=false;
    private String nextMarker=null;
    private boolean isEchoedPrefix=false;
    private List keyEntries=null;
    private ListEntry keyEntry=null;
    private List commonPrefixEntries=null;
    private CommonPrefixEntry commonPrefixEntry=null;
    private StringBuffer currText=null;
    private SimpleDateFormat iso8601Parser=null;

    public ListBucketHandler() {
        super();
        keyEntries=new ArrayList();
        commonPrefixEntries=new ArrayList();
        this.iso8601Parser=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        this.iso8601Parser.setTimeZone(new SimpleTimeZone(0, "GMT"));
        this.currText=new StringBuffer();
    }

    public void startDocument() {
        this.isEchoedPrefix=true;
    }

    public void endDocument() {
        // ignore
    }

    public void startElement(String uri, String name, String qName, Attributes attrs) {
        if(name.equals("Contents")) {
            this.keyEntry=new ListEntry();
        }
        else if(name.equals("Owner")) {
            this.keyEntry.owner=new Owner();
        }
        else if(name.equals("CommonPrefixes")) {
            this.commonPrefixEntry=new CommonPrefixEntry();
        }
    }

    public void endElement(String uri, String name, String qName) {
        if(name.equals("Name")) {
            this.name=this.currText.toString();
        }
        // this prefix is the one we echo back from the request
        else if(name.equals("Prefix") && this.isEchoedPrefix) {
            this.prefix=this.currText.toString();
            this.isEchoedPrefix=false;
        }
        else if(name.equals("Marker")) {
            this.marker=this.currText.toString();
        }
        else if(name.equals("MaxKeys")) {
            this.maxKeys=Integer.parseInt(this.currText.toString());
        }
        else if(name.equals("Delimiter")) {
            this.delimiter=this.currText.toString();
        }
        else if(name.equals("IsTruncated")) {
            this.isTruncated=Boolean.valueOf(this.currText.toString());
        }
        else if(name.equals("NextMarker")) {
            this.nextMarker=this.currText.toString();
        }
        else if(name.equals("Contents")) {
            this.keyEntries.add(this.keyEntry);
        }
        else if(name.equals("Key")) {
            this.keyEntry.key=this.currText.toString();
        }
        else if(name.equals("LastModified")) {
            try {
                this.keyEntry.lastModified=this.iso8601Parser.parse(this.currText.toString());
            }
            catch(ParseException e) {
                throw new RuntimeException("Unexpected date format in list bucket output", e);
            }
        }
        else if(name.equals("ETag")) {
            this.keyEntry.eTag=this.currText.toString();
        }
        else if(name.equals("Size")) {
            this.keyEntry.size=Long.parseLong(this.currText.toString());
        }
        else if(name.equals("StorageClass")) {
            this.keyEntry.storageClass=this.currText.toString();
        }
        else if(name.equals("ID")) {
            this.keyEntry.owner.id=this.currText.toString();
        }
        else if(name.equals("DisplayName")) {
            this.keyEntry.owner.displayName=this.currText.toString();
        }
        else if(name.equals("CommonPrefixes")) {
            this.commonPrefixEntries.add(this.commonPrefixEntry);
        }
        // this is the common prefix for keys that match up to the delimiter
        else if(name.equals("Prefix")) {
            this.commonPrefixEntry.prefix=this.currText.toString();
        }
        if(this.currText.length() != 0)
            this.currText=new StringBuffer();
    }

    public void characters(char ch[], int start, int length) {
        this.currText.append(ch, start, length);
    }

    public String getName() {
        return this.name;
    }

    public String getPrefix() {
        return this.prefix;
    }

    public String getMarker() {
        return this.marker;
    }

    public String getDelimiter() {
        return this.delimiter;
    }

    public int getMaxKeys() {
        return this.maxKeys;
    }

    public boolean getIsTruncated() {
        return this.isTruncated;
    }

    public String getNextMarker() {
        return this.nextMarker;
    }

    public List getKeyEntries() {
        return this.keyEntries;
    }

    public List getCommonPrefixEntries() {
        return this.commonPrefixEntries;
    }
}
