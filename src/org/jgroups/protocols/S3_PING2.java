package org.jgroups.protocols;

import org.jgroups.protocols.aws.s3.Bucket;
import org.jgroups.protocols.aws.s3.GetResponse;
import org.jgroups.protocols.aws.s3.ListAllMyBucketsResponse;
import org.jgroups.protocols.aws.s3.ListBucketResponse;
import org.jgroups.protocols.aws.s3.ListEntry;
import org.jgroups.protocols.aws.s3.PreSignedUrlParser;
import org.jgroups.protocols.aws.s3.Response;
import org.jgroups.protocols.aws.s3.S3Object;
import org.jgroups.protocols.aws.s3.Utils;
import org.jgroups.protocols.aws.s3.signer.AWSAuthConnection;

import org.jgroups.Address;
import org.jgroups.annotations.Property;
import org.jgroups.util.Responses;

import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.helpers.XMLReaderFactory;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import static java.lang.String.valueOf;


/**
 * Discovery protocol using Amazon's S3 storage. 
 * The S3 access code reuses the example shipped by Amazon.
 * @author Bela Ban
 */
public class S3_PING2 extends FILE_PING {

    /////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////

    @Property(description="The name of the AWS server")
    protected String host;

    @Property(description="The port at which AWS is listening")
    protected int port;

    @Property(description="Whether or not to use SSL to connect to host:port")
    protected boolean use_ssl=true;

    @Property(description="The access key to AWS (S3)",exposeAsManagedAttribute=false)
    protected String access_key;

    @Property(description="The secret access key to AWS (S3)",exposeAsManagedAttribute=false)
    protected String secret_access_key;

    @Property(description="When non-null, we set location to prefix-UUID")
    protected String prefix;

    @Property(description="When non-null, we use this pre-signed URL for PUTs",exposeAsManagedAttribute=false)
    protected String pre_signed_put_url;

    @Property(description="When non-null, we use this pre-signed URL for DELETEs",exposeAsManagedAttribute=false)
    protected String pre_signed_delete_url;

    @Property(description="Skip the code which checks if a bucket exists in initialization")
    protected boolean skip_bucket_existence_check=false;

    /////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////
    
 
    protected AWSAuthConnection conn = null;
    
    
    /////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////
    
    
    @Override
    /////////////////////////////////////////////////////////////////////////////////
    public void init() throws Exception 
    /////////////////////////////////////////////////////////////////////////////////
    {
        super.init();
        
        if( host == null )
        {
            host = Utils.DEFAULT_HOST;
        }
        
        validateProperties();
        
        conn = createAWSConnection();

        if( prefix != null && !prefix.isEmpty() ) 
        {
            ListAllMyBucketsResponse bucket_list = conn.listAllMyBuckets(null);
            
            List buckets = bucket_list.entries;
            
            if( buckets != null ) 
            {
                boolean found = false;
                
                for( Object tmp: buckets ) 
                {
                    if( tmp instanceof Bucket ) 
                    {
                        Bucket bucket = (Bucket)tmp;
                        
                        if( bucket.name.startsWith( prefix ) ) 
                        {
                            location = bucket.name;
                            found = true;
                        }
                    }
                }
                
                if( !found ) 
                {
                    location = prefix + "-" + java.util.UUID.randomUUID().toString();
                }
            }
        }

        if( usingPreSignedUrls() ) 
        {
            PreSignedUrlParser parsedPut = new PreSignedUrlParser( pre_signed_put_url );
            location = parsedPut.getBucket();
        }

        if( !skip_bucket_existence_check && !conn.checkBucketExists(location) ) 
        {
            Response response = conn.createBucket( location, AWSAuthConnection.LOCATION_DEFAULT, null );
            String string_response = response.connection.getResponseMessage();
        }
    }
        
    /////////////////////////////////////////////////////////////////////    
    protected AWSAuthConnection createAWSConnection() 
    /////////////////////////////////////////////////////////////////////    
    {
        return port > 0 ? 
        		new AWSAuthConnection( access_key, secret_access_key, use_ssl, host, port)
        		: 
        		new AWSAuthConnection( access_key, secret_access_key, use_ssl, host);
    }
    

    @Override
    /////////////////////////////////////////////////////////////////////    
    protected void createRootDir() 
    /////////////////////////////////////////////////////////////////////    
    {
        ; // do *not* create root file system (don't remove !)
    }

    @Override
    /////////////////////////////////////////////////////////////////////    
    protected void readAll( List<Address> members, String clustername, Responses responses ) 
    /////////////////////////////////////////////////////////////////////    
    {
        if( clustername == null )
        {
            return;
        }

        try 
        {
            if ( usingPreSignedUrls() ) 
            {
                PreSignedUrlParser parsedPut = new PreSignedUrlParser( pre_signed_put_url );
                clustername = parsedPut.getPrefix();
            }

            clustername=sanitize( clustername );
            
            ListBucketResponse rsp = conn.listBucket( location, clustername, null, null, null );
            
            if( rsp.entries != null ) 
            {
                for( Iterator<ListEntry> it = rsp.entries.iterator(); it.hasNext(); ) 
                {
                    ListEntry key = it.next();
                    
                    try 
                    {
                        GetResponse val = conn.get( location, key.key, null );
                        readResponse( val, members, responses );
                    }
                    catch( Throwable t ) 
                    {
                        log.error( "failed reading key %s: %s", key.key, t );
                    }
                }
            }
        }
        catch( IOException ex ) 
        {
            log.error( "failed reading addresses", ex );
        }
    }


    /////////////////////////////////////////////////////////////////////    
    protected void readResponse( GetResponse rsp, List<Address> mbrs, Responses responses ) 
    /////////////////////////////////////////////////////////////////////    
    {
        if(rsp.object == null)
        {
            return;
        }
        
        byte[] buf = rsp.object.data;
        
        List<PingData> list;
        
        if( buf != null && buf.length > 0 ) 
        {
            try 
            {
                list = read( new ByteArrayInputStream( buf ) );
                
                if( list != null ) 
                {
                    for( PingData data : list ) 
                    {
                        if( mbrs == null || mbrs.contains( data.getAddress() ) )
                        {
                            responses.addResponse( data, data.isCoord() );
                        }
                        
                        if( local_addr != null && !local_addr.equals( data.getAddress() ) )
                        {
                            addDiscoveryResponseToCaches( 
                            		data.getAddress(), data.getLogicalName(), data.getPhysicalAddr() );
                        }
                    }
                }
            }
            catch( Throwable e ) 
            {
                log.error( "failed unmarshalling response", e );
            }
        }
    }



    @Override
    /////////////////////////////////////////////////////////////////////    
    protected void write( List<PingData> list, String clustername ) 
    /////////////////////////////////////////////////////////////////////    
    {
        String filename = addressToFilename( local_addr );
        
        String key = sanitize( clustername ) + "/" + sanitize( filename );
        
        HttpURLConnection httpConn = null;
        
        try 
        {
            ByteArrayOutputStream out = new ByteArrayOutputStream( 4096 );
            
            write( list, out );
            
            byte[] data = out.toByteArray();
            
            S3Object val = new S3Object( data, null );

            if ( usingPreSignedUrls() ) 
            {
                Map headers = new TreeMap();
                
                headers.put( "x-amz-acl", Arrays.asList( "public-read" ) );
                
                httpConn = conn.put( pre_signed_put_url, val, headers ).connection;
                
            } 
            else 
            {
                Map headers = new TreeMap();
                
                headers.put( "Content-Type", Arrays.asList( "text/plain" ) );
                
                httpConn = conn.put( location, key, val, headers ).connection;
                
            }
            
            if( !httpConn.getResponseMessage().equals("OK") ) 
            {
                log.error( "Failed to write file to S3 bucket - HTTP Response code: (" + httpConn.getResponseCode() + ")");
            }
        } 
        catch ( Exception e ) 
        {
            log.error( "Error marshalling object", e );
        }
    }


    /////////////////////////////////////////////////////////////////////    
    protected void remove( String clustername, Address addr ) 
    /////////////////////////////////////////////////////////////////////    
    {
        if( clustername == null || addr == null )
        {
            return;
        }
        
        String filename = addressToFilename( addr );
        
        String key=sanitize( clustername ) + "/" + sanitize( filename );
        
        try 
        {
            Map headers = new TreeMap();
            
            headers.put( "Content-Type", Arrays.asList( "text/plain" ) );
            
            if ( usingPreSignedUrls() ) 
            {
                conn.delete( pre_signed_delete_url ).connection.getResponseMessage();
            } 
            else 
            {
                conn.delete( location, key, headers ).connection.getResponseMessage();
            }
            
            if( log.isTraceEnabled() )
            {
                log.trace( "removing " + location + "/" + key );
            }
        }
        catch( Exception e ) 
        {
            log.error( "failure removing data", e );
        }
    }

    @Override
    /////////////////////////////////////////////////////////////////////    
    protected void removeAll(String clustername) 
    /////////////////////////////////////////////////////////////////////    
    {
        if(clustername == null)
        {
            return;
        }

        try 
        {
            Map headers = new TreeMap();
            
            headers.put( "Content-Type", Arrays.asList("text/plain" ) );
            
            clustername=sanitize( clustername );
            
            ListBucketResponse rsp = conn.listBucket( location, clustername, null, null, null );
            
            if( rsp.entries != null) 
            {
                for( Iterator<ListEntry> it = rsp.entries.iterator(); it.hasNext(); ) 
                {
                    ListEntry key = it.next();
                    
                    try 
                    {
                        if ( usingPreSignedUrls() )
                        {
                            conn.delete( pre_signed_delete_url ).connection.getResponseMessage();
                        }
                        else
                        {
                            conn.delete( location, key.key, headers ).connection.getResponseMessage();
                        }
                        
                        log.trace( "removing %s/%s", location, key.key );
                    }
                    catch( Throwable t ) 
                    {
                        log.error( "failed deleting object %s/%s: %s", location, key.key, t );
                    }
                }
            }
        }
        catch( IOException ex ) 
        {
            log.error( "failed deleting all objects", ex );
        }
    }

    /////////////////////////////////////////////////////////////////////    
    protected void validateProperties() 
    /////////////////////////////////////////////////////////////////////    
    {
        if ( pre_signed_put_url != null && pre_signed_delete_url != null ) 
        {
            PreSignedUrlParser parsedPut = new PreSignedUrlParser( pre_signed_put_url );
            
            PreSignedUrlParser parsedDelete = new PreSignedUrlParser( pre_signed_delete_url );
            
            if (!parsedPut.getBucket().equals( parsedDelete.getBucket() ) 
            		||
                !parsedPut.getPrefix().equals( parsedDelete.getPrefix() ) ) 
            {
                throw new IllegalArgumentException(
                		"pre_signed_put_url and pre_signed_delete_url must have the same path"
                		);
            }
        } 
        else if ( pre_signed_put_url != null || pre_signed_delete_url != null ) 
        {
            throw new IllegalArgumentException( 
            		"pre_signed_put_url and pre_signed_delete_url must both be set or both unset"
            		);
        }
        
        if ( prefix != null && location != null ) 
        {
            throw new IllegalArgumentException( 
            		"set either prefix or location, but not both" 
            		);
        }
        
        if (prefix != null && ( access_key == null || secret_access_key == null ) ) 
        {
            throw new IllegalArgumentException( 
            		"access_key and secret_access_key must be set when setting prefix" 
            		);
        }
    }
    
    /////////////////////////////////////////////////////////////////////    
    protected boolean usingPreSignedUrls() 
    /////////////////////////////////////////////////////////////////////
    {
        return pre_signed_put_url != null;
    }


    /** Sanitizes bucket and folder names according to AWS guidelines */
    /////////////////////////////////////////////////////////////////////
    protected static String sanitize( final String name ) 
    /////////////////////////////////////////////////////////////////////
    {
        String retval = name;
        
        retval = retval.replace( '/', '-' );
        
        retval = retval.replace( '\\', '-' );
        
        return retval;
    }


    /**
     * Use this helper method to generate pre-signed S3 urls for use with S3_PING.
     * You'll need to generate urls for both the put and delete http methods.
     * Example:
     * Your AWS Access Key is "abcd".
     * Your AWS Secret Access Key is "efgh".
     * You want this node to write its information to "/S3_PING/DemoCluster/node1".
     * So, your bucket is "S3_PING" and your key is "DemoCluster/node1".
     * You want this to expire one year from now, or
     *   (System.currentTimeMillis / 1000) + (60 * 60 * 24 * 365)
     *   Let's assume that this equals 1316286684
     * 
     * Here's how to generate the value for the pre_signed_put_url property:
     * String putUrl = S3_PING.generatePreSignedUrl("abcd", "efgh", "put",
     *                                              "S3_Ping", "DemoCluster/node1",
     *                                              1316286684);
     *                                              
     * Here's how to generate the value for the pre_signed_delete_url property:
     * String deleteUrl = S3_PING.generatePreSignedUrl("abcd", "efgh", "delete",
     *                                                 "S3_Ping", "DemoCluster/node1",
     *                                                 1316286684);
     * 
     * @param awsAccessKey Your AWS Access Key
     * @param awsSecretAccessKey Your AWS Secret Access Key
     * @param method The HTTP method - use "put" or "delete" for use with S3_PING
     * @param bucket The S3 bucket you want to write to
     * @param key The key within the bucket to write to
     * @param expirationDate The date this pre-signed url should expire, in seconds since epoch
     * @return The pre-signed url to be used in pre_signed_put_url or pre_signed_delete_url properties
     */
    public static String generatePreSignedUrl( 
    		String awsAccessKey
    		, String awsSecretAccessKey
    		, String method
    		, String bucket
    		, String key
    		, long expirationDate
    		) 
    {
        Map headers = new HashMap();
        
        if ( method.equalsIgnoreCase( "PUT" ) ) 
        {
            headers.put( "x-amz-acl", Arrays.asList( "public-read" ) );
        }
        
        return Utils.generateQueryStringAuthentication(
        		awsAccessKey
        		, awsSecretAccessKey
        		, method
        		, bucket
        		, key
        		, new HashMap()
        		, headers
        		, expirationDate
        		);
    }



}





