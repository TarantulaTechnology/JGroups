package org.jgroups.protocols.aws.s3;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.jgroups.protocols.aws.s3.signer.AWS4Signer;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


@Test
public class AWS_AUTH4Test {
    
	private String awsAccessKey;
	private String awsSecretKey;
	private String bucketName;
	private String regionName;
	private String key;
	private String objectContent = "Test " + new Date().toString();
	private boolean isSecure;

    @BeforeClass
    void setUp() {
        isSecure = true;
        awsAccessKey = System.getenv("awsAccessKey");
        awsSecretKey = System.getenv("awsSecretKey");
        bucketName = System.getenv("bucketName");
        regionName = System.getenv("regionName"); // Frankfurt eu-central-1
        key = System.getenv("key");
    }

    @Test
    public void testPutObject() {
        String method = "PUT"; // test lowercase to uppercase
        byte[] contentHash = AWS4Signer.hash(objectContent);
        String bodyHash = AWS4Signer.toHex(contentHash);
        Map<String, String> queryParameters = new HashMap<String, String>();;
        Map<String, String> headers = new HashMap<String, String>();
        String content_length = "" + objectContent.length();
        headers.put("content-length", content_length);
        headers.put("x-amz-content-sha256", bodyHash);
        headers.put("x-amz-storage-class", "REDUCED_REDUNDANCY");
        String authorization = AWS4Signer.getAWS4AuthorizationForHeader(
        		isSecure, method, bucketName, key, regionName, 
        		headers, queryParameters, bodyHash, awsAccessKey, 
        		awsSecretKey);
        Assert.assertNotNull(authorization);
        // add authorization header
        headers.put( "Authorization", authorization );
        URL endpointUrl = AWS4Signer.buildEndpointURLForVirtualHostedStyle( 
        		isSecure, regionName, bucketName, key );
        // make HTTP request, null request body
        String response = AWS4Signer.invokeHttpRequest( 
        		endpointUrl, method, headers, objectContent );
        Assert.assertFalse(response.contains("Error"));
   }

    @Test(dependsOnMethods = { "testPutObject" })
    public void testGetBucketLocation() {
        String versioningCommand = "versioning";
        String cmd = versioningCommand;
        
    	String method = "GET"; // test lowercase to uppercase
        String bodyHash = AWS4Signer.EMPTY_BODY_SHA256;
        
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("x-amz-content-sha256", AWS4Signer.EMPTY_BODY_SHA256);
        
        Map<String, String> queryParameters = new HashMap<String, String>();
        //queryParameters.put("location", "");
        queryParameters.put(cmd, "");
       
        String authorization = AWS4Signer.getAWS4AuthorizationForHeader(
        		isSecure, method, bucketName, regionName, headers, 
        		queryParameters, bodyHash, awsAccessKey, awsSecretKey);
        
        Assert.assertNotNull(authorization);
        // add authorization header
        headers.put( "Authorization", authorization );

        URL endpointUrl = null;
		try {
			endpointUrl = new URL(
					"https://" + bucketName + ".s3.amazonaws.com/?" + cmd);
		} catch (MalformedURLException e) {
			e.printStackTrace();
		}
        // make HTTP request, null request body
        String response = AWS4Signer.invokeHttpRequest( 
        		endpointUrl, method, headers, null );
        System.out.println("\n------- testGetBucketLocation request ------");
        System.out.println(response);
        System.out.println("------------------------------------");
        Assert.assertTrue(!response.contains("Error"));
   }
    
    @Test(dependsOnMethods = { "testPutObject" })
    public void testGetService() {
        String method = "GET"; // test lowercase to uppercase
        String bodyHash = AWS4Signer.EMPTY_BODY_SHA256;
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("x-amz-content-sha256", AWS4Signer.EMPTY_BODY_SHA256);
        Map<String, String> queryParameters = new HashMap<String, String>();
        
        String authorization = AWS4Signer.getAWS4AuthorizationForHeader(
        		isSecure, method, headers, queryParameters, 
        		bodyHash, awsAccessKey, awsSecretKey);
        
        Assert.assertNotNull(authorization);
        // add authorization header
        headers.put( "Authorization", authorization );
        URL endpointUrl = null;
		try {
			endpointUrl = new URL("https://s3.amazonaws.com/");
		} catch (MalformedURLException e) {
			e.printStackTrace();
		}
        // make HTTP request, null request body
        String response = AWS4Signer.invokeHttpRequest( 
        		endpointUrl, method, headers, null );
        System.out.println("\n--------- testGetService request --------");
        System.out.println(response);
        System.out.println("------------------------------------");
        Assert.assertTrue(response.contains("ListAllMyBucketsResult"));
   }

    @Test(dependsOnMethods = { "testPutObject" })
    public void testGetObject() {
     String method = "GET"; // test lowercase to uppercase
     String bodyHash = AWS4Signer.EMPTY_BODY_SHA256;
     Map<String, String> headers = new HashMap<String, String>();
     headers.put("x-amz-content-sha256", AWS4Signer.EMPTY_BODY_SHA256);
     Map<String, String> queryParameters = null;//new HashMap<String, String>();
     
     String authorization = AWS4Signer.getAWS4AuthorizationForHeader(
     		isSecure, method, bucketName, key, regionName, 
     		headers, queryParameters, bodyHash, awsAccessKey, awsSecretKey);
     
     Assert.assertNotNull(authorization);
     // add authorization header
     headers.put( "Authorization", authorization );
     URL endpointUrl = AWS4Signer.buildEndpointURLForVirtualHostedStyle( 
    		 isSecure, regionName, bucketName, key );
     // make HTTP request, null request body
     String response = AWS4Signer.invokeHttpRequest( 
    		 endpointUrl, method, headers, null );
     System.out.println("\n--------- testGetObject request --------");
     System.out.println(response);
     System.out.println("------------------------------------");
     Assert.assertTrue(response.contains(objectContent));
}

    @Test(dependsOnMethods = { "testPutObject" })
    public void testGetPresignedURL() {
        String method = "get"; // test lowercase to uppercase
        Map<String, String>queryParameters = new HashMap<String, String>();;
        int expiresIn = 7 * 24 * 60 * 60;
        queryParameters.put("X-Amz-Expires", "" + expiresIn);
        Map<String, String> headers = new HashMap<String, String>();
        String bodyHash = AWS4Signer.UNSIGNED_PAYLOAD;
        String url = AWS4Signer.getPresignedURL(
        		method, isSecure, bucketName, regionName, 
        		key, queryParameters, headers, bodyHash, 
        		awsAccessKey, awsSecretKey);
        // test url string
        URL req = null;
		try {
			req = new URL(url);
		} catch (MalformedURLException e) {
			e.printStackTrace();
		}
		String response = AWS4Signer.invokeHttpRequest( 
				req, method, null, null );
        System.out.println("\n--------- testGetPresignedURL request --------");
        System.out.println(response);
        System.out.println("------------------------------------");
        Assert.assertTrue(response.contains(objectContent));
    }

}
