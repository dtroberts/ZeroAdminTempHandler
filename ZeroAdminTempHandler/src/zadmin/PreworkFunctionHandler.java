package zadmin;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Scanner;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

/**
 * Further optimizations: 
 *  - find better workaround for lack ResultSet.CONCUR_UPDATABLE
 *  - tune MAX_PROCESSED size
 *  @author Devin Roberts
 */
public class PreworkFunctionHandler implements RequestHandler<S3Event, Object> {
	static final int MAX_PROCESSED = 500;
	static final String masterUser = "dtroberts";
	static final String masterPassword = "Masterpw1";
	static final String dbURL = "jdbc:redshift://dtroberts-ec2-test.c0y2ox1yoyin.us-west-2.redshift.amazonaws.com:5439/db";
	static final String destBucket = "dtrobertstestbucket";
	static final String destSNS = "arn:aws:sns:us-west-2:602634635920:RedshiftSuccessfulLoads";
	
    @Override
    /**
     * This function performs a check against the rules file in this bucket.
     * If the file is deemed valid, it is copied to the next bucket for Lambda processing.
     * @param input S3 event which triggered this instance
     * @param context Context used for logging
     */
    public Object handleRequest(S3Event input, Context context) {
    	// Get information about the added file
        S3EventNotificationRecord record = input.getRecords().get(0);
        String srcBucket = record.getS3().getBucket().getName();
        String srcKey = record.getS3().getObject().getKey().replace('+', ' ');
        try {
			srcKey = URLDecoder.decode(srcKey, "UTF-8");
		} catch (UnsupportedEncodingException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
        
        // Check file type
        if (!srcKey.substring(srcKey.length()-4, srcKey.length()).equals(".csv")) {
        	context.getLogger().log("ERROR: File is not .csv. The file will be ignored.");
        	return null;
        }
        String destKey = srcKey;
        
        // Get the object
        AmazonS3 s3Client = new AmazonS3Client();
        S3Object s3Object = s3Client.getObject(new GetObjectRequest(srcBucket, srcKey));
        InputStream objectData = s3Object.getObjectContent();
        
        // Retrieve the first line 
        Scanner scan = new Scanner(objectData, "US-ASCII");
        String dataString = scan.nextLine();
        scan.close();
    	
    	// check formatting
    	// check columns against rules file
    	
    	return null;
    }
}
