package zadmin;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Scanner;

import org.json.JSONArray;
import org.json.JSONObject;

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
		S3Object rulesObject, s3Object = null;
		InputStream rulesData, objectData = null;
		ArrayList<String> columnNames;
		S3EventNotificationRecord record = input.getRecords().get(0);
		String srcBucket = record.getS3().getBucket().getName();
		String srcKey = record.getS3().getObject().getKey().replace('+', ' ');

		try {
			srcKey = URLDecoder.decode(srcKey, "UTF-8");
		} catch (UnsupportedEncodingException e1) {
			e1.printStackTrace();
		}

		// Check file type
		if (!srcKey.substring(srcKey.length()-4, srcKey.length()).equals(".csv")) {
			context.getLogger().log("ERROR: File is not .csv. The file will be ignored.");
			return null;
		}
		String destKey = srcKey;

		// Get the rules file
		// Assumption: The rules file will always have the name "rules.json"
		AmazonS3 s3Client = new AmazonS3Client();
		rulesObject = s3Client.getObject(new GetObjectRequest("dtroberts-preprocess", "rules.json"));
		rulesData = rulesObject.getObjectContent();
		String rulesString = readInputStream(rulesData);

		JSONObject o = new JSONObject(rulesString);
		JSONObject configurationObj = new JSONObject(o.getJSONObject("configuration").toString());
		JSONObject schemaObj = new JSONObject(configurationObj.getJSONObject("schema").toString());
		JSONArray fieldsArray = new JSONArray(schemaObj.getJSONArray("fields").toString());

		// Get column names
		columnNames = new ArrayList<String>();
		for (int i = 0; i < fieldsArray.length(); i++) {
			// Do  something
			String currentField = fieldsArray.getJSONObject(i).getString("name");
			if (!currentField.substring(0, 3).equals("ob_")) {	// ignore "ob_" fields
				columnNames.add(fieldsArray.getJSONObject(i).getString("name"));
				System.out.println(columnNames.get(i));
			}
		}

		// Get the object
		s3Object = s3Client.getObject(new GetObjectRequest(srcBucket, srcKey));
		objectData = s3Object.getObjectContent();

		// Retrieve the first line of the new .csv object
		Scanner scan = new Scanner(objectData, "US-ASCII");
		String dataString = scan.nextLine();
		scan.close();

		// TODO: Load the temp table in the order specified by the rules file
		// - reorder the CSV file itself?
		//		- this allows us to leverage the usefulness of the zero-admin loader
		// - copy into the temp table manually?

		return null;
	}

	public static String readInputStream(InputStream stream) {
		int i;
		char c;
		String result = "";

		try {
			InputStreamReader isr = new InputStreamReader(stream);
			StringBuilder sb = new StringBuilder();
			while((i = isr.read()) != -1) {
				// int to character
				c= (char) i;
				sb.append(c);
			}
			result = sb.toString();
		} catch(Exception e) {
			e.printStackTrace();
		}
		return result;
	}
}
