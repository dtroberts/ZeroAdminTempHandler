package zadmin;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;

import org.json.*;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

/** 
 * Used for on-the-fly testing.
 * @author Devin Roberts
 *
 */
public class Main {
	static final int MAX_PROCESSED = 500;
	static final String masterUser = "dtroberts";
	static final String masterPassword = "Masterpw1";
	static final String dbURL = "jdbc:redshift://dtroberts-ec2-test.c0y2ox1yoyin.us-west-2.redshift.amazonaws.com:5439/db";
	static final String destBucket = "dtrobertstestbucket";
	static final String destKey = "" + System.currentTimeMillis() + ".csv";

	public static void main(String[] args) {
		// Get information about the added file
		S3Object rulesObject, s3Object = null;
		InputStream rulesData, objectData = null;
		String rulesKey = "rules.json";
		ArrayList<String> columnNames = new ArrayList<String>();

		try {
			rulesKey = URLDecoder.decode(rulesKey, "UTF-8");
		} catch (UnsupportedEncodingException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

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
		System.out.println();
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
