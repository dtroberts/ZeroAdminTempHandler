package zeroadmin;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.Scanner;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;

public class LambdaFunctionHandler implements RequestHandler<S3Event, Object> {
	static final String masterUser = "dtroberts";
	static final String masterPassword = "7Drebinx7";
	static final String dbURL = "jdbc:redshift://dtroberts-ec2-test.c0y2ox1yoyin.us-west-2.redshift.amazonaws.com:5439/db";
	static final String destBucket = "dtrobertstestbucket";
	
    @Override
    public String handleRequest(S3Event input, Context context) {
        String deduped = "";
        
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
        System.out.println(srcBucket);
        String destKey = srcKey;
        
        // Get the object
        AmazonS3 s3Client = new AmazonS3Client();
        S3Object s3Object = s3Client.getObject(new GetObjectRequest(srcBucket, srcKey));
        InputStream objectData = s3Object.getObjectContent();
        
        Scanner scan = new Scanner(objectData, "US-ASCII");
        String dataString = "";
        // Scan data to String
        while (scan.hasNext()) {
        	dataString += scan.nextLine() + "\n";
        }
        scan.close();
        
        // Perform deduplication check
        deduped = dedupe(dataString);
        
        // Write the output to the source bucket
        if (!deduped.isEmpty()) {	// If so, no new data needs to be written to the bucket
        	try {
        		InputStream is = new ByteArrayInputStream(deduped.getBytes("US-ASCII"));
        		ObjectMetadata metadata = new ObjectMetadata();
        		metadata.setContentType((String) "application/octet-stream");
        		metadata.setContentLength(deduped.length());
        		s3Client.putObject(destBucket, destKey, is, metadata);
        	} catch (IOException e) {
        		// TODO Auto-generated catch block
        		e.printStackTrace();
        	}
        	context.getLogger().log("Successfully added the following entries: " + deduped);
            return deduped;
        }
        else {
        	context.getLogger().log("Success: No new data");
        	return "";
        }
    }

    /** Perform deduplication check against the master table. 
     * 	@param dataString new data to check against master table
     * 	@return	CSV-formatted String of entries which are not currently in the master table
     * */
	private String dedupe(String dataString) {
		Connection conn = null;
		Statement stmt = null;
		String sql, deduped = "";
		ResultSet rs = null;
		ResultSetMetaData rsMetaData = null;
		
		try {
			Class.forName("com.amazon.redshift.jdbc41.Driver");
			Properties props = new Properties();
			
			props.setProperty("user", masterUser);
			props.setProperty("password", masterPassword);
			conn = DriverManager.getConnection(dbURL, props);
			// Get metadata
			stmt = conn.createStatement();
			sql = "select * from test";
			rs = stmt.executeQuery(sql);
			rsMetaData = rs.getMetaData();
			String[] columns = new String[rsMetaData.getColumnCount()];
			for (int i = 1; i <= rsMetaData.getColumnCount(); i++) {
				columns[i-1] = rsMetaData.getColumnName(i);
			}
			
			// Create an ArrayList of all results to use for comparison
			ArrayList<String[]> tableData = new ArrayList<String[]>();
			while (rs.next()) {
				String[] rowData = new String[columns.length];
				for (int i = 1; i <= columns.length; i++) {
					rowData[i-1] = rs.getString(i);
				}
				tableData.add(rowData);
			}
			
			for (String row : dataString.split("\n")) {
				boolean flag = false;
				String[] values = row.split(",");
				for (String[] data : tableData) {
					if (Arrays.equals(values, data)) {
						flag = true;
						break;
					}
				}
				if (!flag) {
					deduped += "" + row + "\r\n";
				}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return deduped;
	}

}
