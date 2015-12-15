package zadmin;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.services.sns.AmazonSNSClient;

public class LambdaFunctionHandler implements RequestHandler<SNSEvent, Object> {
	static final int MAX_PROCESSED = 500;
	static final String masterUser = "dtroberts";
	static final String masterPassword = "7Drebinx7";
	static final String dbURL = "jdbc:redshift://dtroberts-ec2-test.c0y2ox1yoyin.us-west-2.redshift.amazonaws.com:5439/db";
	static final String destBucket = "dtrobertstestbucket";
	static final String destSNS = "arn:aws:sns:us-west-2:602634635920:RedshiftSuccessfulLoads";
	
    @Override
    public Object handleRequest(SNSEvent input, Context context) {
        // dedupe temp table against main table
        Connection conn = null;
		Statement stmt = null;
		String sql, deduped = "";
		ResultSet rs = null;
		
		try {
			Class.forName("com.amazon.redshift.jdbc4.Driver");
			Properties props = new Properties();
			
			props.setProperty("user", masterUser);
			props.setProperty("password", masterPassword);
			conn = DriverManager.getConnection(dbURL, props);
			// Get metadata
			stmt = conn.createStatement();
			sql = "select * from temp except (select * from test)";
			rs = stmt.executeQuery(sql);
			
			int count = 0;
			while (count < MAX_PROCESSED && rs.next()) {
				for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
					deduped += "" + rs.getString(i) + ",";
				}
				deduped = deduped.substring(0, deduped.length()-1);
				deduped += "\r\n";
				count++;
			}
			
			// remove all processed entries from temp table
			String[] delete = deduped.split("\r\n");
			stmt = conn.createStatement();
			sql = "delete from temp where (";
			while (count > 0) {
				sql += "(";
				String[] deleteRow = delete[count-1].split(",");
				for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
					sql += "(temp." + rs.getMetaData().getColumnName(i) + "=\'" + deleteRow[i-1] + "\')";
					if (i != rs.getMetaData().getColumnCount()) {
						sql += " and ";
					}
				}
				sql += ") or ";
				count--;
			}
			if (!(sql.length() <= 25)) {	// protection against the case where there are no rows to clean up
				sql = sql.substring(0, sql.length()-4);
				sql += ")";
				stmt.executeUpdate(sql);
			}
			
			if (!deduped.isEmpty()) {	// If empty, no new data needs to be written to the bucket
				TransferManager tx = new TransferManager();
				InputStream is = new ByteArrayInputStream(deduped.getBytes("US-ASCII"));
				ObjectMetadata metadata = new ObjectMetadata();
				metadata.setContentType((String) "application/octet-stream");
				metadata.setContentLength(deduped.length());
				String destKey = "" + System.currentTimeMillis() + ".csv";
				PutObjectRequest putRequest = new PutObjectRequest(destBucket, destKey, is, metadata);
				Upload up = tx.upload(putRequest);
				up.waitForCompletion();		// must block so that S3 does not get overloaded
				context.getLogger().log("Successfully added " + destKey + " to " + destBucket);
				context.getLogger().log(deduped);
	        }
			
			if (rs.next()) {
				stmt.close();
				conn.close();
				AmazonSNSClient snsClient = new AmazonSNSClient();
				snsClient.setRegion(Region.getRegion(Regions.US_WEST_2));
				context.getLogger().log("New instance requested for further processing");
				snsClient.publish(destSNS, "run new dedupe");
			}
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
        return null;
    }

}