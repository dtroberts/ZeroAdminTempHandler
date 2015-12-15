package zadmin;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.services.sns.AmazonSNSClient;

/**
 * Further optimizations: 
 *  - find better workaround for lack ResultSet.CONCUR_UPDATABLE
 *  - tune MAX_PROCESSED size
 *  @author Devin Roberts
 */
public class LambdaFunctionHandler implements RequestHandler<SNSEvent, Object> {
	static final int MAX_PROCESSED = 500;
	static final String masterUser = "dtroberts";
	static final String masterPassword = "Masterpw1";
	static final String dbURL = "jdbc:redshift://dtroberts-ec2-test.c0y2ox1yoyin.us-west-2.redshift.amazonaws.com:5439/db";
	static final String destBucket = "dtrobertstestbucket";
	static final String destSNS = "arn:aws:sns:us-west-2:602634635920:RedshiftSuccessfulLoads";
	
    @Override
    /**
     * This function performs a dedupe operation on the temp table against the main table.
     * @param input SNS event which triggered this instance
     * @param context Context used for logging
     */
    public Object handleRequest(SNSEvent input, Context context) {
        Connection conn = null;
		Statement stmt = null;
		String sql, deduped = "";
		ResultSet rs = null;
		
		try {
			Class.forName("com.amazon.redshift.jdbc41.Driver");
			Properties props = new Properties();
			props.setProperty("user", masterUser);
			props.setProperty("password", masterPassword);
			conn = DriverManager.getConnection(dbURL, props);
			
			// retrieve unique rows
			stmt = conn.createStatement();
			sql = "select * into temporary small_temp from temp limit " + MAX_PROCESSED;
			stmt.execute(sql);
			sql = "select * from small_temp except (select * from test)";
			rs = stmt.executeQuery(sql);
			
			while (rs.next()) {
				for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
					deduped += "" + rs.getString(i) + ",";
				}
				deduped = deduped.substring(0, deduped.length()-1);
				deduped += "\r\n";
			}
			
			// remove all processed entries from temp table
			sql = "select * from small_temp";
			rs = stmt.executeQuery(sql);
			String processed = "";
			while (rs.next()) {
				for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
					processed += "" + rs.getString(i) + ",";
				}
				processed = processed.substring(0, processed.length()-1);
				processed += "\r\n";
			}
			stmt = updateTempTable(conn, processed, rs);
			
			// If empty, no new data needs to be written to the bucket
			if (!deduped.isEmpty()) {	
				writeDedupedToBucket(context, deduped);
	        }
			else {
				context.getLogger().log("No new tuples to add");
			}
			
			// If rows still need to be processed, request a new function instance
			sql = "select * from temp limit 1";
			rs = stmt.executeQuery(sql);
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

    /**
     * Update the temp table by removing the processed tuples.
     * @param conn Connection to the database
     * @param processed String containing the 
     **/
	private Statement updateTempTable(Connection conn, String processed,
			ResultSet rs) throws SQLException {
		Statement stmt;
		String sql;
		String[] delete = processed.split("\r\n");
		stmt = conn.createStatement();
		sql = "delete from temp where (";
		for (int i = 0; i < delete.length; i++) {
			sql += "(";
			String[] deleteRow = delete[i].split(",");
			for (int j = 1; j <= rs.getMetaData().getColumnCount(); j++) {
				sql += "(temp." + rs.getMetaData().getColumnName(j) + "=\'" + deleteRow[j-1] + "\')";
				if (j != rs.getMetaData().getColumnCount()) {
					sql += " and ";
				}
			}
			sql += ") or ";
		}
		
		if (!(sql.length() <= 25)) {	// protection against the case where there are no rows to clean up
			sql = sql.substring(0, sql.length()-4);
			sql += ")";
			stmt.executeUpdate(sql);
		}
		return stmt;
	}
	
	/** Write the deduped String as a CSV to the destination bucket.
	 * @param context Context to write log to
	 * @param deduped String to write to the bucket
	 **/
	private void writeDedupedToBucket(Context context, String deduped)
			throws UnsupportedEncodingException, InterruptedException {
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
}
