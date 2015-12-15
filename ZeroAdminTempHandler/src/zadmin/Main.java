package zadmin;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sns.AmazonSNSClient;

/** 
 * Used for on-the-fly testing.
 * @author Devin Roberts
 *
 */
public class Main {
	static final int maxProcessed = 1000;
	static final String masterUser = "dtroberts";
	static final String masterPassword = "Masterpw1";
	static final String dbURL = "jdbc:redshift://dtroberts-ec2-test.c0y2ox1yoyin.us-west-2.redshift.amazonaws.com:5439/db";
	static final String destBucket = "dtrobertstestbucket";
	static final String destKey = "" + System.currentTimeMillis() + ".csv";
	
	public static void main(String[] args) {
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
			conn.setReadOnly(false);
			// Get metadata
			stmt = conn.createStatement();
			sql = "select * from temp except (select * from test)";
			rs = stmt.executeQuery(sql);

			int count = 0;
			while (count < maxProcessed && rs.next()) {
				for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
					deduped += "" + rs.getString(i) + ",";
				}
				deduped = deduped.substring(0, deduped.length()-1);
				deduped += "\r\n";
				count++;
			}
			if (rs.next()) {
				AmazonSNSClient snsClient = new AmazonSNSClient();
				snsClient.setRegion(Region.getRegion(Regions.US_WEST_2));
			}
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
			sql = sql.substring(0, sql.length()-4);
			sql += ")";
			System.out.println(sql);
			stmt.execute(sql);
			System.out.println(deduped);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}

}
