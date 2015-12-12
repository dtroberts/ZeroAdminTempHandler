package zeroadmin;

import java.awt.List;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.Scanner;

public class Main {
	static final String masterUser = "dtroberts";
	static final String masterPassword = "7Drebinx7";
	static final String dbURL = "jdbc:redshift://dtroberts-ec2-test.c0y2ox1yoyin.us-west-2.redshift.amazonaws.com:5439/db";
	static final String destBucket = "dtrobertstestbucket";
	
	public static void main(String[] args) {
		Connection conn = null;
		Statement stmt = null;
		String sql, deduped = "";
		ResultSet rs = null;
		ResultSetMetaData rsMetaData = null;
		
        String dataString = "17\n19\n3\n4\n5\n";
        // Scan data to String
		
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
			
			ArrayList<String[]> tableData = new ArrayList<String[]>();
			rsMetaData = rs.getMetaData();
			String[] columns = new String[rsMetaData.getColumnCount()];
			for (int i = 1; i <= rsMetaData.getColumnCount(); i++) {
				columns[i-1] = rsMetaData.getColumnName(i);
			}
			
			
			while (rs.next()) {
				String[] rowData = new String[columns.length];
				for (int i = 1; i <= columns.length; i++) {
					rowData[i-1] = rs.getString(i);
				}
				tableData.add(rowData);
			}
			
			
			// TODO: Optimize; possibly iterate through rs instead
			
			
			
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
			//	}
				/*stmt = conn.createStatement();
				sql = "select * from test where ";
				for (int i = 0; i < values.length; i++) {
					if (i == values.length-1) {
						sql += "" + columns[i] + " = " + "\'" + values[i] + "\'";
					}
					else {
						sql += "" + columns[i] + " = " + "\'" + values[i] + "\' and ";
					}
				}
				rs = stmt.executeQuery(sql);
				
				if (!rs.next()) {
					 deduped += "" + row + "\r\n";
				}*/
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println(deduped);
	}
}
