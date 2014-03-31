package com.cloudera.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

public class ClouderaImpalaJdbcExample {
	
	// here is an example query based on one of the Hue Beeswax sample tables 
	private static final String SQL_STATEMENT = "select count(*) from per_test.parquet_table_uservisits UV join per_test.parquet_table_rankings RA on UV.desturl = RA.pageurl";
	
	// set the impalad host
	private static final String IMPALAD_HOST = "MyImpaladHost";
	
	// port 21050 is the default impalad JDBC port 
	private static final String IMPALAD_JDBC_PORT = "21050";

	private static final String CONNECTION_URL = "jdbc:hive2://" + IMPALAD_HOST + ':' + IMPALAD_JDBC_PORT + "/;auth=noSasl";

	private static final String JDBC_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
	
	private static Connections connections = null;

	public static void main(String[] args)  {
		StringBuilder allArgs = new StringBuilder();
		System.getProperties().toString();
		for (int i=0; i < args.length; i++)
		{
		    //System.out.println("arg"+i+": "+args[i]);
		    allArgs.append(args[i]+" ");
		}
		System.out.println("\n=============================================");
		System.out.println("Cloudera Impala JDBC Example:" + args[0]);
		System.out.println("Using Connection URL: " + CONNECTION_URL);
		System.out.println("Running Query: " + SQL_STATEMENT);
		connections = new Connections();
		for( int i = 0 ; i <3 ; i++){
			try {
				DoQuery();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		connections.CloseAllConnections();
	}
	private static void DoQuery() throws SQLException{
		Connection con = null;
		ResultSet rs = null;
		try {
			Class.forName(JDBC_DRIVER_NAME);
			ImpalaConnection im_cnn = connections.GetConnectionInstance();
			con = im_cnn.con;
			Statement stmt = con.createStatement();
			System.out.println("\n== Host " + im_cnn.host);
			rs = stmt.executeQuery(SQL_STATEMENT);
			System.out.println("\n== Begin Query Results ======================");
			// print the results to the console
			while (rs.next()) {
				// the example query returns one String column
				System.out.println(rs.getString(1));
			}
			System.out.println("== End Query Results =======================\n\n");

		} catch (SQLException e) {
			e.printStackTrace();
			
			SQLWarning warning = rs.getWarnings();
            if (warning != null) 
                    System.out.println("\n---Warning---\n");
            while (warning != null) {
                    System.out.println("Message: " + warning.getMessage());
                    System.out.println("SQLState: " + warning.getSQLState());
                    System.out.print("Vendor error code: ");
                    System.out.println(warning.getErrorCode());
                    System.out.println("");
                    warning = warning.getNextWarning();
             }

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				con.close();
			} catch (Exception e) {
				// swallow
			}
		}		
	}
}
