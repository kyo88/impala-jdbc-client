package com.cloudera.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ImpalaConnection {
	public Connection con = null;
	public Flag flag = Flag.NOTCONNECT;
	public String host = null;
//	private String JDBC_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
	private final String JDBC_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
	public ImpalaConnection(String ip,String CONNECTION_URL){
		host = ip;
		try {
			try {
				Class.forName(JDBC_DRIVER_NAME);
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			con = DriverManager.getConnection(CONNECTION_URL);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		flag = Flag.CONNECTED;
	}
	
}
