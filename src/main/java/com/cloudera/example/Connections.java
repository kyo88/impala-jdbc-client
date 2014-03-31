package com.cloudera.example;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class Connections {
	List<ImpalaConnection> list = new ArrayList();
	private int index = 0;
	ArrayList<String> hosts = new ArrayList<String>();
	private String IMPALAD_HOST = null;
	private String IMPALAD_JDBC_PORT = "21050";

	public Connections() {
		CreateListHost();
		CreateConnections();
	}

	private void CreateListHost() {
		hosts.add("10.112.21.75");
		hosts.add("10.112.21.76");
		hosts.add("10.112.21.77");
		hosts.add("10.112.21.78");
		hosts.add("10.112.21.79");
	}

	private void CreateConnections() {
		int num = hosts.size();
		for (int i = 0; i < num; i++) {
			CreateConnection(hosts.get(i));
		}
	}

	private void CreateConnection(String host) {
		list.add(new ImpalaConnection(host, CreateConnectionString(host)));
	}

	private String CreateConnectionString(String host) {
		IMPALAD_HOST = host;
		String CONNECTION_URL = "jdbc:hive2://" + IMPALAD_HOST + ':'
				+ IMPALAD_JDBC_PORT + "/;auth=noSasl";
		return CONNECTION_URL;
	}

	public ImpalaConnection GetConnectionInstance() {
		int num = list.size();
		if (index < num - 1) {
			for (int i = index + 1; i < num; i++) {
				if (list.get(i).flag == Flag.CONNECTED) {
					index = i;
					return list.get(i);
				}
			}
		}else{
			index = 0;
			for (int i = index + 1; i < num; i++) {
				if (list.get(i).flag == Flag.CONNECTED) {
					index = i;
					return list.get(i);
				}
			}
		}
		return null;
	}
	public void CloseAllConnections(){
		int num = hosts.size();
		for (int i = 0; i < num; i++) {
			try {
				list.get(i).con.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
}
