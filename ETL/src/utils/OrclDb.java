package utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class OrclDb {

	private String username;
	private Connection conn;

	public OrclDb(String[] info) throws SQLException {
		this.username = info[0];
		String url = "jdbc:oracle:this:@" + info[2] + ":" + info[3] + ":" + info[4];
		this.conn = DriverManager.getConnection(url, this.username, info[1]);
	}

	public Map<String, String> getColumns(String tableName) throws SQLException {
		Map<String, String> columns = new HashMap<String, String>();
		ResultSet rs = this.conn.createStatement().executeQuery(String.format(
				"SELECT COLUMN_NAME,DATA_TYPE FROM USER_TAB_COLUMNS WHERE TABLE_NAME='%s'", tableName.toUpperCase()));

		while (rs.next())
			columns.put(rs.getString("COLUMN_NAME"), rs.getString("DATA_TYPE"));
		rs.getStatement().close();
		return columns;
	}

	public ResultSet getResultSet(String tableName, int partition) throws SQLException {
		return this.conn.createStatement()
				.executeQuery(String.format("SELECT * FROM %s PARTITION(p%s)", tableName, partition));
	}

	public void shutDown() throws SQLException {
		this.conn.close();
	}

}
