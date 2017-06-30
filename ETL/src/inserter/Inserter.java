package inserter;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import utils.Logger;
import utils.NeoDb;
import utils.OrclDb;

public abstract class Inserter implements Runnable {
	public NeoDb neoDb;
	public String tableName;
	public Logger logger;
	public int partition;
	private ResultSet rs;
	private Map<String, Object> properties;
	private Map<String, String> columns;
	public Calendar c = Calendar.getInstance();

	public Inserter(OrclDb orclDb, NeoDb neoDb, String tableName, int partition, Logger logger) throws SQLException {
		this.neoDb = neoDb;
		this.tableName = tableName;
		this.partition = partition;
		this.logger = logger;
		this.columns = orclDb.getColumns(tableName);
		this.rs = orclDb.getResultSet(tableName, partition);
	}

	public abstract void createEntity();

	public Map<String, Object> getProperties() {
		return this.properties;
	}

	public void go(Set<String> keys) throws SQLException {
		while (this.makeRecord(keys))
			this.createEntity();
		this.rs.getStatement().close();
		this.rs.close();
	}

	public boolean makeRecord(Set<String> keys) throws SQLException {
		this.properties = new HashMap<String, Object>();
		if (this.rs.next()) {
			for (String key : this.columns.keySet()) {
				String dataType = this.columns.get(key);
				if (dataType.contains("CHAR")) {
					String value = this.rs.getString(key);
					if (value != null)
						this.properties.put(key, value);
				} else if (dataType.equals("NUMBER")) {
					if (keys.contains(key)) {
						Long value = this.rs.getLong(key);
						if (value != null)
							this.properties.put(key, value);
					} else {
						Float value = this.rs.getFloat(key);
						if (value != null)
							this.properties.put(key, value);
					}
				} else if (dataType.equals("DATE")) {
					Date value = this.rs.getDate(key);
					if (value != null)
						this.properties.put(key, this.transDate(value));
				}
			}
			return true;
		}
		return false;
	}

	private long transDate(Date date) {
		c.setTime(date);
		return c.getTimeInMillis();
	}
}
