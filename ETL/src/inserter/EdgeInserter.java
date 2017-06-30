package inserter;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import utils.Logger;
import utils.NeoDb;
import utils.OrclDb;

public class EdgeInserter extends Inserter {

	private static final Set<String> FK = new HashSet<String>(Arrays.asList(new String[] { "SRC", "DEST" }));
	private String fromLabel;
	private String type;
	private String toLabel;
	private InserterFactory factory;

	public EdgeInserter(OrclDb orclDb, NeoDb neoDb, String tableName, String[] triGroup, int partition, Logger logger,
			InserterFactory factory) throws SQLException {
		super(orclDb, neoDb, tableName, partition, logger);
		this.fromLabel = triGroup[0];
		this.type = triGroup[1];
		this.toLabel = triGroup[2];
		this.factory = factory;
	}

	@Override
	public void createEntity() {
		this.neoDb.insertEdge(this.getId(), this.type, this.getProperties());
	}

	@Override
	public void run() {
		this.logger.write("Start to deal with table:" + this.tableName + "(p" + this.partition + ").");
		String logInfo = "Map " + this.tableName + "(p" + this.partition + ")->[" + this.type + "]";
		try {
			super.go(FK);
			logInfo += " finished.";
		} catch (Exception e) {
			logInfo += " failed.";
			e.printStackTrace();
		} finally {
			this.logger.write(logInfo);
			this.factory.inform(this.tableName, "edge");
		}
	}

	private long[] getId() {
		Map<String, Object> properties = this.getProperties();
		long id1 = (Long) properties.get("SRC");
		long id2 = (Long) properties.get("DEST");
		properties.remove("SRC");
		properties.remove("DEST");
		return new long[] { this.neoDb.getId(this.fromLabel, id1), this.neoDb.getId(this.toLabel, id2) };
	}

}
