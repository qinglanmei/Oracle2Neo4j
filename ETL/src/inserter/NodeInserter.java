package inserter;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;

import org.neo4j.unsafe.batchinsert.BatchInserterIndex;

import utils.Logger;
import utils.NeoDb;
import utils.OrclDb;

public class NodeInserter extends Inserter {

	private String pk;
	private String label;
	private BatchInserterIndex idx;
	private InserterFactory factory;

	public NodeInserter(OrclDb orclDb, NeoDb neoDb, String tableName, String[] biGroup, Integer partition,
			Logger logger, InserterFactory factory) throws SQLException {
		super(orclDb, neoDb, tableName, partition, logger);
		this.label = biGroup[0];
		this.pk = biGroup[1];
		this.idx = neoDb.createIndex(this.label, this.pk);
		this.factory = factory;
	}

	@Override
	public void createEntity() {
		this.neoDb.insertNode(this.getProperties(), this.label, this.idx, this.pk);
	}

	@Override
	public void run() {
		this.logger.write("Start to deal with table:" + this.tableName + "(p" + this.partition + ").");
		String logInfo = "Map " + this.tableName + "(p" + this.partition + ")->(" + this.label + ")";
		try {
			super.go(new HashSet<String>(Arrays.asList(new String[] { this.pk })));
			this.idx.flush();
			logInfo += " finished.";
		} catch (Exception e) {
			logInfo += " failed.";
			e.printStackTrace();
		} finally {
			this.logger.write(logInfo);
			this.factory.inform(this.tableName, "node");
		}
	}

}
