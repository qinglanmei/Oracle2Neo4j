package inserter;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import utils.Logger;
import utils.NeoDb;
import utils.OrclDb;

public class InserterFactory {

	private Logger logger;
	private NeoDb neoDb;
	private OrclDb orclDb;
	private Map<String, String[]> nodeMap;
	private Map<String, String[]> edgeMap;
	private Map<String, Integer[]> nodeCondition;
	private Map<String, Integer[]> edgeCondition;
	private String currentTableName;

	public InserterFactory(OrclDb orclDb, NeoDb neoDb, Map<String, String[]> nodeMap, Map<String, String[]> edgeMap,
			int partitions, Logger logger) {
		this.logger = logger;
		this.neoDb = neoDb;
		this.orclDb = orclDb;
		this.nodeMap = nodeMap;
		this.edgeMap = edgeMap;
		this.nodeCondition = this.initCondition(nodeMap.keySet(), partitions);
		this.edgeCondition = this.initCondition(edgeMap.keySet(), partitions);
	}

	public Iterator<Inserter> inserters(String insertType) {
		return new Iterator<Inserter>() {
			@Override
			public boolean hasNext() {
				return pickTable(insertType.equals("node") ? nodeCondition : edgeCondition);
			}

			@Override
			public Inserter next() {
				Inserter inserter = null;
				try {
					return getInserter(insertType);
				} catch (SQLException e) {
					e.printStackTrace();
				}
				return inserter;
			}
		};
	}

	public synchronized void inform(String tableName, String type) {
		if (type.equals("node"))
			this.nodeCondition.get(tableName)[0]--;
		else
			this.edgeCondition.get(tableName)[0]--;
	}

	public void shutdown() throws SQLException {
		this.orclDb.shutDown();
		this.neoDb.shutDown();
	}

	private synchronized Inserter getInserter(String insertType) throws SQLException {
		Inserter res = null;
		Map<String, Integer[]> condition = null;
		if (insertType.equals("node")) {
			res = new NodeInserter(this.orclDb, this.neoDb, this.currentTableName,
					this.nodeMap.get(this.currentTableName), this.nodeCondition.get(this.currentTableName)[1], this.logger,
					this);
			condition = this.nodeCondition;
		} else {
			res = new EdgeInserter(this.orclDb, this.neoDb, this.currentTableName,
					this.edgeMap.get(this.currentTableName), this.edgeCondition.get(this.currentTableName)[1], this.logger,
					this);
			condition = this.edgeCondition;
		}
		Integer[] pair = condition.get(this.currentTableName);
		pair[0]++;
		pair[1]--;
		condition.put(this.currentTableName, pair);
		return res;
	}

	private Map<String, Integer[]> initCondition(Set<String> keySet, int partitions) {
		Map<String, Integer[]> res = new HashMap<String, Integer[]>();
		for (String tableName : keySet)
			res.put(tableName, new Integer[] { 0, partitions });
		return res;
	}

	private int min(Set<Integer> set) {
		int res = Integer.MAX_VALUE;
		for (int i : set)
			res = i < res ? i : res;
		return res;
	}

	private synchronized boolean pickTable(Map<String, Integer[]> condition) {
		Set<Integer> set = new HashSet<Integer>();

		for (Integer[] pair : condition.values())
			set.add(pair[0]);

		while (!set.isEmpty()) {
			int minExecution = this.min(set);
			for (String tableName : condition.keySet()) {
				if (condition.get(tableName)[0] == minExecution && condition.get(tableName)[1] > 0) {
					this.currentTableName = tableName;
					return true;
				}
			}
			set.remove(minExecution);
		}
		return false;
	}

}
