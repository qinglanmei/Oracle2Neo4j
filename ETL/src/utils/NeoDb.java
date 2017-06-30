package utils;

import java.io.File;
import java.util.Map;
import java.util.HashMap;
import java.io.IOException;

import org.neo4j.io.fs.FileUtils;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;
import org.neo4j.index.lucene.unsafe.batchinsert.LuceneBatchInserterIndexProvider;

@SuppressWarnings("deprecation")
public class NeoDb {

	private int cacheCapacity;
	private long flushInterval;
	private BatchInserter inserter;
	private LuceneBatchInserterIndexProvider indexProvider;
	private Map<String, Long> counter = new HashMap<String, Long>();
	private Map<String, String> pkMap = new HashMap<String, String>();

	public NeoDb(String dbPath, long flushInterval, int cacheCapacity) throws IOException {
		this.flushInterval = flushInterval;
		this.cacheCapacity = cacheCapacity;
		File tempStoreDir = new File(dbPath);
		FileUtils.deleteRecursively(tempStoreDir);
		this.inserter = BatchInserters.inserter(tempStoreDir);
		this.indexProvider = new LuceneBatchInserterIndexProvider(this.inserter);
	}

	public BatchInserterIndex createIndex(String labelName, String pk) {
		BatchInserterIndex idx = this.getIndex(labelName);
		idx.setCacheCapacity(pk, this.cacheCapacity);
		this.counter.put(labelName, this.flushInterval);
		this.pkMap.put(labelName, pk);
		return idx;
	}

	public void insertNode(Map<String, Object> properties, String label, BatchInserterIndex idx, String pk) {
		this.createNode(label, properties, idx, pk);
	}

	public synchronized void insertEdge(long[] id, String typeName, Map<String, Object> properties) {
		this.inserter.createRelationship(id[0], id[1], DynamicRelationshipType.withName(typeName), properties);
	}

	public long getId(String labelName, long orclId) {
		try (IndexHits<Long> hits = this.getIndex(labelName).get(this.pkMap.get(labelName), orclId)) {
			return hits.getSingle();
		}
	}

	public void shutDown() {
		this.indexProvider.shutdown();
		this.inserter.shutdown();
	}

	private synchronized void createNode(String labelName, Map<String, Object> properties, BatchInserterIndex idx,
			String pk) {
		idx.add(this.inserter.createNode(properties, DynamicLabel.label(labelName)),
				MapUtil.map(pk, properties.get(pk)));
		long n = this.counter.get(labelName);
		this.counter.put(labelName, ++n);
		if(n % this.flushInterval == 0)
			idx.flush();
	}

	private BatchInserterIndex getIndex(String labelName) {
		return this.indexProvider.nodeIndex(labelName, MapUtil.stringMap("type", "exact"));
	}

}
