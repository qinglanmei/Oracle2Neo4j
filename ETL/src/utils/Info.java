package utils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Info {

	public String dbPath;
	public String[] orclInfo;
	public long flushInterval;
	public int threads, partitions, cacheCapacity;
	public Map<String, String[]> edgeMap = new HashMap<String, String[]>();
	public Map<String, String[]> nodeMap = new HashMap<String, String[]>();

	public Info(String conf) throws IOException {
		Properties info = new Properties();
		info.load(this.getClass().getResourceAsStream(conf));
		this.initNodeMap(info.getProperty("node"));
		this.initEdgeMap(info.getProperty("edge"));
		this.initOrclInfo(info);
		this.initNeoInfo(info);
		this.initExecuteInfo(info);
	}
	
	private void initNodeMap(String nodeStr) {
		for (String line : nodeStr.split(",")) {
			String[] s = line.split("[:()]");
			this.edgeMap.put(s[0], new String[] { s[1], s[2] });
		}
	}
	
	private void initEdgeMap(String edgeStr) {
		for (String line : edgeStr.split(",")) {
			String[] s = line.split("(:\\()|(\\)-\\[)|(\\]->\\()|\\)");
			this.edgeMap.put(s[0], new String[] { s[1], s[2], s[3] });
		}
	}

	private void initOrclInfo(Properties info) {
		this.orclInfo = new String[5];
		this.orclInfo[0] = info.getProperty("username");
		this.orclInfo[1] = info.getProperty("password");
		this.orclInfo[2] = info.getProperty("host");
		this.orclInfo[3] = info.getProperty("port");
		this.orclInfo[4] = info.getProperty("sid");
	}
	
	private void initNeoInfo(Properties info) {
		this.dbPath = info.getProperty("dbPath");
		this.flushInterval = Long.parseLong(info.getProperty("flushInterval"));
		this.cacheCapacity = Integer.parseInt(info.getProperty("cacheCapacity"));
	}
	
	private void initExecuteInfo(Properties info) {
		this.partitions = Integer.parseInt(info.getProperty("partitions"));
		this.threads = Integer.parseInt(info.getProperty("threads"));
	}

}
