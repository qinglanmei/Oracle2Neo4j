package app;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import inserter.Inserter;
import inserter.InserterFactory;
import utils.Info;
import utils.Logger;
import utils.NeoDb;
import utils.OrclDb;

public class Controller {
	private int threads;
	private NeoDb neoDb;
	private OrclDb orclDb;
	private Logger logger;
	private InserterFactory factory;

	public Controller(String conf) throws IOException, SQLException {
		this.logger = new Logger("log");
		Info info = new Info(conf);
		this.threads = info.threads;
		this.neoDb = new NeoDb(info.dbPath, info.flushInterval, info.cacheCapacity);
		this.orclDb = new OrclDb(info.orclInfo);
		this.factory = new InserterFactory(this.orclDb, this.neoDb, info.nodeMap, info.edgeMap, info.partitions,
				this.logger);
	}

	public static void main(String[] args) throws InterruptedException, IOException, SQLException {
		new Controller("/init.properties").go().shutDown();
	}

	public Controller go() throws InterruptedException {
		this.logger.write("Start to insert.");
		this.insert("node");
		this.insert("edge");
		this.logger.write("Mission over");
		return this;
	}

	private void insert(String insertType) throws InterruptedException {
		this.logger.write("Start to insert" + insertType + ".");
		Iterator<Inserter> iter = this.factory.inserters(insertType);
		ExecutorService pool = Executors.newFixedThreadPool(this.threads);
		while (iter.hasNext())
			pool.execute(iter.next());
		pool.shutdown();
		while (true) {
			if (pool.isTerminated()) {
				this.logger.write("Finish " + insertType + " insert.");
				break;
			}
			Thread.sleep(20);
		}

	}

	private void shutDown() throws SQLException {
		this.factory.shutdown();
	}
}
