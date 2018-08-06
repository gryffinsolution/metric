package main;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import util.ADao;
import util.CDao;
import util.Conf;
import util.RDao;

public class MetricMgr {
	private static final Logger LOG = LogManager.getLogger(MetricMgr.class);

	public static void printSQLException(SQLException e) {
		while (e != null) {
			LOG.error("\n----- SQLException -----");
			LOG.error("  SQL State:  " + e.getSQLState());
			LOG.error("  Error Code: " + e.getErrorCode());
			LOG.error("  Message:    " + e.getMessage());
			e = e.getNextException();
		}
	}

	public static void main(String[] args) {

		DateTime start = new DateTime();

		Conf cf = new Conf();
		if (args.length != 0 && args[0] != null) {
			cf.setConfFile(args[0]);
		} else {
			LOG.error("there is no config.xml as a args[0]");
			System.exit(0);
		}

		//
		//
		//
		//

		String rdbUrl = cf.getDbURL();
		String rdbUser = cf.getSingleString("user");
		String rdbPasswd = cf.getSingleString("password");
		int thAll = cf.getSingleValue("no_of_thread");
		int thNo = cf.getSingleValue("thread_no");
		int agentPort = cf.getSingleValue("agent_port");
		long baseMinusSecFrConf = cf
				.getLongValue("base_sec_from_now_for_late_host");
		String customCategory1st = cf.getSingleString("custom_category_1st");
		String sql = cf.getSingleString("get_host_sql");
		String seed = cf.getSingleString("seeds");
		String[] seeds = seed.split(",");
		CDao cdao = new CDao();
		cdao.connect(seeds);
		String TTL = cf.getSingleString("TTL_Second");

		RDao rDao = new RDao();
		Connection conn = rDao.getConnection(rdbUrl, rdbUser, rdbPasswd);
		ArrayList<String> hosts = rDao.getHostsMT(conn, thNo, thAll, sql);

		// ArrayList<String> hosts = rDao.getHostsTest("localhost");
		// rDao.setCollectionTimeStampTest(conn, rdbUrl, thNo, "localhost");

		HashMap<String, String> hostKVtime = new HashMap<String, String>();
		HashMap<String, Boolean> hostKVisCustom = new HashMap<String, Boolean>();

		rDao.getLastCollectionTimeNisCustom(conn, hosts, hostKVtime,
				hostKVisCustom, customCategory1st, baseMinusSecFrConf);

		int i = 0;
		ADao adao = new ADao();
		for (String host : hosts) {
			LOG.trace(thNo + "-" + i + ":Checking:" + host);
			i++;
			DateTime startT = new DateTime();
			String line = null;
			rDao.setCollectionStartTimestamp(conn, rdbUrl, thNo, host);
			try {
				line = adao.getMetricsNT(agentPort, host, hostKVtime.get(host),
						hostKVisCustom.get(host));
				LOG.info("FINAL=" + line);
			} catch (NullPointerException e) {
				LOG.error("host(" + host + ") output is null");
				continue;
			}
			DateTime endT = new DateTime();
			Duration elapsedT = new Duration(startT, endT);
			long elapsedSecInt = (elapsedT.getMillis() / 1000);

			if (line != null && line.length() > 100) {
				if (!cdao.insertMetric(host, line, TTL)) {
					LOG.error("metricInsertError2Cassandra=" + host);
				}
				rDao.setCollectionEndTimestamp(conn, rdbUrl, thNo, host,
						elapsedSecInt);
			} else {
				if (line == null)
					LOG.info(host + " agent is not working");
				else if (line.length() <= 100)
					LOG.info(host + " no data");
			}
			DateTime end = new DateTime();
			Duration elapsedTime = new Duration(start, end);
			LOG.fatal("elapsedTime=" + elapsedTime);
		}
		cdao.disconnect();
		rDao.setWorkingTimestamp(conn, rdbUrl, thNo);
		rDao.disconnect(conn);
	}
}