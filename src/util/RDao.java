package util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RDao {
	private static final Logger LOG = LogManager.getLogger(RDao.class);

	public static void printSQLException(SQLException e) {
		while (e != null) {
			LOG.error("\n----- SQLException -----");
			LOG.error("  SQL State:  " + e.getSQLState());
			LOG.error("  Error Code: " + e.getErrorCode());
			LOG.error("  Message:    " + e.getMessage());
			e = e.getNextException();
		}
	}

	public Connection getConnection(String dbURL, String user, String password) {
		LOG.info("DB_URL=" + dbURL);
		Connection con = null;
		try {
			if (dbURL.startsWith("jdbc:postgresql:")) {
				Class.forName("org.postgresql.Driver");
			} else if (dbURL.startsWith("jdbc:oracle:")) {
				Class.forName("oracle.jdbc.driver.OracleDriver");
			}
		} catch (ClassNotFoundException e) {
			LOG.error("DB Driver loading error!");
			e.printStackTrace();
		}
		try {
			con = DriverManager.getConnection(dbURL, user, password);
		} catch (SQLException e) {
			LOG.error("getConn Exception)");
			e.printStackTrace();
		}
		return con;
	}

	public void disconnect(Connection conn) {
		try {
			conn.close();
		} catch (SQLException e) {
			LOG.error("disConn Exception)");
			printSQLException(e);
		}
	}

	public int getTotalHostNo(Connection con) {
		Statement stmt;
		int NoOfHost = 0;
		try {
			String sql = "SELECT DISTINCT MAX(HOST_NO) FROM HOST_INFOS ";
			stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next()) {
				NoOfHost = rs.getInt(1);
				LOG.info("Total hosts=" + NoOfHost);
			}
			rs.close();
			stmt.close();
		} catch (SQLException e) {
			printSQLException(e);
		}
		return NoOfHost;
	}

	public ArrayList<String> getHostsMT(Connection con, int seq, int Total,
			String sql) {
		Statement stmt;
		ArrayList<String> hostList = new ArrayList<String>();
		try {
			int NoOfHost = getTotalHostNo(con);
			int sliceTerm = (int) Math.ceil(NoOfHost / (Total * 1.0));
			LOG.info("GAP=>" + sliceTerm);
			int sliceStart = 0;
			int sliceEnd = 0;
			sliceStart = sliceStart + sliceTerm * seq;
			sliceEnd = sliceStart + sliceTerm - 1;
			LOG.info(seq + ":" + sliceStart + "~" + sliceEnd);

			Random random = new Random();
			boolean isAsc = random.nextBoolean();
			sql = sql + " AND HOST_NO > " + sliceStart + " AND HOST_NO <"
					+ sliceEnd + " ORDER BY HOSTNAME ";
			if (!isAsc) {
				sql = sql + " DESC ";
			}

			LOG.info(sql);
			stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next()) {
				String host = rs.getString("HOSTNAME");
				LOG.info(seq + ":hostname:" + host);
				hostList.add(host);
			}
			rs.close();
			stmt.close();
		} catch (SQLException e) {
			printSQLException(e);
		}
		return hostList;
	}

	public ArrayList<String> getHostsTest() {
		ArrayList<String> host = new ArrayList<String>();
		host.add("localhost.localdomain");
		return host;
	}

	public void setWorkingTimestamp(Connection conR, String dbURL, int thNo) {
		PreparedStatement pst = null;
		try {
			conR.setAutoCommit(false);

			String sqlLastUpdateTime = null;

			if (dbURL.startsWith("jdbc:postgresql:")) {
				sqlLastUpdateTime = "UPDATE MANAGER_SERVICE_HEALTH_CHECK SET LAST_UPDATED_TIME=NOW() WHERE SERVICE_NAME='metric"
						+ thNo + "'"; // pgsql
			} else if (dbURL.startsWith("jdbc:oracle:")) {
				sqlLastUpdateTime = "UPDATE MANAGER_SERVICE_HEALTH_CHECK SET LAST_UPDATED_TIME=SYSDATE WHERE SERVICE_NAME='metric"
						+ thNo + "'";// oracle
			} else {
				LOG.fatal("Can't find right JDBC. please check you config.xml");
				System.exit(0);
			}

			LOG.trace(sqlLastUpdateTime);
			pst = conR.prepareStatement(sqlLastUpdateTime);
			pst.executeUpdate();
			conR.commit();
			pst.close();
		} catch (SQLException sqle) {
			printSQLException(sqle);
		} catch (ArrayIndexOutOfBoundsException e) {

		} finally {
			try {
				if (pst != null)
					pst.close();
			} catch (SQLException e) {
				printSQLException(e);
			}
			pst = null;
		}
	}

	public void getLastCollectionTimeNisCustom(Connection conR,
			ArrayList<String> hosts, HashMap<String, String> hostKVtime,
			HashMap<String, Boolean> hostKVisCustom, String customCategory1st,
			Long baseMinusSecFrConf, HashMap<String, Boolean> isV3) {
		Statement stmt;
		try {
			for (String host : hosts) {
				String sql = "SELECT DISTINCT METRIC_LAST_TIME,CATEGORY_1ST FROM HOST_INFOS "
						+ "WHERE HOSTNAME='" + host + "'";
				// + " AND METRIC_LAST_TIME IS NOT NULL";
				stmt = conR.createStatement();
				ResultSet rs = stmt.executeQuery(sql);
				while (rs.next()) {
					Timestamp tsLastTimeatDB = rs
							.getTimestamp("METRIC_LAST_TIME");
					LOG.info(tsLastTimeatDB);
					if (tsLastTimeatDB == null) {
						Timestamp tsNow = new Timestamp(
								System.currentTimeMillis());
						LOG.info(host + ":METRIC_LAST_TIME IS NULL ");
						tsLastTimeatDB = new Timestamp(
								System.currentTimeMillis());
						tsLastTimeatDB.setTime(tsNow.getTime()
								- baseMinusSecFrConf * 1000L);
					}
					Timestamp tsNow = new Timestamp(System.currentTimeMillis());
					long timediff = tsNow.getTime() - tsLastTimeatDB.getTime();
					LOG.info("timediff_ms=" + timediff);
					if (timediff > baseMinusSecFrConf * 1000L) {
						tsLastTimeatDB.setTime(tsNow.getTime()
								- baseMinusSecFrConf * 1000L);
						String strCollTime = new SimpleDateFormat(
								"yyyy-MM-dd HH:mm:ss.sss")
								.format(tsLastTimeatDB);
						if (isV3.containsKey(host) && isV3.get(host)) {
							hostKVtime.put(host, tsLastTimeatDB.getTime()
									/ 1000L + "");
							LOG.info(host + "<=" + tsLastTimeatDB.getTime()
									/ 1000L + "");
						} else {
							hostKVtime.put(host, strCollTime);
							LOG.info(host + "<=" + strCollTime);
						}
					} else {
						String strCollTime = new SimpleDateFormat(
								"yyyy-MM-dd HH:mm:ss.sss")
								.format(tsLastTimeatDB);
						if (isV3.containsKey(host) && isV3.get(host)) {
							hostKVtime.put(host, tsLastTimeatDB.getTime()
									/ 1000L + "");
							LOG.info(host + "==" + tsLastTimeatDB.getTime()
									/ 1000L + "");
						} else {
							hostKVtime.put(host, strCollTime);
							LOG.info(host + "==" + strCollTime);
						}
					}

					String cate_1st = rs.getString("CATEGORY_1ST");
					if (cate_1st.matches(customCategory1st)) {
						hostKVisCustom.put(host, true);
					} else {
						hostKVisCustom.put(host, false);
					}
				}
				rs.close();
				stmt.close();
			}
		} catch (SQLException e) {
			printSQLException(e);
		}
	}

	public void setCollectionEndTimestamp(Connection conn, String rdbUrl,
			int thNo, String hostname, long elapsedSecInt) {
		PreparedStatement pst = null;
		try {
			conn.setAutoCommit(false);
			String sqlLastUpdateTime = null;
			sqlLastUpdateTime = "UPDATE HOST_INFOS SET METRIC_LAST_TIME=SYSDATE ,THREAD_NO="
					+ thNo
					+ ", METRIC_ELAPSED_SEC="
					+ elapsedSecInt
					+ " WHERE HOSTNAME='" + hostname + "'";
			LOG.trace(sqlLastUpdateTime);
			pst = conn.prepareStatement(sqlLastUpdateTime);
			pst.executeUpdate();
			conn.commit();
			pst.close();
		} catch (SQLException sqle) {
			printSQLException(sqle);
		} catch (ArrayIndexOutOfBoundsException e) {
			LOG.error(e);
		} finally {
			try {
				if (pst != null)
					pst.close();
			} catch (SQLException e) {
				printSQLException(e);
			}
			pst = null;
		}
	}

	public void setCollectionStartTimestamp(Connection conn, String rdbUrl,
			int thNo, String hostname) {
		PreparedStatement pst = null;
		try {
			conn.setAutoCommit(false);
			String sqlLastUpdateTime = null;
			sqlLastUpdateTime = "UPDATE HOST_INFOS SET METRIC_START_TIME=SYSDATE ,THREAD_NO="
					+ thNo + " WHERE HOSTNAME='" + hostname + "'";
			LOG.trace(sqlLastUpdateTime);
			pst = conn.prepareStatement(sqlLastUpdateTime);
			pst.executeUpdate();
			conn.commit();
			pst.close();
		} catch (SQLException sqle) {
			printSQLException(sqle);
		} catch (ArrayIndexOutOfBoundsException e) {
			LOG.error(e);
		} finally {
			try {
				if (pst != null)
					pst.close();
			} catch (SQLException e) {
				printSQLException(e);
			}
			pst = null;
		}
	}

	public HashMap<String, Boolean> getV3Info(Connection conn) {
		HashMap<String, Boolean> isV3 = new HashMap<String, Boolean>();
		Statement stmt;
		try {
			String sql = "SELECT DISTINCT HOSTNAME,IS_V3 FROM HOST_INFOS WHERE IS_V3=1 ";
			LOG.info(sql);
			;
			stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next()) {
				String host = rs.getString("HOSTNAME");
				isV3.put(host, true);
			}
			rs.close();
			stmt.close();
		} catch (SQLException e) {
			printSQLException(e);
		}
		return isV3;
	}
}