package util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ADao {
	private static final Logger LOG = LogManager.getLogger(ADao.class);
	Connection conn = null;
	String protocol = null;

	public boolean isWorking() {
		return true;
	}

	public static String printSQLException(SQLException e) {
		LOG.error("\n----- SQLException -----");
		LOG.error("  SQL State:  " + e.getSQLState());
		LOG.error("  Error Code: " + e.getErrorCode());
		LOG.error("  Message:    " + e.getMessage());
		if (e.getMessage().contains("Table/View")
				|| e.getMessage().contains(" does not exist.")) {
			LOG.fatal(e.getMessage());
			LOG.error("error__NoTable");
			return null;
		}
		if (e.getMessage().contains("Error connecting to server")) {
			LOG.info(e.getMessage());
			LOG.error("error__connection");
			return null;
		}
		LOG.error("error__unknown");
		return null;
	}

	public String getMetricsNT(int port, String host, String strTime,
			boolean custom) {

		Properties props = new Properties();
		props.put("user", "agent");
		props.put("password", "catallena7");

		StringBuffer sb = new StringBuffer("");
		if (host.matches("localhost.localdomain")) {// TEST
			host = "192.168.178.131";
		}
		protocol = "jdbc:derby://" + host + ":" + port + "/";

		PreparedStatement pst = null;
		ResultSet rs = null;
		Statement s = null;

		LOG.info("protocol=" + protocol);

		try {
			conn = DriverManager.getConnection(
					protocol + "derbyDB;create=true", props);
			DriverManager.setLoginTimeout(1);

			s = conn.createStatement();
			
			// CPU
			String sql = "SELECT TIME,CPU_IRQ,CPU_NICE,CPU_SOFTIRQ,CPU_SYSTEM,CPU_IOWAIT,CPU_USER "
					+ "FROM CPU WHERE TIME>=TIMESTAMP('" + strTime + "') ";
			LOG.trace(host + ":CPU=" + sql);
			rs = s.executeQuery(sql);
			while (rs.next()) {
				LOG.info("SB1=" + sb);
				sb.append(rs.getTimestamp("TIME"));
				sb.append(",,");
				sb.append(rs.getFloat("CPU_IRQ"));
				sb.append(",,");
				sb.append(rs.getFloat("CPU_NICE"));
				sb.append(",,");
				sb.append(rs.getFloat("CPU_SOFTIRQ"));
				sb.append(",,");
				sb.append(rs.getFloat("CPU_SYSTEM"));
				sb.append(",,");
				sb.append(rs.getFloat("CPU_IOWAIT"));
				sb.append(",,");
				sb.append(rs.getFloat("CPU_USER"));
				sb.append("\n");
			}
			sb.append("-FLOG-CPU-");

			// CPU-LOAD
			sql = "SELECT TIME,LOAD1,LOAD5,LOAD15 "
					+ "FROM CPU_LOAD WHERE TIME>=TIMESTAMP('" + strTime + "') ";
			LOG.trace(host + ":CPU_LOAD=" + sql);
			rs = s.executeQuery(sql);
			while (rs.next()) {
				sb.append(rs.getTimestamp("TIME"));
				sb.append(",,");
				sb.append(rs.getFloat("LOAD1"));
				sb.append(",,");
				sb.append(rs.getFloat("LOAD5"));
				sb.append(",,");
				sb.append(rs.getFloat("LOAD15"));
				sb.append("\n");
			}
			sb.append("-FLOG-CPULOAD-");

			// MEM
			sql = "SELECT TIME,MEMTOTAL,SWAPTOTAL,MEMFREE,BUFFERS,SWAPFREE,CACHED "
					+ "FROM MEM WHERE TIME>=TIMESTAMP('" + strTime + "') ";
			LOG.trace(host + ":MEM=" + sql);
			rs = s.executeQuery(sql);
			while (rs.next()) {
				sb.append(rs.getTimestamp("TIME"));
				sb.append(",,");
				sb.append(rs.getFloat("MEMTOTAL"));
				sb.append(",,");
				sb.append(rs.getFloat("SWAPTOTAL"));
				sb.append(",,");
				sb.append(rs.getFloat("MEMFREE"));
				sb.append(",,");
				sb.append(rs.getFloat("BUFFERS"));
				sb.append(",,");
				sb.append(rs.getFloat("SWAPFREE"));
				sb.append(",,");
				sb.append(rs.getFloat("CACHED"));
				sb.append("\n");
			}
			sb.append("-FLOG-MEM-");

			// DISK
			sql = "SELECT TIME,DEV_NAME,CAPACITY,TOTAL_KBYTES,USED_KBYTES "
					+ "FROM DISK WHERE TIME>=TIMESTAMP('" + strTime + "') ";
			LOG.trace(host + ":DISK=" + sql);
			rs = s.executeQuery(sql);
			while (rs.next()) {
				sb.append(rs.getTimestamp("TIME"));
				sb.append(",,");
				sb.append(rs.getString("DEV_NAME"));
				sb.append(",,");
				sb.append(rs.getFloat("CAPACITY"));
				sb.append(",,");
				sb.append(rs.getFloat("TOTAL_KBYTES"));
				sb.append(",,");
				sb.append(rs.getFloat("USED_KBYTES"));
				sb.append("\n");
			}
			sb.append("-FLOG-DISK-");

			// DISK_IO
			sql = "SELECT TIME,DEV_NAME,KB_READ,KB_READPSEC,KB_WRTN,KB_WRTNPSEC,TPS "
					+ "FROM DISK_IO WHERE TIME>=TIMESTAMP('" + strTime + "') ";
			LOG.trace(host + ":DISK_IO=" + sql);
			rs = s.executeQuery(sql);
			while (rs.next()) {
				sb.append(rs.getTimestamp("TIME"));
				sb.append(",,");
				sb.append(rs.getString("DEV_NAME"));
				sb.append(",,");
				sb.append(rs.getFloat("KB_READ"));
				sb.append(",,");
				sb.append(rs.getFloat("KB_READPSEC"));
				sb.append(",,");
				sb.append(rs.getFloat("KB_WRTN"));
				sb.append(",,");
				sb.append(rs.getFloat("KB_WRTNPSEC"));
				sb.append(",,");
				sb.append(rs.getFloat("TPS"));
				sb.append("\n");
			}
			sb.append("-FLOG-DISK_IO-");

			// NETWORK
			sql = "SELECT TIME,DEV_NAME,RX_ERRS,TX,FRAME,TX_ERRS,COLLS,TX_PACKETS,RX_PACKETS,TX_DROP,RX,RX_DROP "
					+ "FROM NETWORK_IO WHERE TIME>=TIMESTAMP('"
					+ strTime
					+ "') ";
			LOG.trace(host + ":NETWORK_IO=" + sql);
			rs = s.executeQuery(sql);
			while (rs.next()) {
				sb.append(rs.getTimestamp("TIME"));
				sb.append(",,");
				sb.append(rs.getString("DEV_NAME"));
				sb.append(",,");
				sb.append(rs.getFloat("RX_ERRS"));
				sb.append(",,");
				sb.append(rs.getFloat("TX"));
				sb.append(",,");
				sb.append(rs.getFloat("FRAME"));
				sb.append(",,");
				sb.append(rs.getFloat("TX_ERRS"));
				sb.append(",,");
				sb.append(rs.getFloat("COLLS"));
				sb.append(",,");
				sb.append(rs.getFloat("TX_PACKETS"));
				sb.append(",,");
				sb.append(rs.getFloat("RX_PACKETS"));
				sb.append(",,");
				sb.append(rs.getFloat("TX_DROP"));
				sb.append(",,");
				sb.append(rs.getFloat("RX"));
				sb.append(",,");
				sb.append(rs.getFloat("RX_DROP"));
				sb.append("\n");
			}
			sb.append("-FLOG-NETWORK-");

			// PROCESS
			sql = "SELECT TIME,PID,CMD,PCPU,RSS,USER_NAME,VSZ "
					+ "FROM PROCESS_CPUINFO WHERE TIME>=TIMESTAMP('" + strTime
					+ "') ";
			LOG.trace(host + ":PROCESS_CPU=" + sql);
			rs = s.executeQuery(sql);
			while (rs.next()) {
				sb.append(rs.getTimestamp("TIME"));
				sb.append(",,");
				sb.append(rs.getString("PID"));
				sb.append(",,");
				sb.append(rs.getString("CMD"));
				sb.append(",,");
				sb.append(rs.getFloat("PCPU"));
				sb.append(",,");
				sb.append(rs.getFloat("RSS"));
				sb.append(",,");
				sb.append(rs.getString("USER_NAME"));
				sb.append(",,");
				sb.append(rs.getFloat("VSZ"));
				sb.append("\n");
			}
			sb.append("-FLOG-PROCESS_CPU-");

			// NFS
			if (custom == false) {
				sql = "SELECT TIME,FILER_NAME,VALUE,DATA_TYPE,USER_ID "
						+ "FROM NFS WHERE TIME>=TIMESTAMP('" + strTime + "') ";
				LOG.trace(host + ":NFS=" + sql);
				rs = s.executeQuery(sql);
				while (rs.next()) {
					sb.append(rs.getTimestamp("TIME"));
					sb.append(",,");
					sb.append(rs.getString("FILER_NAME"));
					sb.append(",,");
					sb.append(rs.getFloat("VALUE"));
					sb.append(",,");
					sb.append(rs.getString("DATA_TYPE"));
					sb.append(",,");
					sb.append(rs.getString("USER_ID"));
					sb.append("\n");
				}
			} else {
				LOG.info("skip_nfs:" + host);
			}
			if (sb.length() > 0) {
				LOG.info(host + ":res=" + sb);
			}
		} catch (SQLException sqle) {
			printSQLException(sqle);
			return null;
		} finally {
			try {
				if (pst != null) {
					pst.close();
					pst = null;
				}
				if (conn != null) {
					conn.close();
					conn = null;
					return sb.toString();
				}
			} catch (SQLException e) {
				printSQLException(e);
				return null;
			}
		}
		return sb.toString();
	}
}