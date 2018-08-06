package util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

public class CDao {
	private static final Logger LOG = LogManager.getLogger(CDao.class);
	Cluster cluster;
	Session session;

	public void connect(String[] seeds) {
		cluster = Cluster.builder().addContactPoints(seeds).build();
		session = cluster.connect("FLOG");
	}

	public void insertSyslog(String host, long epTime, String messages,
			String TTL) {
		PreparedStatement preparedStatement = session
				.prepare("INSERT INTO flog.messages (host,time,message) values ('"
						+ host + "'," + epTime + ",?) USING TTL " + TTL);
		// BatchStatement batch = new BatchStatement();
		BoundStatement boundStatement = new BoundStatement(preparedStatement);
		session.execute(boundStatement.bind(messages));

	}

	public long getEpTime(String strdate) {
		try {
			LOG.info(strdate);
			SimpleDateFormat formatter = new SimpleDateFormat(
					"yyyy-MM-dd HH:mm:ss.sss", Locale.ENGLISH);
			Date date = formatter.parse(strdate);
			long epoch = date.getTime() / 1000;
			return epoch;
		} catch (ParseException e) {
			e.printStackTrace();
			return -1;
		}
	}

	public boolean insertMetric(String host, String message, String TTL) {
		LOG.info("input=" + message + ":");

		// CPU
		try {
			String[] linesCPU = message.split("-FLOG-CPU-");
			String cpuLines = linesCPU[0];
			String[] cpuLine = cpuLines.split("\n");
			for (String cpuOneLine : cpuLine) {
				LOG.info(cpuOneLine);
				try {
					if (cpuOneLine.length() > 10) {
						StringBuffer sbColumn = new StringBuffer("");
						StringBuffer sbValue = new StringBuffer("");

						String[] items = cpuOneLine.split(",,");
						long epTime = getEpTime(items[0]);
						if (items.length <= 0 || epTime < 0) {
							LOG.error("1st Value is not EpochTime in "
									+ cpuLines);
							break;
						}
						if (items.length >= 7) {
							sbColumn.append("HOST,TIME,");
							sbColumn.append("CPU_IRQ,CPU_NICE,CPU_SOFTIRQ,CPU_SYSTEM,CPU_IOWAIT,CPU_USER ");
							sbValue.append("'");
							sbValue.append(host);
							sbValue.append("',");
							sbValue.append(epTime);
							sbValue.append(",");
							sbValue.append(items[1]);
							sbValue.append(",");
							sbValue.append(items[2]);
							sbValue.append(",");
							sbValue.append(items[3]);
							sbValue.append(",");
							sbValue.append(items[4]);
							sbValue.append(",");
							sbValue.append(items[5]);
							sbValue.append(",");
							sbValue.append(items[6]);
							String strColumn = sbColumn.toString();
							String strValue = sbValue.toString();
							String SQL = "INSERT INTO FLOG.CPU (" + strColumn
									+ ") VALUES (" + strValue + ") USING TTL "
									+ TTL + ";";
							LOG.info(SQL);
							PreparedStatement preparedStatement = session
									.prepare(SQL);
							BoundStatement boundStatement = new BoundStatement(
									preparedStatement);
							session.execute(boundStatement);
						}
					}
				} catch (ArrayIndexOutOfBoundsException e) {
					LOG.info("Array Out:" + cpuOneLine);
					return false;
				} catch (NumberFormatException e) {
					LOG.error("NumberFormatException:" + cpuOneLine);
					return false;
				}
			}// CPU

			// CPU_LOAD
			String[] linesCPULOAD = linesCPU[1].split("-FLOG-CPULOAD-");
			String[] cpuLoadLine = cpuLines.split("\n");
			for (String cpuLoadOneLine : cpuLoadLine) {
				LOG.info(cpuLoadOneLine);
				try {
					if (cpuLoadOneLine.length() > 4) {
						StringBuffer sbColumn = new StringBuffer("");
						StringBuffer sbValue = new StringBuffer("");

						String[] items = cpuLoadOneLine.split(",,");
						long epTime = getEpTime(items[0]);
						if (items.length <= 0 || epTime < 0) {
							LOG.error("1st value is not EpochTime in "
									+ cpuLoadOneLine);
							break;
						}
						if (items.length >= 4) {
							sbColumn.append("HOST,TIME,");
							sbColumn.append("LOAD1,LOAD5,LOAD15 ");
							sbValue.append("'");
							sbValue.append(host);
							sbValue.append("',");
							sbValue.append(epTime);
							sbValue.append(",");

							sbValue.append(items[1]);
							sbValue.append(",");
							sbValue.append(items[2]);
							sbValue.append(",");
							sbValue.append(items[3]);
							String strColumn = sbColumn.toString();
							String strValue = sbValue.toString();
							String SQL = "INSERT INTO FLOG.CPU_LOAD ("
									+ strColumn + ") VALUES (" + strValue
									+ ") USING TTL " + TTL + ";";
							LOG.info(SQL);
							PreparedStatement preparedStatement = session
									.prepare(SQL);
							BoundStatement boundStatement = new BoundStatement(
									preparedStatement);
							session.execute(boundStatement);
						}
					}
				} catch (ArrayIndexOutOfBoundsException e) {
					LOG.info("Array Out:" + cpuLoadOneLine);
					return false;
				} catch (NumberFormatException e) {
					LOG.error("NumberFormatException:" + cpuLoadOneLine);
					return false;
				}
			}// CPU_LOAD

			// MEM
			String[] linesMEM = linesCPULOAD[1].split("-FLOG-MEM-");
			String memLines = linesMEM[0];
			String[] memLine = memLines.split("\n");
			for (String memOneLine : memLine) {
				LOG.info(memOneLine);
				try {
					if (memOneLine.length() > 4) {
						StringBuffer sbColumn = new StringBuffer("");
						StringBuffer sbValue = new StringBuffer("");

						String[] items = memOneLine.split(",,");
						long epTime = getEpTime(items[0]);
						if (items.length <= 0 || epTime < 0) {
							LOG.error("1st value is not EpochTime in "
									+ memOneLine);
							break;
						}
						if (items.length >= 7) {
							sbColumn.append("HOST,TIME,");
							sbColumn.append("MEMTOTAL,SWAPTOTAL,MEMFREE,BUFFERS,SWAPFREE,CACHED ");
							sbValue.append("'");
							sbValue.append(host);
							sbValue.append("',");
							sbValue.append(epTime);
							sbValue.append(",");
							sbValue.append(items[1]);
							sbValue.append(",");
							sbValue.append(items[2]);
							sbValue.append(",");
							sbValue.append(items[3]);
							sbValue.append(",");
							sbValue.append(items[4]);
							sbValue.append(",");
							sbValue.append(items[5]);
							sbValue.append(",");
							sbValue.append(items[6]);
							String strColumn = sbColumn.toString();
							String strValue = sbValue.toString();
							String SQL = "INSERT INTO FLOG.MEM (" + strColumn
									+ ") VALUES (" + strValue + ") USING TTL "
									+ TTL + ";";
							LOG.info(SQL);
							PreparedStatement preparedStatement = session
									.prepare(SQL);
							BoundStatement boundStatement = new BoundStatement(
									preparedStatement);
							session.execute(boundStatement);
						}
					}
				} catch (ArrayIndexOutOfBoundsException e) {
					LOG.info("Array Out:" + memOneLine);
					return false;
				} catch (NumberFormatException e) {
					LOG.error("NumberFormatException:" + memOneLine);
					return false;
				}
			}// MEM

			// DISK
			String[] linesDISK = linesMEM[1].split("-FLOG-DISK-");
			String diskLines = linesDISK[0];
			String[] diskLine = diskLines.split("\n");
			for (String diskOneLine : diskLine) {
				LOG.info(diskOneLine);
				try {
					if (diskOneLine.length() > 4) {
						StringBuffer sbColumn = new StringBuffer("");
						StringBuffer sbValue = new StringBuffer("");

						String[] items = diskOneLine.split(",,");
						long epTime = getEpTime(items[0]);
						if (items.length <= 0 || epTime < 0) {
							LOG.error("1st value is not EpochTime in "
									+ diskOneLine);
							break;
						}
						if (items.length >= 3) {
							sbColumn.append("HOST,TIME,");
							sbColumn.append("DEV_NAME,CAPACITY,TOTAL_KBYTES,USED_KBYTES ");
							sbValue.append("'");
							sbValue.append(host);
							sbValue.append("',");
							sbValue.append(epTime);
							sbValue.append(",'");
							sbValue.append(items[1]);
							sbValue.append("',");
							sbValue.append(items[2]);
							sbValue.append(",");
							sbValue.append(items[3]);
							sbValue.append(",");
							sbValue.append(items[4]);

							String strColumn = sbColumn.toString();
							String strValue = sbValue.toString();
							String SQL = "INSERT INTO FLOG.DISK (" + strColumn
									+ ") VALUES (" + strValue + ") USING TTL "
									+ TTL + ";";
							LOG.info(SQL);
							PreparedStatement preparedStatement = session
									.prepare(SQL);
							BoundStatement boundStatement = new BoundStatement(
									preparedStatement);
							session.execute(boundStatement);
						}
					}
				} catch (ArrayIndexOutOfBoundsException e) {
					LOG.info("Array Out:" + diskOneLine);
					return false;
				} catch (NumberFormatException e) {
					LOG.error("NumberFormatException:" + diskOneLine);
					return false;
				}
			}// DISK

			// DISK_IO
			String[] linesDISK_IO = linesDISK[1].split("-FLOG-DISK_IO-");
			String disk_ioLines = linesDISK_IO[0];
			String[] disk_ioLine = disk_ioLines.split("\n");
			for (String disk_ioOneLine : disk_ioLine) {
				LOG.info(disk_ioOneLine);
				try {
					if (disk_ioOneLine.length() > 4) {
						StringBuffer sbColumn = new StringBuffer("");
						StringBuffer sbValue = new StringBuffer("");

						String[] items = disk_ioOneLine.split(",,");
						long epTime = getEpTime(items[0]);
						if (items.length <= 0 || epTime < 0) {
							LOG.error("1st value is not EpochTime in "
									+ disk_ioOneLine);
							break;
						}
						if (items.length >= 5) {
							sbColumn.append("HOST,TIME,");
							sbColumn.append("DEV_NAME,KB_READ,KB_READPSEC,KB_WRTN,KB_WRTNPSEC,TPS ");
							sbValue.append("'");
							sbValue.append(host);
							sbValue.append("',");
							sbValue.append(epTime);
							sbValue.append(",'");
							sbValue.append(items[1]);
							sbValue.append("',");
							sbValue.append(items[2]);
							sbValue.append(",");
							sbValue.append(items[3]);
							sbValue.append(",");
							sbValue.append(items[4]);
							sbValue.append(",");
							sbValue.append(items[5]);
							sbValue.append(",");
							sbValue.append(items[6]);

							String strColumn = sbColumn.toString();
							String strValue = sbValue.toString();
							String SQL = "INSERT INTO FLOG.DISK_IO ("
									+ strColumn + ") VALUES (" + strValue
									+ ") USING TTL " + TTL + ";";
							LOG.info(SQL);
							PreparedStatement preparedStatement = session
									.prepare(SQL);
							BoundStatement boundStatement = new BoundStatement(
									preparedStatement);
							session.execute(boundStatement);
						}
					}
				} catch (ArrayIndexOutOfBoundsException e) {
					LOG.info("Array Out:" + disk_ioOneLine);
					return false;
				} catch (NumberFormatException e) {
					LOG.error("NumberFormatException:" + disk_ioOneLine);
					return false;
				}
			}// DISK

			// NETWORK_IO
			String[] linesNETWORK_IO = linesDISK_IO[1].split("-FLOG-NETWORK-");
			String netLines = linesNETWORK_IO[0];
			String[] netLine = netLines.split("\n");
			for (String netOneLine : netLine) {
				LOG.info(netOneLine);
				try {
					if (netOneLine.length() > 4) {
						StringBuffer sbColumn = new StringBuffer("");
						StringBuffer sbValue = new StringBuffer("");

						String[] items = netOneLine.split(",,");
						long epTime = getEpTime(items[0]);
						if (items.length <= 0 || epTime < 0) {
							LOG.error("1st value is not EpochTime in "
									+ netOneLine);
							break;
						}
						if (items.length >= 12) {
							sbColumn.append("HOST,TIME,");
							sbColumn.append("DEV_NAME,RX_ERRS,TX,FRAME,TX_ERRS,COLLS,TX_PACKETS,RX_PACKETS,TX_DROP,RX,RX_DROP ");
							sbValue.append("'");
							sbValue.append(host);
							sbValue.append("',");
							sbValue.append(epTime);
							sbValue.append(",'");
							sbValue.append(items[1]);
							sbValue.append("',");
							sbValue.append(items[2]);
							sbValue.append(",");
							sbValue.append(items[3]);
							sbValue.append(",");
							sbValue.append(items[4]);
							sbValue.append(",");
							sbValue.append(items[5]);
							sbValue.append(",");
							sbValue.append(items[6]);
							sbValue.append(",");
							sbValue.append(items[7]);
							sbValue.append(",");
							sbValue.append(items[8]);
							sbValue.append(",");
							sbValue.append(items[9]);
							sbValue.append(",");
							sbValue.append(items[10]);
							sbValue.append(",");
							sbValue.append(items[11]);
							String strColumn = sbColumn.toString();
							String strValue = sbValue.toString();
							String SQL = "INSERT INTO FLOG.NETWORK_IO ("
									+ strColumn + ") VALUES (" + strValue
									+ ") USING TTL " + TTL + ";";
							LOG.info(SQL);
							PreparedStatement preparedStatement = session
									.prepare(SQL);
							BoundStatement boundStatement = new BoundStatement(
									preparedStatement);
							session.execute(boundStatement);
						}
					}
				} catch (ArrayIndexOutOfBoundsException e) {
					LOG.info("Array Out:" + netOneLine);
					return false;
				} catch (NumberFormatException e) {
					LOG.error("NumberFormatException:" + netOneLine);
					return false;
				}
			}// NETWORK_IO

			// PROCESS_CPU
			String[] linesProcessCPU = linesNETWORK_IO[1]
					.split("-FLOG-PROCESS_CPU-");
			if (linesProcessCPU.length > 0) {
				String processCpuLines = linesProcessCPU[0];
				String[] processCpuLine = processCpuLines.split("\n");
				for (String processCpuOneLine : processCpuLine) {
					LOG.info(processCpuOneLine + ":");
					try {
						if (processCpuOneLine.length() > 4) {
							StringBuffer sbColumn = new StringBuffer("");
							StringBuffer sbValue = new StringBuffer("");

							String[] items = processCpuOneLine.split(",,");
							long epTime = getEpTime(items[0]);
							if (items.length <= 0 || epTime < 0) {
								LOG.error("1st value is not EpochTime in "
										+ processCpuOneLine);
								break;
							}
							if (items.length >= 7) {
								sbColumn.append("HOST,TIME,");
								sbColumn.append("PID,CMD,PCPU,RSS,USER_NAME,VSZ ");
								sbValue.append("'");
								sbValue.append(host);
								sbValue.append("',");
								sbValue.append(epTime);
								sbValue.append(",");
								sbValue.append(items[1]);
								sbValue.append(",'");
								sbValue.append(items[2]);
								sbValue.append("',");
								sbValue.append(items[3]);
								sbValue.append(",");
								sbValue.append(items[4]);
								sbValue.append(",'");
								sbValue.append(items[5]);
								sbValue.append("',");
								sbValue.append(items[6]);

								String strColumn = sbColumn.toString();
								String strValue = sbValue.toString();
								String SQL = "INSERT INTO FLOG.PROCESS_CPU ("
										+ strColumn + ") VALUES (" + strValue
										+ ") USING TTL " + TTL + ";";
								LOG.info(SQL);
								PreparedStatement preparedStatement = session
										.prepare(SQL);
								BoundStatement boundStatement = new BoundStatement(
										preparedStatement);
								session.execute(boundStatement);
							}
						}
					} catch (ArrayIndexOutOfBoundsException e) {
						LOG.info("Array Out:" + processCpuOneLine);
						return false;
					} catch (NumberFormatException e) {
						LOG.error("NumberFormatException:" + processCpuOneLine);
						return false;
					}
				}// PROCESS_CPU

				// NFS
				if (linesProcessCPU.length > 1) { // no nfs info case check
					String nfsLines = linesProcessCPU[1];
					String[] nfsLine = nfsLines.split("\n");
					for (String nfsOneLine : nfsLine) {
						LOG.info(nfsOneLine + ":");
						try {
							if (nfsOneLine.length() > 4) {
								StringBuffer sbColumn = new StringBuffer("");
								StringBuffer sbValue = new StringBuffer("");

								String[] items = nfsOneLine.split(",,");
								long epTime = getEpTime(items[0]);
								if (items.length <= 0 || epTime < 0) {
									LOG.error("1st value is not EpochTime in "
											+ nfsOneLine);
									break;
								}
								if (items.length >= 5) {
									sbColumn.append("HOST,TIME,");
									sbColumn.append("FILER_NAME,VALUE,DATA_TYPE,USER_ID ");
									sbValue.append("'");
									sbValue.append(host);
									sbValue.append("',");
									sbValue.append(epTime);
									sbValue.append(",'");
									sbValue.append(items[1]);
									sbValue.append("',");
									sbValue.append(items[2]);
									sbValue.append(",'");
									sbValue.append(items[3]);
									sbValue.append("','");
									sbValue.append(items[4]);
									sbValue.append("'");

									String strColumn = sbColumn.toString();
									String strValue = sbValue.toString();
									String SQL = "INSERT INTO FLOG.NFS ("
											+ strColumn + ") VALUES ("
											+ strValue + ") USING TTL " + TTL
											+ ";";
									LOG.info(SQL);
									PreparedStatement preparedStatement = session
											.prepare(SQL);
									BoundStatement boundStatement = new BoundStatement(
											preparedStatement);
									session.execute(boundStatement);
								} else {
									LOG.info("NFS_ITEM=" + items.length);
								}
							}
						} catch (ArrayIndexOutOfBoundsException e) {
							LOG.info("Array Out:" + nfsOneLine);
							return false;
						} catch (NumberFormatException e) {
							LOG.error("NumberFormatException:" + nfsOneLine);
							return false;
						}
					}
				}// NFS
			}
			return true;
		} catch (ArrayIndexOutOfBoundsException e) {
			LOG.info("Array Out :" + message);
			return false;
		}
	}

	public void disconnect() {
		session.close();
		cluster.close();
	}
}