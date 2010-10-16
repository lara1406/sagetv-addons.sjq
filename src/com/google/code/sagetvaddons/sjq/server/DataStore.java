/*
 *      Copyright 2010 Battams, Derek
 *       
 *       Licensed under the Apache License, Version 2.0 (the "License");
 *       you may not use this file except in compliance with the License.
 *       You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *       Unless required by applicable law or agreed to in writing, software
 *       distributed under the License is distributed on an "AS IS" BASIS,
 *       WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *       See the License for the specific language governing permissions and
 *       limitations under the License.
 */
package com.google.code.sagetvaddons.sjq.server;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import sagex.api.Configuration;
import sagex.api.Global;

import com.google.code.sagetvaddons.sjq.server.TaskQueue.PendingTask;
import com.google.code.sagetvaddons.sjq.shared.Client;
import com.google.code.sagetvaddons.sjq.shared.QueuedTask;
import com.google.code.sagetvaddons.sjq.shared.Task;


/**
 * @author dbattams
 *
 */
public final class DataStore {
	static private final Logger LOG = Logger.getLogger(DataStore.class);
	static private final ThreadLocal<DataStore> POOL = new ThreadLocal<DataStore>() {
		@Override
		public DataStore initialValue() {
			try {
				return new DataStore();
			} catch (ClassNotFoundException e) {
				LOG.fatal("H2 JDBC driver is missing!", e);
			} catch (SQLException e) {
				LOG.fatal(SQL_ERROR, e);
			}
			return null;
		}
	};

	static public final DataStore get() {
		DataStore ds = POOL.get();
		int retries = 5;
		while(retries-- > 0 && !ds.isValid())
			ds = POOL.get();
		return ds;
	}

	static private final String SQL_ERROR = "SQL Error";
	static private final String JDBC_URL= "jdbc:h2:tcp://" + Global.GetServerAddress() + ":" + Configuration.GetServerProperty("h2/tcp_port", "9092") + "/plugins/sjq/sjq4";
	static private final String READ_SETTING = "ReadSetting";
	static private final String READ_CLIENT = "ReadClient";
	static private final String READ_ALL_CLIENTS = "ReadAllClnts";
	static private final String ADD_CLIENT = "AddClnt";
	static private final String UPDATE_CLIENT = "UpdtClnt";
	static private final String DELETE_CLIENT = "DelClnt";
	static private final String WIPE_CLNT_TASKS = "WipeClntTasks";
	static private final String ADD_CLNT_TASK = "AddClntTask";
	static private final String ADD_TASK = "AddTask";
	static private final String DELETE_METADATA = "DelMetadata";
	static private final String ADD_METADATA = "AddMetadata";
	static private final String CLIENT_FOR_TASK = "ClntForTask";
	static private final String GET_USED_RESOURCES = "GetUsedRes";
	static private final String GET_ACTIVE_INSTS = "GetActiveInstances";
	static private final String GET_METADATA = "GetMetadata";
	static private final String UPDATE_QUEUE = "UpdateQueue";
	static private final String UPDATE_LOG = "UpdateLog";
	static private final String COUNT_LOG = "CountLog";
	static private final String ADD_LOG = "AddLog";
	static private final String READ_LOG = "ReadLog";
	static private final String READ_CLNT_TASKS = "ReadClntTasks";
	
	static private boolean dbInitialized = false;

	private Connection conn;
	private Map<String, PreparedStatement> stmts;

	private DataStore() throws ClassNotFoundException, SQLException {
		Class.forName("org.h2.Driver");
		conn = DriverManager.getConnection(JDBC_URL, "admin", "admin");
		synchronized(DataStore.class) {
			if(!dbInitialized) {
				conn.setAutoCommit(false);
				try {
					initDb();
					upgradeDb();
					conn.commit();
				} catch(SQLException e) {
					conn.rollback();
					throw(e);
				} finally {
					conn.setAutoCommit(true);
				}
				dbInitialized = true;
			}
		}
		prepStatements();
	}

	@Override
	protected void finalize() {
		try {
			close();
		} finally {
			try { super.finalize(); } catch(Throwable t) { LOG.error("FinalizeError", t); }
		}		
	}

	private void initDb() throws SQLException {
		Statement s = conn.createStatement();
		String qry = "CREATE TABLE IF NOT EXISTS settings (var VARCHAR(128) NOT NULL PRIMARY KEY, val LONGVARCHAR, " +
		"CONSTRAINT IF NOT EXISTS var_not_empty__settings CHECK LENGTH(var) > 0)";
		s.executeUpdate(qry);

		qry = String.format("CREATE TABLE IF NOT EXISTS client (host VARCHAR(512) NOT NULL, port INTEGER NOT NULL, state VARCHAR(64) NOT NULL DEFAULT '%s', schedule VARCHAR(128) NOT NULL DEFAULT '%s', last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP, total_resources TINYINT NOT NULL DEFAULT %d, PRIMARY KEY(host, port), " +
				"CONSTRAINT IF NOT EXISTS port_gt_zero__client CHECK port > 0, " +
				"CONSTRAINT IF NOT EXISTS host_not_empty__client CHECK LENGTH(host) > 0, " +
				"CONSTRAINT IF NOT EXISTS total_res_ge_zero__client CHECK total_resources >= 0)", Client.State.OFFLINE.toString(), Client.DEFAULT_SCHED, Client.DEFAULT_RESOURCES);
		s.executeUpdate(qry);

		qry = String.format("CREATE TABLE IF NOT EXISTS client_tasks (id VARCHAR(128) NOT NULL, host VARCHAR(512) NOT NULL, port INT NOT NULL, reqd_resources TINYINT NOT NULL DEFAULT %d, max_instances TINYINT NOT NULL DEFAULT %d, schedule VARCHAR(256) NOT NULL DEFAULT '%s', exe VARCHAR(255) NOT NULL, args VARCHAR(7936) NOT NULL DEFAULT '', max_time INT NOT NULL DEFAULT %d, max_time_ratio REAL NOT NULL DEFAULT %f, min_rc SMALLINT NOT NULL DEFAULT %d, max_rc SMALLINT NOT NULL DEFAULT %d, test VARCHAR(255), test_args VARCHAR(7936) NOT NULL DEFAULT '', " +
				"PRIMARY KEY (id, host, port), " +
				"CONSTRAINT IF NOT EXISTS fk_client__client_tasks FOREIGN KEY (host, port) REFERENCES client (host, port) ON DELETE CASCADE, " +
				"CONSTRAINT IF NOT EXISTS id_not_empty__client_tasks CHECK LENGTH(id) > 0, " +
				"CONSTRAINT IF NOT EXISTS max_inst_ge_zero__client_tasks CHECK max_instances >= 0, " +
				"CONSTRAINT IF NOT EXISTS req_res_ge_zero__client_tasks CHECK reqd_resources >= 0, " +
				"CONSTRAINT IF NOT EXISTS max_time_ge_zero__client_tasks CHECK max_time >= 0, " +
				"CONSTRAINT IF NOT EXISTS max_time_ratio_ge_zero__client_tasks CHECK max_time_ratio >= 0, " +
				"CONSTRAINT IF NOT EXISTS min_rc_ge_zero__client_tasks CHECK min_rc >= 0, " +
				"CONSTRAINT IF NOT EXISTS max_rc_ge_zero__client_tasks CHECK max_rc >= 0, " +
				"CONSTRAINT IF NOT EXISTS max_rc_ge_min_rc__client_tasks CHECK max_rc >= min_rc)", Task.DEFAULT_REQ_RES, Task.DEFAULT_MAX_INST, Task.DEFAULT_SCHED, Task.DEFAULT_MAX_TIME, Task.DEFAULT_MAX_TIME_RATIO, Task.DEFAULT_MIN_RC, Task.DEFAULT_MAX_RC);
		s.executeUpdate(qry);
		
		qry = "CREATE TABLE IF NOT EXISTS queue (id IDENTITY NOT NULL PRIMARY KEY, job_id VARCHAR(128) NOT NULL, created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(), assigned TIMESTAMP, finished TIMESTAMP, state VARCHAR(64) NOT NULL DEFAULT 'WAITING', host VARCHAR(512), port INT, " +
				"CONSTRAINT IF NOT EXISTS fk_client__queue FOREIGN KEY (host, port) REFERENCES client (host, port) ON DELETE CASCADE, " +
				"CONSTRAINT IF NOT EXISTS state_not_empty__queue CHECK LENGTH(state) > 0)";
		s.executeUpdate(qry);
		
		qry = "CREATE TABLE IF NOT EXISTS queue_metadata (id BIGINT NOT NULL, var VARCHAR(128) NOT NULL, val VARCHAR(2048) NOT NULL, " +
				"PRIMARY KEY(id, var), " +
				"CONSTRAINT IF NOT EXISTS fk_queue__queue_metadata FOREIGN KEY (id) REFERENCES queue (id) ON DELETE CASCADE, " +
				"CONSTRAINT IF NOT EXISTS var_not_empty__queue_metadata CHECK LENGTH(var) > 0)";
		s.executeUpdate(qry);
		
		qry = "CREATE TABLE IF NOT EXISTS task_log (id BIGINT NOT NULL, type VARCHAR(64) NOT NULL, log CLOB, " +
				"PRIMARY KEY(id, type), " +
				"CONSTRAINT IF NOT EXISTS fk_queue__task_log FOREIGN KEY (id) REFERENCES queue (id) ON DELETE CASCADE, " +
				"CONSTRAINT IF NOT EXISTS type_not_empty__task_log CHECK LENGTH(type) > 0)";
		s.executeUpdate(qry);

		qry = "CREATE VIEW IF NOT EXISTS used_res_by_clnt AS SELECT q.host, q.port, SUM(reqd_resources) AS used_resources FROM queue AS q LEFT OUTER JOIN client_tasks AS t ON (q.host = t.host AND q.port = t.port AND q.job_id = t.id) WHERE q.host IS NOT NULL AND q.state = 'RUNNING' GROUP BY (q.host, q.port)";
		s.executeUpdate(qry);
		
		qry = "CREATE VIEW IF NOT EXISTS active_cnt_for_clnt_by_task AS SELECT q.host, q.port, q.job_id, COUNT(q.job_id) AS active FROM queue AS q LEFT OUTER JOIN client_tasks AS t ON (q.host = t.host AND q.port = t.port AND q.job_id = t.id) WHERE q.host IS NOT NULL AND q.state = 'RUNNING' GROUP BY (q.host, q.port, q.job_id)";
		s.executeUpdate(qry);
		
		qry = "CREATE VIEW IF NOT EXISTS active_cnt_by_task AS SELECT job_id, SUM(active) AS active FROM active_cnt_for_clnt_by_task GROUP BY (job_id)"; 
		s.executeUpdate(qry);

		qry = "CREATE VIEW IF NOT EXISTS active_cnt_by_clnt AS SELECT host, port, SUM(active) AS active FROM active_cnt_for_clnt_by_task GROUP BY (host, port)";
		s.executeUpdate(qry);
				
		qry = "INSERT INTO settings (var, val) VALUES ('schema', '1')";
		try {
			s.executeUpdate(qry);
		} catch(SQLException e) {
			if(!e.getMessage().contains("index or primary key violation"))
				throw(e);
		}

		s.close();
	}

	private void upgradeDb() {
		return;
	}

	private void prepStatements() throws SQLException {
		stmts = new HashMap<String, PreparedStatement>();

		String qry = "SELECT val FROM settings WHERE var = ?";
		stmts.put(READ_SETTING, conn.prepareStatement(qry));

		qry = "SELECT state, schedule, last_update, total_resources FROM client WHERE host = ? AND port = ?";
		stmts.put(READ_CLIENT, conn.prepareStatement(qry));

		qry = "SELECT host, port FROM client";
		stmts.put(READ_ALL_CLIENTS, conn.prepareStatement(qry));

		qry = "INSERT INTO client (host, port, state, schedule, last_update, total_resources) VALUES (?, ?, ?, ?, ?, ?)";
		stmts.put(ADD_CLIENT, conn.prepareStatement(qry));

		qry = "UPDATE client SET state = ?, schedule = ?, last_update = ?, total_resources = ? WHERE host = ? AND port = ?";
		stmts.put(UPDATE_CLIENT, conn.prepareStatement(qry));

		qry = "DELETE FROM client WHERE host = ? AND port = ?";
		stmts.put(DELETE_CLIENT, conn.prepareStatement(qry));

		qry = "DELETE FROM client_tasks WHERE host = ? AND port = ?";
		stmts.put(WIPE_CLNT_TASKS, conn.prepareStatement(qry));

		qry = "INSERT INTO client_tasks (host, port, reqd_resources, max_instances, schedule, exe, args, max_time, max_time_ratio, min_rc, max_rc, id, test, test_args) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		stmts.put(ADD_CLNT_TASK, conn.prepareStatement(qry));
		
		qry = "INSERT INTO queue (job_id) VALUES (?)";
		stmts.put(ADD_TASK, conn.prepareStatement(qry));
		
		qry = "DELETE FROM queue_metadata WHERE id = ?";
		stmts.put(DELETE_METADATA, conn.prepareStatement(qry));
		
		qry = "INSERT INTO queue_metadata (id, var, val) VALUES (?, ?, ?)";
		stmts.put(ADD_METADATA, conn.prepareStatement(qry));
		
		qry = "SELECT host, port FROM client_tasks WHERE id = ?";
		stmts.put(CLIENT_FOR_TASK, conn.prepareStatement(qry));
		
		qry = "SELECT used_resources FROM used_res_by_clnt WHERE host = ? AND port = ?";
		stmts.put(GET_USED_RESOURCES, conn.prepareStatement(qry));
		
		qry = "SELECT active FROM active_cnt_for_clnt_by_task WHERE host = ? AND port = ? AND job_id = ?";
		stmts.put(GET_ACTIVE_INSTS, conn.prepareStatement(qry));
		
		qry = "SELECT var, val FROM queue_metadata WHERE id = ?";
		stmts.put(GET_METADATA, conn.prepareStatement(qry));
		
		qry = "UPDATE queue SET assigned = ?, finished = ?, state = ?, host = ?, port = ? WHERE id = ? AND job_id = ?";
		stmts.put(UPDATE_QUEUE, conn.prepareStatement(qry));
		
		qry = "UPDATE task_log SET log = ? WHERE id = ? AND type = ?";
		stmts.put(UPDATE_LOG, conn.prepareStatement(qry));
		
		qry = "SELECT COUNT(*) FROM task_log WHERE id = ? AND type = ?";
		stmts.put(COUNT_LOG, conn.prepareStatement(qry));
		
		qry = "INSERT INTO task_log (id, type, log) VALUES (?, ?, ?)";
		stmts.put(ADD_LOG, conn.prepareStatement(qry));
		
		qry = "SELECT log FROM task_log WHERE id = ? AND type = ?";
		stmts.put(READ_LOG, conn.prepareStatement(qry));
		
		qry = "SELECT t.id, t.reqd_resources, t.max_instances, t.schedule, t.exe, t.args, t.max_time, t.max_time_ratio, t.min_rc, t.max_rc, t.test, t.test_args FROM client AS c LEFT OUTER JOIN client_tasks AS t ON (c.host = t.host AND c.port = t.port) WHERE c.host = ? AND c.port = ?";
		stmts.put(READ_CLNT_TASKS, conn.prepareStatement(qry));
	}

	/**
	 * <p>Determine if this data store connection is still in a valid state (i.e. it's still connected to the database server)</p>
	 * 
	 * <p>If isValid() returns false then the data store connection is removed from the thread local storage such that the next
	 * call to getInstance() will return a new, freshly initiated data store connection for use.</p>
	 * @return True if the connection is still valid or false otherwise
	 */
	public boolean isValid() {
		try {
			boolean retVal = conn.isValid(3);
			if(!retVal)
				POOL.remove();
			return retVal;
		} catch (SQLException e) {
			POOL.remove();
			LOG.error(SQL_ERROR, e);
			return false;
		}
	}

	public int getSchema() {
		PreparedStatement stmt = stmts.get(READ_SETTING);
		ResultSet rs = null;
		try {
			stmt.setString(1, "schema");
			rs = stmt.executeQuery();
			if(rs.next())
				return rs.getInt(1);
			return -1;
		} catch (SQLException e) {
			LOG.error(SQL_ERROR, e);
			return -1;
		} finally {
			try {
				if(rs != null)
					rs.close();
			} catch(SQLException e) {
				LOG.error(SQL_ERROR, e);
			}
		}	
	}

	Task[] getTasksForClient(Client c) {
		return getTasksForClient(c.getHost(), c.getPort());
	}
	
	Task[] getTasksForClient(String host, int port) {
		if(host == null || host.length() == 0 || port < 1)
			throw new IllegalArgumentException("Client keys are invalid!");
		PreparedStatement pStmt = stmts.get(READ_CLNT_TASKS);
		ResultSet rs = null;
		Collection<Task> tasks = new ArrayList<Task>();
		try {
			pStmt.setString(1, host);
			pStmt.setInt(2, port);
			rs = pStmt.executeQuery();
			while(rs.next())
				tasks.add(new Task(rs.getString(1), rs.getInt(2), rs.getInt(3), rs.getString(4), rs.getString(5), rs.getString(6), rs.getLong(7), rs.getFloat(8), rs.getInt(9), rs.getInt(10), rs.getString(11), rs.getString(12)));
			return tasks.toArray(new Task[tasks.size()]);
		} catch(SQLException e) {
			LOG.error(SQL_ERROR, e);
			return new Task[0];
		} finally {
			if(rs != null)
				try { rs.close(); } catch(SQLException e) { LOG.warn(SQL_ERROR, e); }
		}
	}
	
	Client[] getClientsForTask(String taskId) {
		PreparedStatement pStmt = stmts.get(CLIENT_FOR_TASK);
		ResultSet rs = null;
		Collection<Client> clnts = new ArrayList<Client>();
		try {
			pStmt.setString(1, taskId);
			rs = pStmt.executeQuery();
			while(rs.next())
				clnts.add(getClient(rs.getString(1), rs.getInt(2)));
			return clnts.toArray(new Client[clnts.size()]);
		} catch(SQLException e) {
			LOG.error(SQL_ERROR, e);
			return new Client[0];
		} finally {
			if(rs != null)
				try { rs.close(); } catch(SQLException e) { LOG.warn(SQL_ERROR, e); }
		}
	}
	
	public QueuedTask[] getActiveQueue() {
		Collection<QueuedTask> tasks = new ArrayList<QueuedTask>();
		String qry = "SELECT q.id, q.job_id, created, assigned, finished, state, reqd_resources, max_instances, schedule, exe, args, max_time, max_time_ratio, min_rc, max_rc, test, test_args, q.host, q.port FROM queue AS q LEFT OUTER JOIN client_tasks AS t ON (q.job_id = t.id AND q.host = t.host AND q.port = t.port) WHERE state NOT IN ('COMPLETED', 'FAILED', 'SKIPPED')";
		Statement stmt = null;
		ResultSet rs = null;
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery(qry);
			while(rs.next())
				tasks.add(new QueuedTask(rs.getLong(1), rs.getString(2), rs.getInt(7), rs.getInt(8), rs.getString(9), rs.getString(10), rs.getString(11), rs.getLong(12), rs.getFloat(13), rs.getInt(14), rs.getInt(15), getMetadata(rs.getLong(1)), rs.getTimestamp(3), rs.getTimestamp(4), rs.getTimestamp(5), QueuedTask.State.valueOf(rs.getString(6)), getClient(rs.getString(18), rs.getInt(19)), "localhost", -1, rs.getString(16), rs.getString(17), 1098));
			return tasks.toArray(new QueuedTask[tasks.size()]);
		} catch(SQLException e) {
			LOG.error(SQL_ERROR, e);
			return new QueuedTask[0];
		} finally {
			try {
				if(rs != null)
					rs.close();
				if(stmt != null)
					stmt.close();
			} catch(SQLException e) {
				LOG.warn(SQL_ERROR, e);
			}
		}
	}
	
	PendingTask[] getPendingTasks() {
		String qry = "SELECT id, job_id, created FROM queue WHERE state = 'WAITING' OR state = 'RETURNED'";
		Collection<PendingTask> tasks = new ArrayList<PendingTask>();
		Statement stmt = null;
		ResultSet rs = null;
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery(qry);
			while(rs.next())
				tasks.add(new PendingTask(rs.getLong(1), rs.getString(2), rs.getTimestamp(3)));
			return tasks.toArray(new PendingTask[tasks.size()]);
		} catch(SQLException e) {
			LOG.error(SQL_ERROR, e);
			return new PendingTask[0];
		} finally {
			try {
				if(rs != null)
					rs.close();
				if(stmt != null)
					stmt.close();
			} catch(SQLException e) {
				LOG.warn(SQL_ERROR, e);
			}
		}
	}
	
	boolean updateTask(QueuedTask qt) {
		PreparedStatement pStmt = stmts.get(UPDATE_QUEUE);
		try {
			pStmt.setTimestamp(1, new Timestamp(qt.getStarted().getTime()));
			Date completed = qt.getCompleted();
			pStmt.setTimestamp(2, completed != null ? new Timestamp(completed.getTime()) : null);
			pStmt.setString(3, qt.getState().toString());
			pStmt.setString(4, qt.getAssignee().getHost());
			pStmt.setInt(5, qt.getAssignee().getPort());
			pStmt.setLong(6, qt.getQueueId());
			pStmt.setString(7, qt.getId());
			pStmt.executeUpdate();
			return true;
		} catch(SQLException e) {
			LOG.error(SQL_ERROR, e);
			return false;
		}
	}
	
	long addTask(String taskId, Map<String, String> metadataSrc) throws SQLException {
		taskId = taskId.toUpperCase();
		boolean localTransaction = false;
		try {
			localTransaction = conn.getAutoCommit();
			if(localTransaction)
				conn.setAutoCommit(false);
			long qId = createTask(taskId);
			setMetadata(qId, metadataSrc);
			if(localTransaction)
				conn.commit();
			return qId;
		} catch (SQLException e) {
			if(localTransaction)
				try { conn.rollback(); } catch(SQLException e1) { LOG.fatal("RollbackError", e1); }
			throw e;
		} finally {
			if(localTransaction)
				try { conn.setAutoCommit(true); } catch(SQLException e) { LOG.fatal("AutoCommitError", e); }
		}
	}
	
	private void setMetadata(long id, Map<String, String> data) throws SQLException {
		PreparedStatement pStmt = stmts.get(DELETE_METADATA);
		pStmt.setLong(1, id);
		pStmt.executeUpdate();
		
		if(data.keySet().size() > 0) {
			pStmt = stmts.get(ADD_METADATA);
			pStmt.setLong(1, id);
			for(String k : data.keySet()) {
				pStmt.setString(2, k);
				pStmt.setString(3, data.get(k));
				pStmt.addBatch();
			}
			pStmt.executeBatch();
		}
	}
	
	private long createTask(String taskId) throws SQLException {
		PreparedStatement pStmt = stmts.get(ADD_TASK);
		ResultSet rs = null;
		try {
			pStmt.setString(1, taskId);
			pStmt.executeUpdate();
			rs = pStmt.getGeneratedKeys();
			if(rs.next())
				return rs.getLong(1);
			else
				throw new SQLException("Unable to obtain new queue entry id!");
		} finally {
			if(rs != null)
				try { rs.close(); } catch(SQLException e) { LOG.warn(SQL_ERROR, e); }
		}
	}
	
	private void updateClientTasks(Client clnt) throws SQLException {
		PreparedStatement pStmt = stmts.get(WIPE_CLNT_TASKS);
		pStmt.setString(1, clnt.getHost());
		pStmt.setInt(2, clnt.getPort());
		pStmt.executeUpdate();

		if(clnt.getTasks().length > 0) {
			pStmt = stmts.get(ADD_CLNT_TASK);
			pStmt.setString(1, clnt.getHost());
			pStmt.setInt(2, clnt.getPort());
			for(Task t : clnt.getTasks()) {
				pStmt.setInt(3, t.getRequiredResources());
				pStmt.setInt(4, t.getMaxInstances());
				pStmt.setString(5, t.getSchedule());
				pStmt.setString(6, t.getExecutable());
				pStmt.setString(7, t.getExeArguments());
				pStmt.setLong(8, t.getMaxTime());
				pStmt.setFloat(9, t.getMaxTimeRatio());
				pStmt.setInt(10, t.getMinReturnCode());
				pStmt.setInt(11, t.getMaxReturnCode());
				pStmt.setString(12, t.getId());
				pStmt.setString(13, t.getTest());
				pStmt.setString(14, t.getTestArgs());
				pStmt.addBatch();
			}
			pStmt.executeBatch();
		}
	}

	public boolean logOutput(QueuedTask qt, String type, String log) {
		ResultSet rs = null;
		PreparedStatement pStmt = stmts.get(COUNT_LOG);
		try {
			pStmt.setLong(1, qt.getQueueId());
			pStmt.setString(2, type);
			rs = pStmt.executeQuery();
			rs.next();
			if(rs.getInt(1) == 1)
				return updateLog(qt.getQueueId(), type, log);
			else
				return addLog(qt.getQueueId(), type, log);
		} catch(SQLException e) {
			LOG.error(SQL_ERROR, e);
			return false;
		} finally {
			if(rs != null)
				try { rs.close(); } catch(SQLException e) { LOG.warn(SQL_ERROR, e); }
		}
	}
	
	private boolean updateLog(long id, String type, String log) {
		PreparedStatement pStmt = stmts.get(READ_LOG);
		ResultSet rs = null;
		Reader r = null;
		try {
			pStmt.setLong(1, id);
			pStmt.setString(2, type);
			rs = pStmt.executeQuery();
			StringBuilder currentLog = new StringBuilder();
			if(rs.next()) {
				r = rs.getCharacterStream(1);
				currentLog.append(IOUtils.toString(r));
				r.close();
			}
			rs.close();
			currentLog.append("\n\n===== " + new Date().toString() + " =====\n\n" + log.concat("\n\n=============================="));
			pStmt = stmts.get(UPDATE_LOG);
			pStmt.setCharacterStream(1, new StringReader(currentLog.toString()));
			pStmt.setLong(2, id);
			pStmt.setString(3, type);
			pStmt.executeUpdate();
			return true;
		} catch(SQLException e) {
			LOG.error(SQL_ERROR, e);
			return false;
		} catch (IOException e) {
			LOG.error("IOError", e);
			return false;
		} finally {
			if(rs != null)
				try { rs.close(); } catch(SQLException e) { LOG.warn(SQL_ERROR, e); }
			if(r != null)
				try { r.close(); } catch(IOException e) { LOG.warn("IOError", e); }
		}
	}
	
	private boolean addLog(long id, String type, String log) {
		PreparedStatement pStmt = stmts.get(ADD_LOG);
		log = "===== " + new Date().toString() + " =====\n\n" + log.concat("\n\n==============================");
		try {
			pStmt.setLong(1, id);
			pStmt.setString(2, type);
			pStmt.setCharacterStream(3, new StringReader(log));
			pStmt.executeUpdate();
			return true;
		} catch(SQLException e) {
			LOG.error(SQL_ERROR, e);
			return false;
		}
	}
	
	public boolean saveClient(Client clnt) {
		if(getClient(clnt.getHost(), clnt.getPort()) != null)
			return updateClient(clnt);
		else {
			boolean localTransaction = false;
			PreparedStatement pStmt = stmts.get(ADD_CLIENT);
			try {
				localTransaction = conn.getAutoCommit();
				if(localTransaction)
					conn.setAutoCommit(false);
				pStmt.setString(1, clnt.getHost());
				pStmt.setInt(2, clnt.getPort());
				pStmt.setString(3, clnt.getState().toString());
				pStmt.setString(4, clnt.getSchedule());
				pStmt.setTimestamp(5, new java.sql.Timestamp(clnt.getLastUpdate().getTime()));
				pStmt.setInt(6, clnt.getMaxResources());
				pStmt.executeUpdate();
				updateClientTasks(clnt);
				if(localTransaction)
					conn.commit();
				return true;
			} catch(SQLException e) {
				LOG.error(SQL_ERROR, e);
				if(localTransaction) {
					try {
						conn.rollback();
					} catch (SQLException e1) {
						LOG.fatal("RollbackFailure", e1);
					}
				}
				return false;
			} finally {
				if(localTransaction) {
					try {
						conn.setAutoCommit(true);
					} catch (SQLException e) {
						LOG.fatal(SQL_ERROR, e);
					}
				}
			}
		}
	}

	public Client getClient(String host, int port) {
		PreparedStatement pStmt = stmts.get(READ_CLIENT);
		ResultSet rs = null;
		try {
			pStmt.setString(1, host);
			pStmt.setInt(2, port);
			rs = pStmt.executeQuery();
			if(rs.next()) {
				Client c = new Client(host, port, 0, rs.getString(2), Client.State.valueOf(rs.getString(1)), rs.getTimestamp(3), rs.getInt(4), getTasksForClient(host, port));
				c.setFreeResources(getFreeResources(c));
				return c;
			}
			return null;
		} catch(SQLException e) {
			LOG.error(SQL_ERROR, e);
			return null;
		} finally {
			if(rs != null)
				try { rs.close(); } catch(SQLException e) { LOG.error(SQL_ERROR, e); }
		}
	}

	public Client[] getAllClients() {
		PreparedStatement pStmt = stmts.get(READ_ALL_CLIENTS);
		ResultSet rs = null;
		Collection<Client> clnts = new ArrayList<Client>();
		try {
			rs = pStmt.executeQuery();
			while(rs.next())
				clnts.add(getClient(rs.getString(1), rs.getInt(2)));
			return clnts.toArray(new Client[clnts.size()]);
		} catch(SQLException e) {
			LOG.error(SQL_ERROR, e);
			return new Client[0];
		}
	}

	int getActiveInstances(String taskId, Client c) {
		PreparedStatement pStmt = stmts.get(GET_ACTIVE_INSTS);
		ResultSet rs = null;
		try {
			pStmt.setString(1, c.getHost());
			pStmt.setInt(2, c.getPort());
			pStmt.setString(3, taskId);
			rs = pStmt.executeQuery();
			return rs.next() ? rs.getInt(1) : 0;
		} catch(SQLException e) {
			LOG.error(SQL_ERROR, e);
			return Integer.MAX_VALUE;
		} finally {
			if(rs != null)
				try { rs.close(); } catch(SQLException e) { LOG.warn(SQL_ERROR, e); }
		}
	}
	
	Map<String, String> getMetadata(long id) {
		PreparedStatement pStmt = stmts.get(GET_METADATA);
		ResultSet rs = null;
		try {
			Map<String, String> map = new HashMap<String, String>();
			pStmt.setLong(1, id);
			rs = pStmt.executeQuery();
			while(rs.next())
				map.put(rs.getString(1), rs.getString(2));
			return map;
				
		} catch(SQLException e) {
			LOG.error(SQL_ERROR, e);
			return Collections.emptyMap();
		} finally {
			if(rs != null)
				try { rs.close(); } catch(SQLException e) { LOG.warn(SQL_ERROR, e); }
		}
	}
	
	int getFreeResources(Client c) {
		PreparedStatement pStmt = stmts.get(GET_USED_RESOURCES);
		ResultSet rs = null;
		try {
			pStmt.setString(1, c.getHost());
			pStmt.setInt(2, c.getPort());
			rs = pStmt.executeQuery();
			int freeRes = c.getMaxResources();
			if(rs.next())
				freeRes -= rs.getInt(1);
			return freeRes; 
		} catch(SQLException e) {
			LOG.error(SQL_ERROR, e);
			return -1;
		} finally {
			if(rs != null)
				try { rs.close(); } catch(SQLException e) { LOG.warn(SQL_ERROR, e); }
		}
	}
	
	private boolean updateClient(Client clnt) {
		PreparedStatement pStmt = stmts.get(UPDATE_CLIENT);
		boolean localTransaction = false;
		try {
			localTransaction = conn.getAutoCommit();
			if(localTransaction)
				conn.setAutoCommit(false);
			pStmt.setString(1, clnt.getState().toString());
			pStmt.setString(2, clnt.getSchedule());
			pStmt.setTimestamp(3, new java.sql.Timestamp(clnt.getLastUpdate().getTime()));
			pStmt.setInt(4, clnt.getMaxResources());
			pStmt.setString(5, clnt.getHost());
			pStmt.setInt(6, clnt.getPort());
			pStmt.executeUpdate();
			updateClientTasks(clnt);
			if(localTransaction)
				conn.commit();
			return true;
		} catch(SQLException e) {
			LOG.error(SQL_ERROR, e);
			if(localTransaction) {
				try {
					conn.rollback();
				} catch (SQLException e1) {
					LOG.fatal("RollbackError", e1);
				}
			}
			return false;
		} finally {
			if(localTransaction) {
				try {
					conn.setAutoCommit(true);
				} catch (SQLException e) {
					LOG.fatal("AutoCommitError", e);
				}
			}
		}
	}

	public void close() {
		for(PreparedStatement s : stmts.values())
			if(s != null)
				try { s.close(); } catch(SQLException e) { LOG.error(SQL_ERROR, e); }
				stmts.clear();
				if(conn != null)
					try { conn.close(); } catch(SQLException e) { LOG.error(SQL_ERROR, e); }
					POOL.remove();
	}
}