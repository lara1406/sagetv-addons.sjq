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
import java.net.InetAddress;
import java.net.UnknownHostException;
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
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Logger;

import sagex.SageAPI;
import sagex.api.Configuration;
import sagex.api.Global;

import com.google.code.sagetvaddons.sjq.shared.Client;
import com.google.code.sagetvaddons.sjq.shared.QueuedTask;
import com.google.code.sagetvaddons.sjq.shared.Task;
import com.google.code.sagetvaddons.sjq.utils.TaskList;


/**
 * The SJQv4 DataStore houses the task queue, task client info, and task logs
 * @author dbattams
 * @version $Id$
 */
public final class DataStore {
	static private final Logger LOG = Logger.getLogger("com.google.code.sagetvaddons.sjq." + (Global.IsClient() || SageAPI.isRemote() ? "agent" : "server") + ".DataStore");
	static final String LIC_PROP = "sjq4/isLicensed";
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

	/**
	 * <p>Get a valid, initiated connection to the SJQv4 DataStore.</p>
	 * <p>The returned DataStore instance is automatically connected to the appropriate
	 * SageTV server regardless of whether this is invoked on the server or a PC client.
	 * If called outside of a SageTV JVM then it is assumed that your sagex-api RMI provider
	 * has been appropriately configured to the SageTV server.</p>
	 * @return The DataStore connection or null in case of error
	 */
	static public final DataStore get() {
		DataStore ds = POOL.get();
		int retries = 15;
		int i = 1;
		while(retries-- > 0 && (ds == null || !ds.isValid())) {
			LOG.warn("Problem connecting to database... trying again in " + i + " seconds...");
			try {
				Thread.sleep(1000L * i);
			} catch (InterruptedException e) {
				// Who cares?
			}
			i *= 2;
			if(i > 30)
				i = 30;
			POOL.remove();
			ds = POOL.get();
		}
		return ds;
	}

	// The JDBC_URL is build using the Sage APIs
	static private final String JDBC_URL= "jdbc:h2:tcp://" + Global.GetServerAddress() + ":" + Configuration.GetServerProperty("h2/tcp_port", "9092") + "/plugins/sjq/sjq4";
	static private final String SQL_ERROR = "SQL Error";
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
	static private final String DELETE_SETTING = "DelSetting";
	static private final String SAVE_SETTING = "SaveSetting";
	static private final String DELETE_TASK = "DelTask";
	static private final String CLEAN_QUEUE = "CleanQueue";

	static private final int DB_SCHEMA = 2;
	
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

	/**
	 * Extend the finalizer to also close the database connection
	 */
	@Override
	protected void finalize() {
		try {
			close();
		} finally {
			try { super.finalize(); } catch(Throwable t) { LOG.error("FinalizeError", t); }
		}		
	}

	// Create the DB tables and view, if they don't exist
	private void initDb() throws SQLException {
		Statement s = conn.createStatement();
		String qry = "CREATE TABLE IF NOT EXISTS settings (var VARCHAR(128) NOT NULL PRIMARY KEY, val LONGVARCHAR, " +
		"CONSTRAINT IF NOT EXISTS var_not_empty__settings CHECK LENGTH(var) > 0)";
		s.executeUpdate(qry);

		qry = String.format(Locale.US, "CREATE TABLE IF NOT EXISTS client (host VARCHAR(512) NOT NULL, port INTEGER NOT NULL, state VARCHAR(64) NOT NULL DEFAULT '%s', schedule VARCHAR(128) NOT NULL DEFAULT '%s', last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP, total_resources TINYINT NOT NULL DEFAULT %d, PRIMARY KEY(host, port), " +
				"CONSTRAINT IF NOT EXISTS port_gt_zero__client CHECK port > 0, " +
				"CONSTRAINT IF NOT EXISTS host_not_empty__client CHECK LENGTH(host) > 0, " +
				"CONSTRAINT IF NOT EXISTS total_res_ge_zero__client CHECK total_resources >= 0)", Client.State.OFFLINE.toString(), Client.DEFAULT_SCHED, Client.DEFAULT_RESOURCES);
		s.executeUpdate(qry);

		qry = String.format(Locale.US, "CREATE TABLE IF NOT EXISTS client_tasks (id VARCHAR(128) NOT NULL, host VARCHAR(512) NOT NULL, port INT NOT NULL, reqd_resources TINYINT NOT NULL DEFAULT %d, max_instances TINYINT NOT NULL DEFAULT %d, schedule VARCHAR(256) NOT NULL DEFAULT '%s', exe VARCHAR(255) NOT NULL, args VARCHAR(7936) NOT NULL DEFAULT '', max_time INT NOT NULL DEFAULT %d, max_time_ratio REAL NOT NULL DEFAULT %f, min_rc SMALLINT NOT NULL DEFAULT %d, max_rc SMALLINT NOT NULL DEFAULT %d, test VARCHAR(255), test_args VARCHAR(7936) NOT NULL DEFAULT '', " +
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
		
		qry = "CREATE INDEX IF NOT EXISTS finished__queue ON queue(finished)";
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

	// Upgrade the DB schema, as necessary
	private void upgradeDb() throws SQLException {
		String qry = "SELECT val FROM settings WHERE var = 'schema'";
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery(qry);
		rs.next();
		int schema = Integer.parseInt(rs.getString(1));
		rs.close();
		while(schema < DB_SCHEMA) {
			stmt = conn.createStatement();
			switch(schema) {
			case 1:
				qry = "ALTER TABLE client ADD COLUMN version INT NOT NULL DEFAULT 0";
				stmt.executeUpdate(qry);
				
				qry = "UPDATE settings SET val = '2' WHERE var = 'schema'";
				stmt.executeUpdate(qry);
				break;
			}
			++schema;
		}
		stmt.close();
	}

	// Prep the various SQL statements used in the DataStore
	private void prepStatements() throws SQLException {
		stmts = new HashMap<String, PreparedStatement>();

		String qry = "SELECT val FROM settings WHERE var = ?";
		stmts.put(READ_SETTING, conn.prepareStatement(qry));

		qry = "SELECT state, schedule, last_update, total_resources, version FROM client WHERE host = ? AND port = ?";
		stmts.put(READ_CLIENT, conn.prepareStatement(qry));

		qry = "SELECT host, port FROM client";
		stmts.put(READ_ALL_CLIENTS, conn.prepareStatement(qry));

		qry = "INSERT INTO client (host, port, state, schedule, last_update, total_resources, version) VALUES (?, ?, ?, ?, ?, ?, ?)";
		stmts.put(ADD_CLIENT, conn.prepareStatement(qry));

		qry = "UPDATE client SET state = ?, schedule = ?, last_update = ?, total_resources = ?, version = ? WHERE host = ? AND port = ?";
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

		qry = "SELECT t.id, t.reqd_resources, t.max_instances, t.schedule, t.exe, t.args, t.max_time, t.max_time_ratio, t.min_rc, t.max_rc, t.test, t.test_args FROM client AS c LEFT OUTER JOIN client_tasks AS t ON (c.host = t.host AND c.port = t.port) WHERE c.host = ? AND c.port = ? AND t.host IS NOT NULL";
		stmts.put(READ_CLNT_TASKS, conn.prepareStatement(qry));

		qry = "DELETE FROM settings WHERE var = ?";
		stmts.put(DELETE_SETTING, conn.prepareStatement(qry));

		qry = "INSERT INTO settings (var, val) VALUES (?, ?)";
		stmts.put(SAVE_SETTING, conn.prepareStatement(qry));

		qry = "DELETE FROM queue WHERE id = ? AND state NOT IN ('STARTED', 'RUNNING')";
		stmts.put(DELETE_TASK, conn.prepareStatement(qry));
		
		qry = "DELETE FROM queue WHERE (DATEDIFF('HOUR', finished, CURRENT_TIMESTAMP) >= ? AND state = 'COMPLETED') OR (DATEDIFF('HOUR', finished, CURRENT_TIMESTAMP) >= ? AND state = 'FAILED') OR (DATEDIFF('HOUR', finished, CURRENT_TIMESTAMP) >= ? AND state = 'SKIPPED')";
		stmts.put(CLEAN_QUEUE, conn.prepareStatement(qry));
	}

	/**
	 * <p>Determine if this data store connection is still in a valid state (i.e. it's still connected to the database server)</p>
	 * 
	 * <p>If isValid() returns false then the data store connection is removed from the thread local storage such that the next
	 * call to get() will return a new, freshly initiated data store connection for use.</p>
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

	/**
	 * Get the current schema version for the database.
	 * @return The current schema version; -1 is returned if the schema is not set (i.e. a new DB file)
	 */
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

	/**
	 * Get all the configured Tasks for the given Client
	 * @param c The client to lookup
	 * @return An array of Tasks the given client is configured to handle; the array may be empty in case of error, but never null
	 */
	Task[] getTasksForClient(Client c) {
		return getTasksForClient(c.getHost(), c.getPort());
	}

	/**
	 * Get all the configured Tasks for a Client, identified by host and port
	 * @param host The Client's hostname
	 * @param port The Client's port
	 * @return An array of Tasks the given client is configured to handle; the array may be empty in case of error, but never null
	 */
	Task[] getTasksForClient(String host, int port) {
		if(host == null || host.length() == 0 || port < 1)
			throw new IllegalArgumentException("Client keys are invalid! ['" + String.valueOf(host) + "':" + port + "]");
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

	/**
	 * Get an array of Clients capable of running the given task id
	 * @param taskId The task id of interest
	 * @return An array of clients capable of running 'taskId' tasks; the array may be empty, but never null
	 */
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

	/**
	 * <p>Return an array of active tasks.</p>
	 * <p>Active tasks are tasks that are assigned to a client and are running or tasks that are waiting to be assigned.</p>
	 * @return An array of QueuedTasks representing all tasks in the queue that are running or waiting to be assigned to a client
	 */
	public QueuedTask[] getActiveQueue() {
		return getQueue(true);
	}
	
	/**
	 * <p>Return an array of every task in the queue.</p>
	 * <p>This method returns every task in the queue at the time of the call; this includes active tasks, completed tasks, and failed tasks. <b>This list can get rather large, rather quickly!</b></p>
	 * @return An array of QueuedTasks representing all tasks in the queue
	 */
	public QueuedTask[] getQueue() {
		return getQueue(false);
	}
	
	QueuedTask[] getQueue(boolean activeOnly) {
		Collection<QueuedTask> tasks = new ArrayList<QueuedTask>();
		String qry = "SELECT q.id, q.job_id, created, assigned, finished, state, reqd_resources, max_instances, schedule, exe, args, max_time, max_time_ratio, min_rc, max_rc, test, test_args, q.host, q.port FROM queue AS q LEFT OUTER JOIN client_tasks AS t ON (q.job_id = t.id AND q.host = t.host AND q.port = t.port)";
		if(activeOnly)
			qry = qry.concat(" WHERE state NOT IN ('COMPLETED', 'FAILED', 'SKIPPED')");
		Statement stmt = null;
		ResultSet rs = null;
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery(qry);
			while(rs.next())
				tasks.add(new QueuedTask(rs.getLong(1), rs.getString(2), rs.getInt(7), rs.getInt(8), rs.getString(9), rs.getString(10), rs.getString(11), rs.getLong(12), rs.getFloat(13), rs.getInt(14), rs.getInt(15), getMetadata(rs.getLong(1)), rs.getTimestamp(3), rs.getTimestamp(4), rs.getTimestamp(5), QueuedTask.State.valueOf(rs.getString(6)), getClient(rs.getString(18), rs.getInt(19)), Global.GetServerAddress(), Integer.parseInt(Configuration.GetServerProperty("sjq4/enginePort", "23347")), rs.getString(16), rs.getString(17), Integer.parseInt(Configuration.GetServerProperty("sagex/api/RMIPort", "1098"))));
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

	/**
	 * Get an array of tasks that are waiting to be assigned to a client
	 * @return The array of pending tasks
	 */
	PendingTask[] getPendingTasks(boolean ignoreReturned) {
		String qry = "SELECT id, job_id, created FROM queue WHERE state = 'WAITING'";
		if(!ignoreReturned)
			qry = qry.concat(" OR state = 'RETURNED'");
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

	/**
	 * Update a QueuedTask; all fields of the task are updated in the DataStore
	 * @param qt The task to be updated
	 * @return True on success or false if any error was encountered that prevented the data from being updated in the database
	 */
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

	/**
	 * <p>Add a new task to the queue.</p>
	 * <p>The metadata map is a mapping of variables to values, each of which becomes an environment
	 * variable in the task's runtime environment when the task is executed.</p>
	 * @param taskId The task id type to be added
	 * @param metadata The metadata to associte with the task; can be an empty map, but CANNOT be null
	 * @return The unique task queue id number assigned to the newly created task
	 * @throws SQLException Thrown if the task could not be added to the queue
	 */
	long addTask(String taskId, Map<String, String> metadata) throws SQLException {
		taskId = taskId.toUpperCase();
		boolean localTransaction = false;
		try {
			localTransaction = conn.getAutoCommit();
			if(localTransaction)
				conn.setAutoCommit(false);
			long qId = createTask(taskId);
			setMetadata(qId, metadata);
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

	// Write the metadata to the database
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

	// Add a new task entry to the queue table and return the unique id assigned to it
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

	// Update the client tasks for the given Client; this should be ran each time a task client is pinged
	private void updateClientTasks(Client clnt) throws SQLException {
		PreparedStatement pStmt = stmts.get(WIPE_CLNT_TASKS);
		pStmt.setString(1, clnt.getHost());
		pStmt.setInt(2, clnt.getPort());
		pStmt.executeUpdate();

		if(clnt.getTasks().length > 0) {
			pStmt = stmts.get(ADD_CLNT_TASK);
			pStmt.setString(1, clnt.getHost());
			pStmt.setInt(2, clnt.getPort());
			boolean maxTasks = false;
			for(Task t : clnt.getTasks()) {
				if(maxTasks && !isLicensed()) {
					LOG.error("Task clients for unlicensed versions of SJQv4 can only have a single task defined.  Additional task definitions ignored!");
					break;
				}
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
				maxTasks = true;
			}
			pStmt.executeBatch();
		}
	}

	/**
	 * Log task output to the database
	 * @param qt The task the log data is to be associated with
	 * @param type The type of log output ('TASK' or 'TEST')
	 * @param log The log contents to be stored
	 * @return True if the log output was saved successfully or false otherwise
	 */
	public boolean logOutput(QueuedTask qt, QueuedTask.OutputType type, String log) {
		ResultSet rs = null;
		PreparedStatement pStmt = stmts.get(COUNT_LOG);
		try {
			pStmt.setLong(1, qt.getQueueId());
			pStmt.setString(2, type.toString());
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

	// Private helper to append log output to an existing entry
	private boolean updateLog(long id, QueuedTask.OutputType type, String log) {
		PreparedStatement pStmt = stmts.get(READ_LOG);
		ResultSet rs = null;
		Reader r = null;
		try {
			pStmt.setLong(1, id);
			pStmt.setString(2, type.toString());
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
			pStmt.setString(3, type.toString());
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

	// Private helper to create a new log entry for a task queue id
	private boolean addLog(long id, QueuedTask.OutputType type, String log) {
		PreparedStatement pStmt = stmts.get(ADD_LOG);
		log = "===== " + new Date().toString() + " =====\n\n" + log.concat("\n\n==============================");
		try {
			pStmt.setLong(1, id);
			pStmt.setString(2, type.toString());
			pStmt.setCharacterStream(3, new StringReader(log));
			pStmt.executeUpdate();
			return true;
		} catch(SQLException e) {
			LOG.error(SQL_ERROR, e);
			return false;
		}
	}

	/**
	 * Create a new Client or update an existing one; use this method to register new task clients with the server
	 * @param clnt The client to be created or updated
	 * @return True if the Client was saved/updated or false otherwise
	 */
	public boolean saveClient(Client clnt) {
		if(getClient(clnt.getHost(), clnt.getPort()) != null)
			return updateClient(clnt);
		else {
			if(getAllClients().length > 0 && !isLicensed()) {
				LOG.error("Unlicensed versions of SJQv4 can only have one registered task client!  You cannot register more task clients!");
				return false;
			}
			try {
				if(clnt.getHost().equals("127.0.0.1") || clnt.getHost().toLowerCase().contains("localhost") || InetAddress.getByName(clnt.getHost()).getHostAddress().equals("127.0.0.1")) {
					LOG.error("Specified host appears to be the loopback interface and cannot be registered as a task client in SJQv4!  Please try another hostname.");
					return false;
				}
			} catch(UnknownHostException e) {
				LOG.error("DNS Error", e);
				return false;
			}
			Set<String> ips = new HashSet<String>();
			for(Client c : getAllClients()) {
				try {
					if(c.getPort() == clnt.getPort())
						for(InetAddress addr : InetAddress.getAllByName(c.getHost()))
							ips.add(addr.getHostAddress());
					else
						LOG.info("Skipped b/c different port: " + c);
				} catch (UnknownHostException e) {
					LOG.warn("UnknownHost", e);
				}
			}
			LOG.info("Found these registered IPs: " + ips);
			try {
				String newIp = InetAddress.getByName(clnt.getHost()).getHostAddress();
				LOG.info("Testing: " + clnt.getHost() + "/" + newIp);
				if(ips.contains(newIp)) {
					LOG.error("IP address of hostname already registered under same port and cannot be registered again!");
					return false;
				}
			} catch (UnknownHostException e2) {
				LOG.warn("UnknownHost", e2);
				return false;
			}
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
				pStmt.setInt(7, clnt.getVersion());
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

	/**
	 * Retrieve a Client instance for the given host/port combination
	 * @param host The hostname of the Client to retrieve
	 * @param port The port number for the Client
	 * @return The Client, if it's registered, or null if the given Client does not exist in the database
	 */
	public Client getClient(String host, int port) {
		PreparedStatement pStmt = stmts.get(READ_CLIENT);
		ResultSet rs = null;
		try {
			pStmt.setString(1, host);
			pStmt.setInt(2, port);
			rs = pStmt.executeQuery();
			if(rs.next()) {
				Client c = new Client(host, port, getFreeResources(host, port, rs.getInt(4)), rs.getString(2), Client.State.valueOf(rs.getString(1)), rs.getTimestamp(3), rs.getInt(4), getTasksForClient(host, port), rs.getInt(5));
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

	/**
	 * Get an array of all registered task clients
	 * @return The array of all registered task clients
	 */
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

	/**
	 * Get a count of how many instances of taskId the given Client is currently executing
	 * @param taskId The taskId to query
	 * @param c The client to query
	 * @return A count of the number of instances of taskId the Client, c, is currently running
	 */
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

	/**
	 * Get the metadata map for the given task queue id
	 * @param id The unique task queue id to build the map for
	 * @return A map of metadata for the given task id; may be an empty map, but never null
	 */
	public Map<String, String> getMetadata(long id) {
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

	/**
	 * Get the free resource count for the given Client
	 * @param c The client to check the free resource count for
	 * @return The free resources available on the given Client, c
	 */
	int getFreeResources(Client c) {
		return getFreeResources(c.getHost(), c.getPort(), c.getMaxResources());
	}
	
	/**
	 * Calculate the free resources for the given client at the time of this call
	 * @param host The hostname
	 * @param port The port
	 * @param maxResources The max resources for the client
	 * @return The free resources available on the given client at the time of the call
	 */
	int getFreeResources(String host, int port, int maxResources) {
		PreparedStatement pStmt = stmts.get(GET_USED_RESOURCES);
		ResultSet rs = null;
		try {
			pStmt.setString(1, host);
			pStmt.setInt(2, port);
			rs = pStmt.executeQuery();
			int freeRes = maxResources;
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

	// Private helper that updates an existing client entry in the database
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
			pStmt.setInt(5, clnt.getVersion());
			pStmt.setString(6, clnt.getHost());
			pStmt.setInt(7, clnt.getPort());
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

	/**
	 * Retrive a setting from the SJQv4 registry
	 * @param var The name of the setting to retrieve
	 * @return The value of the setting or null if the setting does not exist in the registry
	 * @throws IllegalArgumentException Thrown if var = 'schema'; you cannot read the schema this way; use getSchema() instead
	 */
	public String getSetting(String var) {
		return getSetting(var, null);
	}

	/**
	 * Retrieve a setting from the SJQv4 registry
	 * @param var The name of the setting to retrieve
	 * @param defaultVal The default value to return if the setting does not exist in the registry
	 * @return The value of the setting or defaultVal if the setting does not exist
	 * @throws IllegalArgumentException Thrown if var = 'schema'; you cannot read the schema this way; use getSchema() instead
	 */
	public String getSetting(String var, String defaultVal) {
		if("schema".equals(var))
			throw new IllegalArgumentException("Cannot access schema setting via getSetting()");
		PreparedStatement pStmt = stmts.get(READ_SETTING);
		ResultSet rs = null;
		try {
			pStmt.setString(1, var);
			rs = pStmt.executeQuery();
			if(rs.next())
				return rs.getString(1);
			return defaultVal;
		} catch(SQLException e) {
			LOG.error(SQL_ERROR, e);
			return defaultVal;
		} finally {
			if(rs != null)
				try { rs.close(); } catch(SQLException e) { LOG.warn(SQL_ERROR, e); }
		}
	}

	/**
	 * Save a setting to the SJQv4 registry
	 * @param var The name of the setting
	 * @param val The value of the setting
	 * @throws IllegalArgumentException Thrown if var = 'schema'; you cannot manipulate the schema value of the database
	 */
	public void setSetting(String var, String val) {
		if("schema".equals(var))
			throw new IllegalArgumentException("Cannot modify schema setting via setSetting()");
		PreparedStatement del = stmts.get(DELETE_SETTING);
		PreparedStatement add = stmts.get(SAVE_SETTING);
		boolean localTransaction = false;
		try {
			localTransaction = conn.getAutoCommit();
			if(localTransaction)
				conn.setAutoCommit(false);
			del.setString(1, var);
			del.executeUpdate();
			add.setString(1, var);
			add.setString(2, val);
			add.executeUpdate();
			if(localTransaction)
				conn.commit();
		} catch(SQLException e) {
			LOG.error(SQL_ERROR, e);
			if(localTransaction)
				try { conn.rollback(); } catch(SQLException e1) { LOG.error(SQL_ERROR, e1); }
		} finally {
			if(localTransaction)
				try { conn.setAutoCommit(true); } catch(SQLException e) { LOG.error(SQL_ERROR, e); }
		}
	}

	/**
	 * Close the connection to the DataStore; once closed you must call DataStore.get() to get a new
	 * connection as this one will become invalid and will no longer function.
	 */
	public void close() {
		for(PreparedStatement s : stmts.values())
			if(s != null)
				try { s.close(); } catch(SQLException e) { LOG.error(SQL_ERROR, e); }
				stmts.clear();
				if(conn != null)
					try { conn.close(); } catch(SQLException e) { LOG.error(SQL_ERROR, e); }
					POOL.remove();
	}

	boolean deleteTask(long queueId) {
		PreparedStatement pStmt = stmts.get(DELETE_TASK);
		try {
			pStmt.setLong(1, queueId);
			return pStmt.executeUpdate() == 1;
		} catch(SQLException e) {
			LOG.error(SQL_ERROR, e);
			return false;
		}
	}

	boolean deleteClient(Client c) {
		if(c == null) {
			LOG.warn("Can't delete null client!");
			return false;
		} else if(c.getMaxResources() != getFreeResources(c)) {
			LOG.warn("Can't delete " + c + " because it is currently running tasks!");
			return false;
		}

		PreparedStatement pStmt = stmts.get(DELETE_CLIENT);
		try {
			pStmt.setString(1, c.getHost());
			pStmt.setInt(2, c.getPort());
			return pStmt.executeUpdate() == 1;
		} catch(SQLException e) {
			LOG.error(SQL_ERROR, e);
			return false;
		}
	}

	/**
	 * Get a list of all registered task ids from all registered task clients
	 * @return The list of task ids; may be empty in case of error, but never null
	 */
	public String[] getRegisteredTaskIds() {
		String qry = "SELECT DISTINCT id FROM client_tasks";
		Collection<String> tasks = new ArrayList<String>();
		Statement stmt = null;
		ResultSet rs = null;
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery(qry);
			while(rs.next())
				tasks.add(rs.getString(1));
		} catch(SQLException e) {
			LOG.error(SQL_ERROR, e);
			return new String[0];
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
		return tasks.toArray(new String[tasks.size()]);
	}
	
	/**
	 * <p>Get a map of all registered tasks</p>
	 * <p>Each key of the map is a Client.getDescription() value for the client the task belongs to</p>
	 * @return The map of registered tasks; may be empty in case of error, but never null
	 */
	public Map<String, Collection<Task>> getAllRegisteredTasks() {
		String qry = "SELECT id, host, port, reqd_resources, max_instances, schedule, exe, args, max_time, max_time_ratio, min_rc, max_rc, test, test_args FROM client_tasks";
		Map<String, Collection<Task>> map = new HashMap<String, Collection<Task>>();
		Statement stmt = null;
		ResultSet rs = null;
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery(qry);
			while(rs.next()) {
				Client c = getClient(rs.getString(2), rs.getInt(3));
				if(c == null)
					continue;
				Collection<Task> tasks = map.get(c.getDescription());
				if(tasks == null) {
					tasks = new ArrayList<Task>();
					map.put(c.getDescription(), tasks);
				}
				tasks.add(new Task(rs.getString(1), rs.getInt(4), rs.getInt(5), rs.getString(6), rs.getString(7), rs.getString(8), rs.getLong(9), rs.getFloat(10), rs.getInt(11), rs.getInt(12), rs.getString(13), rs.getString(14)));
			}
		} catch(SQLException e) {
			LOG.error(SQL_ERROR, e);
			return Collections.emptyMap();
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
		return map;
	}

	/**
	 * Get an array of tasks currently running on the given Client
	 * @param clnt The client to get active tasks for
	 * @return An array of tasks actively running on the given Client
	 */
	public QueuedTask[] getActiveTasksForClient(Client clnt) {
		if(clnt == null)
			return new QueuedTask[0];
		Collection<QueuedTask> tasks = new ArrayList<QueuedTask>();
		for(QueuedTask qt : getActiveQueue()) {
			Client assignee = qt.getAssignee();
			if(assignee != null && clnt.getHost().equals(assignee.getHost()) && clnt.getPort() == assignee.getPort())
				tasks.add(qt);
		}
		return tasks.toArray(new QueuedTask[tasks.size()]);
	}
	
	void cleanCompletedTasks(int completedDays, int failedDays, int skippedDays) {
		PreparedStatement pStmt = stmts.get(CLEAN_QUEUE);
		try {
			pStmt.setLong(1, 24L * completedDays);
			pStmt.setLong(2, 24L * failedDays);
			pStmt.setLong(3, 24L * skippedDays);
			LOG.info("Cleaned up " + pStmt.executeUpdate() + " row(s) from task queue!");
		} catch(SQLException e) {
			LOG.error(SQL_ERROR, e);
		}
	}
	
	/**
	 * Determine if the user's SJQv4 engine is licensed or not
	 * @return True if the SJQv4 engine is licensed or false otherwise
	 */
	public boolean isLicensed() {
		return Boolean.parseBoolean(Configuration.GetServerProperty(LIC_PROP, "false"));
	}
	
	/**
	 * Get all output generated by the given queue id; this includes test and task output combined into one String
	 * @param qId The queue id to return the task output for
	 * @return All task output found or the empty string in case of error
	 */
	public String getTaskLog(long qId) {
		return getTaskLog(qId, (QueuedTask.OutputType)null);
	}
	
	/**
	 * Return the task output for the given queue id
	 * @param qId The queue id to get output for
	 * @param type The type of output to return or null to return ALL output for the queue id
	 * @return The output as a String or the empty String in case of error
	 */
	public String getTaskLog(long qId, QueuedTask.OutputType type) {
		StringBuilder qry = new StringBuilder("SELECT log FROM task_log WHERE id = %d");
		if(type != null)
			qry.append(" AND type = '%s'");
		Statement stmt = null;
		ResultSet rs = null;
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery(String.format(Locale.US, qry.toString(), qId, type != null ? type.toString() : ""));
			StringBuilder out = new StringBuilder();
			while(rs.next())
				out.append(rs.getString(1));
			return out.toString();
		} catch(SQLException e) {
			LOG.error(SQL_ERROR, e);
			return "";
		} finally {
			try {
				if(rs != null) rs.close();
				if(stmt != null) stmt.close();
			} catch(SQLException e) {
				LOG.warn(SQL_ERROR, e);
			}
		}
	}
	
	/**
	 * Convenience method for STVi access to specific log types
	 * @param qId The queue id to read logs for
	 * @param type The type of logs, must be one of the valid values of the {@see com.google.code.sagetvaddons.sjq.shared.QueuedTask.OutputType} enum
	 * @return The requested logs, as a String, or the empty string in case of error
	 */
	public String getTaskLog(long qId, String type) {
		return getTaskLog(qId, QueuedTask.OutputType.valueOf(type.toUpperCase()));
	}
	
	/**
	 * Get a list of all events supported and processed by the engine
	 * @return The array of event names that the SJQ engine currently listens for and processes
	 */
	public String[] getSupportedEvents() {
		String events = getSetting("SupportedEvents");
		if(events == null || events.length() == 0)
			return new String[0];
		// Remove RecordingSegmentAdded because it's grouped in with RecordingStarted and should be processed internally only
		return (String[])ArrayUtils.removeElement(events.split(","), "RecordingSegmentAdded");
	}
	
	/**
	 * Get a list of events that favourites and manual recordings can attach tasks to
	 * @return The array of supported tv events
	 */
	public String[] getSupportedTvEvents() {
		String events = getSetting("SupportedTvEvents");
		if(events == null || events.length() == 0)
			return new String[0];
		return events.split(",");
	}
	
	/**
	 * Attach a task to an event; the event should be one of the supported engine events, but it's not enforced
	 * @param taskId The task id to attach to the given event
	 * @param eventId The event id the given task is being attached to
	 * @throws NullPointerException If either argument is null
	 */
	public void addTaskToEvent(String taskId, String eventId) {
		setSetting(eventId, TaskList.addTask(taskId, getSetting(eventId, "")));	
	}
	
	/**
	 * Attach an array of task ids to the given event id; the event should be one of the supported engine events, but it's not enforced
	 * @param taskIds An array of task ids to be attached to the given event id
	 * @param eventId The event id the given tasks are to be attached to
	 * @throws NullPointerException If either argument is null
	 */
	public void addTasksToEvent(String[] taskIds, String eventId) {
		for(String taskId : taskIds)
			addTaskToEvent(taskId, eventId);
	}
	
	/**
	 * Attach a Collection of task ids to the given event id; the event should be one of the supported engine events, but it's not enforced
	 * @param taskIds The collection of task ids to attach to the given event id
	 * @param eventId The event id the given tasks are to be attached to
	 * @throws NullPointerException If either argument is null
	 */
	public void addTasksToEvent(Collection<String> taskIds, String eventId) {
		addTasksToEvent(taskIds.toArray(new String[taskIds.size()]), eventId);
	}
	
	/**
	 * Remove the given task id from the given event id
	 * @param taskId The task id to be removed
	 * @param eventId The event from which the given task id will be removed from
	 * @throws NullPointerException If either argument is null
	 */
	public void removeTaskFromEvent(String taskId, String eventId) {
		setSetting(eventId, TaskList.removeTask(taskId, getSetting(eventId, "")));
	}
	
	/**
	 * Remove the array of task ids from the given event id
	 * @param taskIds The array of task ids to be removed
	 * @param eventId The event from which the given task ids will be removed from
	 * @throws NullPointerException If either argument is null
	 */
	public void removeTasksFromEvent(String[] taskIds, String eventId) {
		for(String taskId : taskIds)
			removeTaskFromEvent(taskId, eventId);
	}
	
	/**
	 * Remove the collection of task ids from the given event id
	 * @param taskIds The collection of task ids to be removed
	 * @param eventId The event from which the given task ids will be removed from
	 * @throws NullPointerException If either argument is null
	 */
	public void removeTasksFromEvent(Collection<String> taskIds, String eventId) {
		removeTasksFromEvent(taskIds.toArray(new String[taskIds.size()]), eventId);
	}
	
	/**
	 * Return an array of all task ids attached to the given event
	 * @param eventId The event to lookup
	 * @return The array of tasks ids attached to the given event
	 * @throws NullPointerException If eventId is null
	 */
	public String[] getTasksForEvent(String eventId) {
		return TaskList.getList(getSetting(eventId, ""));
	}
}