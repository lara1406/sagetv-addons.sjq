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

import gkusnick.sagetv.api.API;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Date;
import java.util.Map;

import org.apache.log4j.Logger;

import com.google.code.sagetvaddons.sjq.network.AgentClient;
import com.google.code.sagetvaddons.sjq.shared.Client;
import com.google.code.sagetvaddons.sjq.shared.QueuedTask;
import com.google.code.sagetvaddons.sjq.shared.Task;
import com.google.code.sagetvaddons.sjq.shared.QueuedTask.State;

/**
 * <p>A thread-safe reader/writer of the SJQv4 TaskQueue</p>
 * <p>This class provides thread-safe access to the SJQv4 task queue.  Within the SJQ server
 * code, this is the class to use for manipulating the task queue.  <b>Other JVMs cannot use this
 * class to manipulate the task queue as it does not synchrnoize across processes!</b>  If you're 
 * trying to manipulate the task queue in another JVM, perhaps from a task client, then you must use
 * the {@link com.google.code.sagetvaddons.sjq.network.ServerClient} class, which interacts with the
 * task queue through a socket connection, which is properly synchronized across multiple processes.</p>
 * <p><b>If you interact with the task queue via this class in an external JVM then all bets are off - 
 * weird, unexpected things are probably going to happen to your task queue!</b></p>
 * <p>Eventually I'll come back and remove the public access to this class, but for now it's too
 * convenient for me instead of forcing the app itself to also go through the socket server.</p>
 * @author dbattams
 * @version $Id$
 */
final public class TaskQueue {
	static private final Logger LOG = Logger.getLogger(TaskQueue.class);
	
	static private final TaskQueue INSTANCE = new TaskQueue();
	
	/**
	 * <p>Return the TaskQueue singleton</p>
	 * <p><b>This class should only be used within the same JVM the SJQv4 server is running in!</b></p>
	 * @return The TaskQueue singleton
	 */
	static final public TaskQueue get() { return INSTANCE; }

	/**
	 * Details about a pending task; a task is Pending when it's in WAITING or RETURNED state
	 * @author dbattams
	 *
	 */
	static final class PendingTask {
		private long qId;
		private String taskId;
		private Date created;
		
		PendingTask(long qId, String taskId, Date created) {
			this.qId = qId;
			this.taskId = taskId;
			this.created = created;
		}

		/**
		 * @return the qId
		 */
		public long getQid() {
			return qId;
		}

		/**
		 * @param qId the qId to set
		 */
		public void setQid(long qId) {
			this.qId = qId;
		}

		/**
		 * @return the taskId
		 */
		public String getTaskId() {
			return taskId;
		}

		/**
		 * @param taskId the taskId to set
		 */
		public void setTaskId(String taskId) {
			this.taskId = taskId;
		}

		/**
		 * @return the created
		 */
		public Date getCreated() {
			return created;
		}

		/**
		 * @param created the created to set
		 */
		public void setCreated(Date created) {
			this.created = created;
		}
	}
	
	static private final class QueueLauncher extends Thread {
		
		private boolean ignore;
		
		private QueueLauncher(boolean ignoreReturned) {
			ignore = ignoreReturned;
		}
		
		@Override
		public void run() {
			TaskQueue.get().startTasks(ignore);
		}
	}
	
	private TaskQueue() {
		
	}
	
	/**
	 * Add a new task to the queue
	 * @param id The task id to be added to the queue
	 * @param metadata The var/val pairs of environment variables to inject into this task's runtime env
	 * @return The new task queue id for the created task
	 * @throws IOException Thrown if there was an error adding the task to the queue
	 */
	synchronized public long addTask(String id, Map<String, String> metadata) throws IOException {
		try {
			long qId = DataStore.get().addTask(id, metadata);
			new QueueLauncher(true).start();
			return qId;
		} catch (SQLException e) {
			throw new IOException(e);
		}
	}
	
	/**
	 * Delete a pending task from the queue
	 * @param queueId The unique task queue id number of the task to be deleted
	 * @return True if the task was successfully deleted from the queue or false otherwise; a task not in WAITING, RETURNED or FAILED state cannot be deleted and will return false
	 */
	synchronized public boolean deleteTask(long queueId) {
		return DataStore.get().deleteTask(queueId);
	}
	
	/**
	 * Attempt to assign all pending tasks to a client; tasks that can't be assigned are simply left alone
	 * @param ignoreReturned If true, ignore tasks that have been returned and don't try to reassign them to a task client
	 */
	synchronized void startTasks(boolean ignoreReturned) {
		DataStore ds = DataStore.get();
		Config cfg = Config.get();
		PendingTask[] tasks = ds.getPendingTasks(ignoreReturned);
		for(PendingTask t : tasks) {
			QueuedTask qt = null;
			Client assignedClnt = null;
			Client[] clnts = ds.getClientsForTask(t.getTaskId());
			for(Client c : clnts) {
				if(c.getState() != Client.State.ONLINE) {
					LOG.info("Client offline, skipping: " + c);
					continue;
				}
				AgentClient agent = null;
				try {
					agent = new AgentClient(c, Config.get().getLogPkg());
					Client clnt = agent.ping();
					if(clnt == null) {
						LOG.error("Ping of online client failed, skipping: " + c);
						c.setState(Client.State.OFFLINE);
						ds.saveClient(c);
						continue;
					}
					clnt.setHost(c.getHost());
					clnt.setPort(c.getPort());
					clnt.setState(Client.State.ONLINE);
					ds.saveClient(clnt);
					c = clnt;
					if(!CronUtils.matches(c.getSchedule())) {
						LOG.warn("Client is disabled via client schedule '" + c.getSchedule() + "';  skipping: " + c);
						continue;
					}
					if(!c.handlesTask(t.getTaskId())) {
						LOG.warn("Client no longer handles task type '" + t.getTaskId() + "'; skipping: " + c);
						continue;
					}
					Task task = c.getTask(t.getTaskId());
					if(!CronUtils.matches(task.getSchedule())) {
						LOG.warn("Task '" + t.getTaskId() + "' is disabled on client via schedule '" + task.getSchedule() + "'; skipping: " + c);
						continue;
					}
					int freeRes = ds.getFreeResources(c);
					if(freeRes < task.getRequiredResources()) {
						LOG.warn("Client does not have enough free resources to perform this task! [" + freeRes + " < " + task.getRequiredResources() + "]; skipping: " + c);
						continue;
					}
					int activeInst = ds.getActiveInstances(t.getTaskId(), c);
					if(activeInst >= task.getMaxInstances()) {
						LOG.warn("Client is already running max instances of '" + t.getTaskId() + "'; skipping: " + c);
						continue;
					}
					qt = new QueuedTask(task, t.getQid(), ds.getMetadata(t.getQid()), t.getCreated(), new Date(), null, QueuedTask.State.STARTED, c, agent.getLocalHost(), cfg.getPort(), Integer.parseInt(API.apiNullUI.configuration.GetServerProperty("sagex/api/RMIPort", "1098")));
					assignedClnt = c;
					if(ds.updateTask(qt)) {
						State state = agent.exe(qt);
						qt.setState(state);
						ds.updateTask(qt);
						LOG.info("Assigned task " + t.getQid() + " of type '" + t.getTaskId() + "' to " + assignedClnt);
					} else
						LOG.error("Failed to update data store with assigned task! [" + c + "]");
					break;
				} catch (IOException e) {
					LOG.error("IOError", e);
					continue;
				} finally {
					if(agent != null)
						agent.close();
				}
			}
			if(qt == null)
				LOG.warn("No clients available to accept task of type '" + t.getTaskId() + "'");
		}
	}

	/**
	 * Update a task in the queue; all fields of the object are saved to the data store
	 * @param qt The task to be updated
	 * @return True if the save was successful or false on any error
	 */
	synchronized public boolean updateTask(QueuedTask qt) {
		boolean rc = DataStore.get().updateTask(qt);
		switch(qt.getState()) {
		case COMPLETED:
		case FAILED:
		case SKIPPED:
		case RETURNED:
			new QueueLauncher(true).start();
		}
		return rc;
	}
	
	synchronized public boolean deleteClient(Client c) {
		return DataStore.get().deleteClient(c);
	}
}
