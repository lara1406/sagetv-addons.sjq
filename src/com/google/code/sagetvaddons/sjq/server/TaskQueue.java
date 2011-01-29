/*
 *      Copyright 2010-2011 Battams, Derek
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
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

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
	
	static private final Timer TIMER = new Timer(true);
	static private final class QueueProcessor extends TimerTask {
		private boolean ignoreReturned;
		
		private QueueProcessor(boolean ignoreReturned) {
			this.ignoreReturned = ignoreReturned;
		}
		
		@Override
		public void run() {
			LOG.info("Running queue processor now!");
			synchronized(TaskQueue.class) {
				if(!ignoreReturned) {
					taskAll.cancel();
					taskAll = null;
					if(taskIgnore != null) {
						taskIgnore.cancel();
						taskIgnore = null;
					}
				} else {
					taskIgnore.cancel();
					taskIgnore = null;
				}
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
		}
	}
	static private QueueProcessor taskAll = null;
	static private QueueProcessor taskIgnore = null;
	
	/**
	 * <p>Return the TaskQueue singleton</p>
	 * <p><b>This class should only be used within the same JVM the SJQv4 server is running in!</b></p>
	 * @return The TaskQueue singleton
	 */
	static final public TaskQueue get() { return INSTANCE; }

	private final Map<Long, String> ARGS = Collections.synchronizedMap(new HashMap<Long, String>());
	
	private TaskQueue() {}
	
	/**
	 * Allow scripts to dynamically modify a task's exe args at runtime
	 * @param taskId The task id whose args are to be modified
	 * @param args The new args; if null, remove the mapping (i.e. just use the preconfigured exe args for the task)
	 * @return True on success or false on failure
	 */
	public boolean setExeArgs(long taskId, String args) {
		if(args == null) {
			ARGS.remove(taskId);
			return true;
		} else {
			boolean isValid = false;
			for(QueuedTask qt : DataStore.get().getActiveQueue()) {
				if(qt.getQueueId() == taskId) {
					isValid = true;
					break;
				}
			}
			if(!isValid) {
				LOG.error("Didn't find active task for " + taskId);
				return false;
			}
			LOG.warn("Set args to '" + args + "' for " + taskId);
			ARGS.put(taskId, args);
			return true;
		}
	}
	
	/**
	 * Get the dynamically set exe args for the given task id
	 * @param taskId The task id to get dynamic args for
	 * @return The dynamic args for the task or null if none were set
	 */
	public String getExeArgs(long taskId) {
		String args = ARGS.get(taskId);
		LOG.warn("Returning '" + args + "' for " + taskId);
		return ARGS.get(taskId);
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
			DataStore ds = DataStore.get();
			long qId = ds.addTask(id, metadata);
			startTasks(true);
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
	void startTasks(boolean ignoreReturned) {
		synchronized(TaskQueue.class) { 
			if(ignoreReturned) {
				if(taskIgnore == null) {
					LOG.info("Scheduling queue processor for ~8 seconds from now!");
					taskIgnore = new QueueProcessor(true);
					TIMER.schedule(taskIgnore, 8000);
				}
			} else {
				if(taskAll == null) {
					LOG.info("Scheduling queue processor for ~8 seconds from now!");
					taskAll = new QueueProcessor(false);
					TIMER.schedule(taskAll, 8000);
				}
			}
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
			ARGS.remove(qt.getQueueId());
			startTasks(true);
		}
		return rc;
	}

	synchronized public boolean deleteClient(Client c) {
		return DataStore.get().deleteClient(c);
	}
}
