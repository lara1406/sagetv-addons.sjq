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

import com.google.code.sagetvaddons.sjq.server.network.AgentClient;
import com.google.code.sagetvaddons.sjq.shared.Client;
import com.google.code.sagetvaddons.sjq.shared.QueuedTask;
import com.google.code.sagetvaddons.sjq.shared.Task;
import com.google.code.sagetvaddons.sjq.shared.QueuedTask.State;

final public class TaskQueue {
	static private final Logger LOG = Logger.getLogger(TaskQueue.class);
	
	static private final TaskQueue INSTANCE = new TaskQueue();
	static final public TaskQueue get() { return INSTANCE; }
	
	static final public class PendingTask {
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
	
	private TaskQueue() {
		
	}
	
	synchronized public long addTask(String id, Map<String, String> metadata) throws IOException {
		try {
			return DataStore.get().addTask(id, metadata);
		} catch (SQLException e) {
			throw new IOException(e);
		}
	}
	
	synchronized void startTasks() {
		DataStore ds = DataStore.get();
		Config cfg = Config.get();
		PendingTask[] tasks = ds.getPendingTasks();
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
					agent = new AgentClient(c);
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
	
	synchronized public boolean updateTask(QueuedTask qt) {
		return DataStore.get().updateTask(qt);
	}	
}
