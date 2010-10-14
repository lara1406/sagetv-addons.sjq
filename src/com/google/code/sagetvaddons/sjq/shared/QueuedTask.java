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
package com.google.code.sagetvaddons.sjq.shared;

import java.util.Date;
import java.util.Map;

/**
 * @author dbattams
 *
 */
public class QueuedTask extends Task {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	static public enum State {
		WAITING,
		RUNNING,
		FAILED,
		COMPLETED,
		RETURNED,
		STARTED
	}
	
	private long qId;
	private Map<String, String> metadata;
	private State state;
	private Date created, started, completed;
	private Client assignee;
	private int serverPort;
	private String serverHost;
	
	/**
	 * 
	 */
	public QueuedTask() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param id
	 * @param requiredResources
	 * @param maxInstances
	 * @param schedule
	 * @param executable
	 * @param exeArguments
	 * @param maxTime
	 * @param maxTimeRatio
	 * @param minReturnCode
	 * @param maxReturnCode
	 */
	public QueuedTask(long qId, String id, int requiredResources, int maxInstances,
			String schedule, String executable, String exeArguments,
			long maxTime, float maxTimeRatio, int minReturnCode,
			int maxReturnCode, Map<String, String> metadata, Date created, Date assigned,
			Date completed, State state, Client assignee, String serverHost, int serverPort, String test, String testArgs) {
		super(id, requiredResources, maxInstances, schedule, executable,
				exeArguments, maxTime, maxTimeRatio, minReturnCode,
				maxReturnCode, test, testArgs);
		this.qId = qId;
		this.metadata = metadata;
		this.created = created;
		this.assignee = assignee;
		this.started = assigned;
		this.completed = completed;
		this.state = state;
		this.serverHost = serverHost;
		this.serverPort = serverPort;
	}

	public QueuedTask(Task t, long qId, Map<String, String> metadata, Date created, Date assigned, Date completed, State state, Client assignee, String serverHost, int serverPort) {
		this(qId, t.getId(), t.getRequiredResources(), t.getMaxInstances(), t.getSchedule(), t.getExecutable(), t.getExeArguments(),
				t.getMaxTime(), t.getMaxTimeRatio(), t.getMinReturnCode(), t.getMaxReturnCode(), metadata, created,
				assigned, completed, state, assignee, serverHost, serverPort, t.getTest(), t.getTestArgs());
	}
	
	/**
	 * @return the metadata
	 */
	public Map<String, String> getMetadata() {
		return metadata;
	}

	/**
	 * @param metadata the metadata to set
	 */
	public void setMetadata(Map<String, String> metadata) {
		this.metadata = metadata;
	}

	/**
	 * @return the state
	 */
	public State getState() {
		return state;
	}

	/**
	 * @param state the state to set
	 */
	public void setState(State state) {
		this.state = state;
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

	/**
	 * @return the started
	 */
	public Date getStarted() {
		return started;
	}

	/**
	 * @param started the started to set
	 */
	public void setStarted(Date started) {
		this.started = started;
	}

	/**
	 * @return the completed
	 */
	public Date getCompleted() {
		return completed;
	}

	/**
	 * @param completed the completed to set
	 */
	public void setCompleted(Date completed) {
		this.completed = completed;
	}

	/**
	 * @return the assignee
	 */
	public Client getAssignee() {
		return assignee;
	}

	/**
	 * @param assignee the assignee to set
	 */
	public void setAssignee(Client assignee) {
		this.assignee = assignee;
	}

	/**
	 * @return the taskId
	 */
	public long getQueueId() {
		return qId;
	}

	/**
	 * @param taskId the taskId to set
	 */
	public void setQueueId(long qId) {
		this.qId = qId;
	}

	/**
	 * @return the serverPort
	 */
	public int getServerPort() {
		return serverPort;
	}

	/**
	 * @param serverPort the serverPort to set
	 */
	public void setServerPort(int serverPort) {
		this.serverPort = serverPort;
	}

	/**
	 * @return the serverHost
	 */
	public String getServerHost() {
		return serverHost;
	}

	/**
	 * @param serverHost the serverHost to set
	 */
	public void setServerHost(String serverHost) {
		this.serverHost = serverHost;
	}	
}
