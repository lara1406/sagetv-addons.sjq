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
 * A QueuedTask is a Task which has been added to the queue; it contains additional deatils such as which client is running it, the state of the task, what time it was created, etc.
 * @author dbattams
 * @version $Id$
 */
public class QueuedTask extends Task {
	/**
	 * Valid types of task output
	 * @author dbattams
	 *
	 */
	static public enum OutputType {
		/**
		 * Denotes output from the test execution of a task
		 */
		TEST,
		/**
		 * Denotes output from the exe execution of a task
		 */
		TASK
	}
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * Valid states for a QueuedTask
	 * @author dbattams
	 *
	 */
	static public enum State {
		/**
		 * The task is waiting to be assigned to a task client for execution
		 */
		WAITING,
		/**
		 * The task is currently executing on a task client
		 */
		RUNNING,
		/**
		 * The task has completed execution and failed
		 */
		FAILED,
		/**
		 * The task has completed execution successfully
		 */
		COMPLETED,
		/**
		 * <p>The task was assigned to a client, but was returned because the assigned task client deemed it was "unsafe" to run the task when assigned</p>
		 * <p>Tasks in RETURNED state will be assigned to another task client, possibly the same one, the next time the task queue is executed.</p> 
		 */
		RETURNED,
		/**
		 * The task has been assigned to a task client and the server is waiting to get the initial response back from the client
		 */
		STARTED,
		/**
		 * The task was marked as skipped by a task client; this happens when a task client's pretest marks the task as skipped (i.e. a COMSKIP task might do this when it finds an edl for the recording it was about to comskip)
		 */
		SKIPPED
	}
	
	private long qId;
	private Map<String, String> metadata;
	private State state;
	private Date created, started, completed;
	private Client assignee;
	private int serverPort;
	private String serverHost;
	private int rmiPort;
	
	/**
	 * Default constructor
	 */
	public QueuedTask() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * Constructor
	 * @param qId The unique task queue id assigned to this task
	 * @param id The task id for the task
	 * @param requiredResources The number of resources required to perform this task on the assigned task client
	 * @param maxInstances The max number of instances of this task that can run at once on the assigned task client
	 * @param schedule The task's active schedule; crontab format
	 * @param executable The executable path for this task
	 * @param exeArguments The command line arguments for the exe
	 * @param maxTime The maximum amount of time this task can run before being killed; ignored for SJQ script exes
	 * @param maxTimeRatio Not implemented
	 * @param minReturnCode The minimum return code considered a success
	 * @param maxReturnCode The maximum return code considered a success
	 * @param metadata The env vars to be injected into the runtime env for BOTH the exe and the test script
	 * @param created When this task was added to the queue
	 * @param assigned The date this task was last assigned to a task client
	 * @param completed The date this task was last completed/returned by a task client
	 * @param state The current state of the task
	 * @param assignee The last Client assigned this task or null if the task is currently assigned to no one
	 * @param serverHost The hostname of the SJQ server that assigned this task
	 * @param serverPort The port of the SJQ server agent that assigned this task
	 * @param test The name of a pretest script to execute for this task or null if no test is to be ran
	 * @param testArgs Optional args to be provided to the test script; currently not implemented
	 * @param rmiPort The sagex-api RMI port for the Sage server hosting the SJQ server that assigned this task
	 */
	public QueuedTask(long qId, String id, int requiredResources, int maxInstances,
			String schedule, String executable, String exeArguments,
			long maxTime, float maxTimeRatio, int minReturnCode,
			int maxReturnCode, Map<String, String> metadata, Date created, Date assigned,
			Date completed, State state, Client assignee, String serverHost, int serverPort, String test, String testArgs, int rmiPort) {
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
		this.rmiPort = rmiPort;
	}

	/**
	 * 
	 * @param t The base Task instance this QueuedTask is based on
	 * @param qId The unique task queue id assigned to this task
	 * @param metadata The env var map for the task
	 * @param created The date this task was added to the queue
	 * @param assigned The date it was last assigned to a client
	 * @param completed The date it was last returned by a client
	 * @param state The state of the task
	 * @param assignee The last client assigned this task or null if never assigned to a client
	 * @param serverHost The SJQ server that assigned this task
	 * @param serverPort The port that the SJQ server agent is listening on
	 * @param rmiPort The sagex-api RMI port for the Sage server host running the SJQ server that assigned this task
	 */
	public QueuedTask(Task t, long qId, Map<String, String> metadata, Date created, Date assigned, Date completed, State state, Client assignee, String serverHost, int serverPort, int rmiPort) {
		this(qId, t.getId(), t.getRequiredResources(), t.getMaxInstances(), t.getSchedule(), t.getExecutable(), t.getExeArguments(),
				t.getMaxTime(), t.getMaxTimeRatio(), t.getMinReturnCode(), t.getMaxReturnCode(), metadata, created,
				assigned, completed, state, assignee, serverHost, serverPort, t.getTest(), t.getTestArgs(), rmiPort);
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

	/**
	 * @return the rmiPort
	 */
	public int getRmiPort() {
		return rmiPort;
	}

	/**
	 * @param rmiPort the rmiPort to set
	 */
	public void setRmiPort(int rmiPort) {
		this.rmiPort = rmiPort;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + (int) (qId ^ (qId >>> 32));
		result = prime * result
				+ ((serverHost == null) ? 0 : serverHost.hashCode());
		result = prime * result + serverPort;
		return result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (!super.equals(obj)) {
			return false;
		}
		if (!(obj instanceof QueuedTask)) {
			return false;
		}
		QueuedTask other = (QueuedTask) obj;
		if (qId != other.qId) {
			return false;
		}
		if (serverHost == null) {
			if (other.serverHost != null) {
				return false;
			}
		} else if (!serverHost.equals(other.serverHost)) {
			return false;
		}
		if (serverPort != other.serverPort) {
			return false;
		}
		return true;
	}	
}
