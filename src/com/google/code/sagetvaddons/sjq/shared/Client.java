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
package com.google.code.sagetvaddons.sjq.shared;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;

/**
 * Represents a task client
 * @author dbattams
 * @version $Id$
 */
public final class Client implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * Represents the valid state a task client can be in
	 * @author dbattams
	 *
	 */
	static public enum State {
		/**
		 * The task client is online and ready to response to requests from the server
		 */
		ONLINE,
		/**
		 * The task client failed to respond to the last ping from the server
		 */
		OFFLINE,
		/**
		 * Not currently used
		 */
		DISABLED
	}

	/**
	 * The default number of resources for a task client
	 */
	static public int DEFAULT_RESOURCES = 100;
	static public int DEFAULT_PORT = 23344;
	
	/**
	 * The default active schedule for a task client; this value means the task client is always enabled; see the crontab docs for more details
	 */
	static public String DEFAULT_SCHED = "ON";
	
	private String host;
	private State state;
	private int freeResources;
	private String schedule;
	private Date lastUpdate;
	private int maxResources;
	private int port;
	private Collection<Task> tasks;
	private int version;
	private String mapDir;
	
	/**
	 * Default constructor
	 */
	public Client() {
		this("", DEFAULT_PORT);
	}

	/**
	 * Constructor to be used when creating a new Client that uses the default port
	 * @param host The host where this task client is running
	 */
	public Client(String host) {
		this(host, DEFAULT_PORT);
	}
	
	/**
	 * Constructor to be used when creating a new Client to be registered
	 * @param host The host where this task client is running
	 * @param port The port number the task client is listening on
	 */
	public Client(String host, int port) {
		this(host, port, 0, DEFAULT_SCHED, Client.State.OFFLINE, new Date(), DEFAULT_RESOURCES, new Task[0], 0, "");
	}
	
	/**
	 * Constructor
	 * @param host The hostname this task client is reachable at
	 * @param port The port number the task client's agent is listening on
	 * @param freeResources The number of free resources currently available on this client
	 * @param schedule The client's ACTIVE schedule; crontab format
	 * @param state The current state of this client
	 * @param lastUpdate The last time this client was updated
	 * @param maxResources The max number of resources for this client
	 * @param tasks The array of tasks this client is capable of running
	 * @param version The version number of this client
	 * @param mapDir The mapdir setting for this client
	 */
	public Client(String host, int port, int freeResources, String schedule, State state, Date lastUpdate, int maxResources, Task[] tasks, int version, String mapDir) {
		this.host = host;
		this.freeResources = freeResources;
		this.schedule = schedule;
		this.state = state;
		this.lastUpdate = lastUpdate;
		this.maxResources = maxResources;
		if(this.maxResources > 100)
			this.maxResources = 100;
		this.port = port;
		this.tasks = new ArrayList<Task>();
		if(tasks != null)
			for(Task t : tasks)
				this.tasks.add(t);
		this.version = version;
		this.mapDir = mapDir;
	}

	/**
	 * @return the host
	 */
	public String getHost() {
		return host;
	}

	/**
	 * @param host the host to set
	 */
	public void setHost(String host) {
		this.host = host;
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
	 * @return the freeResources
	 */
	public int getFreeResources() {
		return freeResources;
	}

	/**
	 * @param freeResources the freeResources to set
	 */
	public void setFreeResources(int freeResources) {
		this.freeResources = freeResources;
	}

	/**
	 * @return the schedule
	 */
	public String getSchedule() {
		return schedule;
	}

	/**
	 * @param schedule the schedule to set
	 */
	public void setSchedule(String schedule) {
		this.schedule = schedule;
	}

	/**
	 * @return the lastUpdate
	 */
	public Date getLastUpdate() {
		return lastUpdate;
	}

	/**
	 * @param lastUpdate the lastUpdate to set
	 */
	public void setLastUpdate(Date lastUpdate) {
		this.lastUpdate = lastUpdate;
	}

	/**
	 * @return the maxResources
	 */
	public int getMaxResources() {
		return maxResources;
	}

	/**
	 * @param maxResources the maxResources to set
	 */
	public void setMaxResources(int maxResources) {
		this.maxResources = maxResources;
	}
	
	@Override
	public String toString() {
		return "Client[host=" + host + ":" + port + ",state=" + state + ",lastUpdate=" + lastUpdate + "]";
	}

	/**
	 * @return the port
	 */
	public int getPort() {
		return port;
	}

	/**
	 * @param port the port to set
	 */
	public void setPort(int port) {
		this.port = port;
	}

	/**
	 * @return the tasks
	 */
	public Task[] getTasks() {
		return tasks.toArray(new Task[tasks.size()]);
	}

	/**
	 * @param tasks the tasks to set
	 */
	public void setTasks(Task[] tasks) {
		this.tasks.clear();
		if(tasks != null)
			for(Task t : tasks)
				this.tasks.add(t);
	}
	
	/**
	 * Add a new task to the Client; will NOT replace an existing task with the same taskId
	 * @param task The new task to add
	 * @return True if the task was added or false otherwise; will return false if argument is null or if a task with the same taskId already exists for the client
	 */
	public boolean addTask(Task task) {
		if(task == null)
			return false;
		boolean addIt = true;
		for(Task t : tasks) {
			if(t.getId().equals(task.getId())) {
				addIt = false;
				break;
			}
		}
		if(addIt)
			tasks.add(task);
		return addIt;
	}
	
	/**
	 * Determine if this client can run tasks of the given type
	 * @param taskId The type of task to check
	 * @return True if this client can run tasks of type 'taskId' or false otherwise
	 */
	public boolean handlesTask(String taskId) {
		for(Task t : tasks)
			if(t.getId().equals(taskId))
				return true;
		return false;
	}
	
	/**
	 * Get the task for the given taskId
	 * @param taskId The task id to query
	 * @return The Task definition for the given task id on this client or null if this client cannot run tasks of type 'taskId'
	 */
	public Task getTask(String taskId) {
		for(Task t : tasks)
			if(t.getId().equals(taskId))
				return t;
		return null;
	}

	/**
	 * @return the version
	 */
	public int getVersion() {
		return version;
	}

	/**
	 * @param version the version to set
	 */
	public void setVersion(int version) {
		this.version = version;
	}
	
	/**
	 * Get a short description of this client instance
	 * @return The short description
	 */
	public String getDescription() {
		return getHost() + ":" + getPort();
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((host == null) ? 0 : host.hashCode());
		result = prime * result + port;
		result = prime * result + version;
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
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof Client)) {
			return false;
		}
		Client other = (Client) obj;
		if (host == null) {
			if (other.host != null) {
				return false;
			}
		} else if (!host.equals(other.host)) {
			return false;
		}
		if (port != other.port) {
			return false;
		}
		if (version != other.version) {
			return false;
		}
		return true;
	}
	
	/**
	 * <p>Remove a task from the Client's list of tasks, if it exists.</p>
	 * <p>Removes the first task that matches the one given using an equals() comparison</p>
	 * @param t The task to be removed
	 * @return True if a task was removed from the Client's list or false otherwise
	 */
	public boolean removeTask(Task t) {
		return tasks.remove(t);
	}

	/**
	 * @return the mapDir
	 */
	public String getMapDir() {
		return mapDir;
	}

	/**
	 * @param mapDir the mapDir to set
	 */
	public void setMapDir(String mapDir) {
		this.mapDir = mapDir;
	}
}
