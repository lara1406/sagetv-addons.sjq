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

import java.io.Serializable;
import java.util.Date;

/**
 * @author dbattams
 *
 */
public final class Client implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * @author dbattams
	 *
	 */
	static public enum ClientState {
		ONLINE,
		OFFLINE,
		DISABLED
	}

	static public int DEFAULT_RESOURCES = 100;
	static public String DEFAULT_SCHED = "* * * * *";
	
	private String host;
	private ClientState state;
	private int freeResources;
	private String schedule;
	private Date lastUpdate;
	private int maxResources;
	private int port;
	private Task[] tasks;
	
	/**
	 * 
	 */
	public Client() {
	}

	public Client(String host, int port, int freeResources, String schedule, ClientState state, Date lastUpdate, int maxResources, Task[] tasks) {
		this.host = host;
		this.freeResources = freeResources;
		this.schedule = schedule;
		this.state = state;
		this.lastUpdate = lastUpdate;
		this.maxResources = maxResources;
		if(this.maxResources > 100)
			this.maxResources = 100;
		this.port = port;
		this.tasks = tasks;
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
	public ClientState getState() {
		return state;
	}

	/**
	 * @param state the state to set
	 */
	public void setState(ClientState state) {
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
		return tasks;
	}

	/**
	 * @param tasks the tasks to set
	 */
	public void setTasks(Task[] tasks) {
		this.tasks = tasks;
	}
	
	public boolean handlesTask(String taskId) {
		for(Task t : tasks)
			if(t.getId().equals(taskId))
				return true;
		return false;
	}
	
	public Task getTask(String taskId) {
		for(Task t : tasks)
			if(t.getId().equals(taskId))
				return t;
		return null;
	}
}
