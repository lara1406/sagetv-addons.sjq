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
package com.google.code.sagetvaddons.sjq.network;

import java.io.IOException;

import org.apache.log4j.Logger;

import sagex.SageAPI;
import sagex.remote.rmi.RMISageAPI;

import com.google.code.sagetvaddons.sjq.listener.ListenerClient;
import com.google.code.sagetvaddons.sjq.listener.NetworkAck;
import com.google.code.sagetvaddons.sjq.server.DataStore;
import com.google.code.sagetvaddons.sjq.shared.Client;
import com.google.code.sagetvaddons.sjq.shared.QueuedTask;
import com.google.code.sagetvaddons.sjq.shared.QueuedTask.State;

/**
 * Provide a connection to a task client agent; allow execution of commands on the task client
 * @author dbattams
 * @version $Id$
 */
public final class AgentClient extends ListenerClient {
	static private final Logger LOG = Logger.getLogger(AgentClient.class);

	/**
	 * Construcotr
	 * @param clnt The task client to connect to
	 * @throws IOException If there is any error connecting to the given task client
	 */
	public AgentClient(Client clnt) throws IOException {
		super(clnt.getHost(), clnt.getPort());
	}

	/**
	 * Ping the task client; returns an updated Client instance as provided by the task client
	 * @return The current state of the task client or null if the ping failed
	 */
	public Client ping() {
		NetworkAck ack = null;
		Client c = null;
		try {
			ack = sendCmd("PING");
		} catch (IOException e) {
			LOG.error("IOError", e);
			setIsValid(false);
		}
		if(ack != null && ack.isOk()) {
			try {
				c = (Client)readObj();
				sendAck(NetworkAck.get(NetworkAck.OK));
			} catch (IOException e) {
				LOG.error("IOError", e);
				setIsValid(false);
			}
		} else if(ack != null)
			LOG.error(ack.getMsg());
		else
			LOG.error("Received null ack from server!");
		return c;
	}
	
	/**
	 * Assign the given task to the task client
	 * @param qt The Task to assign to the client
	 * @return The state of the task, which should be RUNNING if successfully started by the task client
	 */
	public State exe(QueuedTask qt) {
		NetworkAck ack = null;
		try {
			ack = sendCmd("EXE");
		} catch(IOException e) {
			LOG.error("IOError", e);
			setIsValid(false);
			return null;
		}
		if(ack != null && ack.isOk()) {
			try {
				getOut().writeObject(qt);
				getOut().flush();
				ack = (NetworkAck)readObj();
				return State.valueOf(ack.getMsg());
			} catch (IOException e) {
				LOG.error("IOError", e);
				setIsValid(false);
				return null;
			}
		}
		return null;
	}
	
	/**
	 * Update the settings on the connected client
	 * @param clnt The new settings values for the client
	 * @return True if the setting were updated successfully or false otherwise; the hostname and port settings are never updated and are ignored by the receiving client.  You cannot update the host or port settings over the network.
	 */
	public boolean update(Client clnt) {
		NetworkAck ack = null;
		try {
			ack = sendCmd("UPDATE");
		} catch (IOException e) {
			LOG.error("IOError", e);
			setIsValid(false);
		}
		if(ack != null && ack.isOk()) {
			try {
				getOut().writeObject(clnt);
				getOut().flush();
				ack = (NetworkAck)getIn().readObject();
				return ack.isOk();
			} catch (IOException e) {
				LOG.error("IOError", e);
				setIsValid(false);
			} catch (ClassNotFoundException e) {
				throw new RuntimeException(e);
			}
		} else if(ack != null)
			LOG.error(ack.getMsg());
		else
			LOG.error("Received null ack from server!");
		return false;
	}

	/**
	 * Kill the given task on the client
	 * @param qt The task to kill
	 * @return True on success or false otherwise; false will be returned in case of any error, including if the client is not running the give task
	 */
	public boolean killTask(QueuedTask qt) {
		NetworkAck ack = null;
		try {
			ack = sendCmd("KILL");
		} catch(IOException e) {
			LOG.error("IOError", e);
			setIsValid(false);
			return false;
		}
		if(ack != null && ack.isOk()) {
			try {
				getOut().writeObject(qt);
				getOut().flush();
				ack = (NetworkAck)readObj();
				return ack.isOk();
			} catch(IOException e) {
				LOG.error("IOError", e);
				setIsValid(false);
				return false;
			}
		} else {
			LOG.error("KILL command rejected by agent!");
			return false;
		}
	}

	/**
	 * Kill all the running tasks on the client
	 */
	public void killAll() {
		NetworkAck ack = null;
		try {
			ack = sendCmd("KILLALL");
		} catch(IOException e) {
			LOG.error("IOError", e);
			setIsValid(false);
		}
		if(ack != null && ack.isOk()) {
			try {
				ack = (NetworkAck)readObj();
				if(ack == null || !ack.isOk())
					LOG.error("KILLALL command failed on agent!");
			} catch(IOException e) {
				LOG.error("IOError", e);
				setIsValid(false);
			}
		} else
			LOG.error("KILLALL command rejected by agent!");
	}
	
	/**
	 * Ask the client if it's running the given task
	 * @param qt The task to query about
	 * @return True if the client is running the given task or false otherwise
	 */
	public boolean isTaskActive(QueuedTask qt) {
		NetworkAck ack = null;
		try {
			ack = sendCmd("ISACTIVE");
		} catch(IOException e) {
			LOG.error("IOError", e);
			setIsValid(false);
		}
		if(ack != null && ack.isOk()) {
			try {
				getOut().writeObject(qt);
				getOut().flush();
				ack = (NetworkAck)readObj();
				if(ack != null && ack.isOk())
					return Boolean.parseBoolean(ack.getMsg());
				else
					return false;
			} catch(IOException e) {
				LOG.error("IOError", e);
				setIsValid(false);
				return false;
			}
		} else
			return false;
	}
}
