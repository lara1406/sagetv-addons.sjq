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
package com.google.code.sagetvaddons.sjq.server.network;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.google.code.sagetvaddons.sjq.listener.ListenerClient;
import com.google.code.sagetvaddons.sjq.listener.NetworkAck;
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
