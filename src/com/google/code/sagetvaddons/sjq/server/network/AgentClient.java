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

public final class AgentClient extends ListenerClient {
	static private final Logger LOG = Logger.getLogger(AgentClient.class);

	public AgentClient(Client clnt) throws IOException {
		super(clnt.getHost(), clnt.getPort());
	}

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
		}
		return c;
	}
	
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
}
