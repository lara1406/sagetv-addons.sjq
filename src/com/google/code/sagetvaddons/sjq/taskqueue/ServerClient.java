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
package com.google.code.sagetvaddons.sjq.taskqueue;

import java.io.IOException;
import java.util.Map;

import sagex.api.Configuration;
import sagex.api.Global;

import com.google.code.sagetvaddons.sjq.listener.ListenerClient;
import com.google.code.sagetvaddons.sjq.listener.NetworkAck;
import com.google.code.sagetvaddons.sjq.server.DataStore;
import com.google.code.sagetvaddons.sjq.shared.Client;
import com.google.code.sagetvaddons.sjq.shared.QueuedTask;

/**
 * @author dbattams
 *
 */
public final class ServerClient extends ListenerClient {
	
	private DataStore datastore;
	
	/**
	 * @param host
	 * @param port
	 * @throws IOException
	 */
	public ServerClient() throws IOException {
		super(Global.GetServerAddress(), Integer.parseInt(Configuration.GetServerProperty("sjq4/agent_port", "23347")));
		datastore = DataStore.get();
	}
	
	@Override
	protected void finalize() {
		try {
			close();
		} finally {
			super.finalize();
		}
	}
	
	@Override
	public void close() {
		if(datastore != null)
			datastore.close();
		super.close();
	}
	
	public long addTask(String taskId, Map<String, String> env) throws IOException {
		NetworkAck ack = null;
		ack = sendCmd("ADDTASK");
		if(ack.isOk()) {
			getOut().writeObject(env);
			getOut().writeUTF(taskId);
			getOut().flush();
			ack = (NetworkAck)readObj();
			if(ack.isOk())
				return Long.parseLong(ack.getMsg());
			throw new IOException("Did not receive new task id from server!");
		} else
			throw new IOException("ADDTASK command rejected by server!");
	}
	
	public QueuedTask[] getActiveQueue() {
		return datastore.getActiveQueue();
	}
	
	public Client[] getAllClients() {
		return datastore.getAllClients();
	}
	
	public Client getClient(String host, int port) {
		return datastore.getClient(host, port);
	}
	
	public boolean saveClient(Client clnt) {
		return datastore.saveClient(clnt);
	}
}
