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
import java.util.Collection;
import java.util.Map;

import sagex.api.Configuration;
import sagex.api.Global;

import com.google.code.sagetvaddons.sjq.listener.ListenerClient;
import com.google.code.sagetvaddons.sjq.listener.NetworkAck;
import com.google.code.sagetvaddons.sjq.server.DataStore;
import com.google.code.sagetvaddons.sjq.shared.Client;
import com.google.code.sagetvaddons.sjq.shared.QueuedTask;
import com.google.code.sagetvaddons.sjq.shared.Task;

/**
 * Provides synchronized access to the task queue across JVMs via sockets
 * @author dbattams
 * @version $Id$
 */
public final class ServerClient extends ListenerClient {
	
	private DataStore datastore;
	
	/**
	 * <p>Constructor; will connect to the SJQ server running on the SageTV server this app is associated with</p>
	 * <p>If called outside the Sage JVM then it is expected that the sagex-api RMI provider has been properly configured before calling this constructor</p>
	 * @throws IOException If there was an error making the socket connection to the SJQ server
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

	/**
	 * Add a new task to the task queue
	 * @param taskId The task id to be added
	 * @param env The map of env vars to be attached to the task's runtime env; can be empty, but not null
	 * @return The task queue id assigned to the newly inserted task
	 * @throws IOException Thrown if there was an error adding the task to the task queue
	 */
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
	
	/**
	 * Delete a task from the task queue
	 * @param queueId The unique task queue id number to be deleted
	 * @return True if the task was deleted or false otherwise; only tasks in WAITING, RETURNED, or FAILED state can be deleted, any other state will cause false to be returned
	 * @throws IOException Thrown if there were fatal errors with the network connection to the SJQ engine socket
	 */
	public boolean deleteTask(long queueId) throws IOException {
		NetworkAck ack = null;
		ack = sendCmd("RMTASK");
		if(ack.isOk()) {
			getOut().writeLong(queueId);
			getOut().flush();
			ack = (NetworkAck)readObj();
			return ack.isOk();
		} else
			throw new IOException("RMTASK command rejected by server!");
	}
	
	/**
	 * Delete a registered task client
	 * @param clnt The client to be deleted
	 * @return True on success or false otherwise; you cannot delete a task client if it is currently running tasks, false will be returned if you try to do so
	 * @thorws IOException In case of communication errors with the server
	 */
	public boolean deleteClient(Client clnt) throws IOException {
		NetworkAck ack = null;
		ack = sendCmd("RMCLNT");
		if(ack.isOk()) {
			getOut().writeObject(clnt);
			getOut().flush();
			ack = (NetworkAck)readObj();
			return ack.isOk();
		} else
			throw new IOException("RMCLNT command rejected by server!");
	}
	
	/**
	 * Get the list of active tasks in the queue; a task is active if it's in WAITING, RETURNED or RUNNING state
	 * @return The array of active tasks; can be empty in case of error, but never null
	 */
	public QueuedTask[] getActiveQueue() {
		return datastore.getActiveQueue();
	}
	
	/**
	 * Get all the registered clients
	 * @return The array of registered task clients
	 */
	public Client[] getAllClients() {
		return datastore.getAllClients();
	}
	
	/**
	 * Get a client by host/port
	 * @param host The hostname of the client to get
	 * @param port The port the client is listening on
	 * @return The Client or null if the client is not registered
	 */
	public Client getClient(String host, int port) {
		return datastore.getClient(host, port);
	}
	
	/**
	 * Register a new task client or update an existing client
	 * @param clnt The client to register/update
	 * @return True if the registration/update succeeded or false otherwise
	 */
	public boolean saveClient(Client clnt) {
		return datastore.saveClient(clnt);
	}
		
	/**
	 * Get the list of all registered task ids
	 * @return The array of all registered task ids, may be empty in case of error but never null
	 */
	public String[] getRegisteredTaskIds() {
		return datastore.getRegisteredTaskIds();
	}
	
	/**
	 * Kill the given task on the task agent that is running it
	 * @param qt The task to be killed
	 * @return True if the signal was sent successfully or false otherwise; this is an asynchronous command, the return value does not represent if a task was actually killed only if the signal to kill was successfully sent
	 * @throws IOException In case of network comm issues
	 */
	public boolean killTask(QueuedTask qt) throws IOException {
		NetworkAck ack = null;
		ack = sendCmd("KILL");
		if(ack.isOk()) {
			getOut().writeObject(qt);
			getOut().flush();
			ack = (NetworkAck)readObj();
			return ack.isOk();
		} else
			throw new IOException("KILL command rejected by server!");				
	}
	
	/**
	 * Kill all running tasks on the given task client
	 * @param clnt The task client to kill all active tasks on
	 * @return True on successful request, false otherwise; this is an asynchronous command so the return value does not signify if any tasks were actually killed
	 * @throws IOException In case of network comm errors
	 */
	public boolean killAllTasks(Client clnt) throws IOException {
		NetworkAck ack = null;
		ack = sendCmd("KILLALL");
		if(ack.isOk()) {
			getOut().writeObject(clnt);
			getOut().flush();
			ack = (NetworkAck)readObj();
			return ack.isOk();
		} else
			throw new IOException("KILLALL command rejected by server!");		
	}
	
	/**
	 * Request the server pings the given client and updates its status
	 * @param clnt The Client to be pinged by the server
	 * @return True on success or false otherwise; this is an asynchronous command; the return value only denotes if the request was received by the server, it doesn't represent the final status of the ping
	 * @throws IOException If there are network comm errors
	 */
	public boolean pingTaskClient(Client clnt) throws IOException {
		NetworkAck ack = null;
		ack = sendCmd("PINGC");
		if(ack.isOk()) {
			getOut().writeObject(clnt);
			getOut().flush();
			ack = (NetworkAck)readObj();
			return ack.isOk();
		} else
			throw new IOException("PINGC command rejected by server!");
	}
	
	/**
	 * Return the metadata map associated with the given QueuedTask
	 * @param qt The task to get metadata for
	 * @return The task's metadata.  An empty map is returned if the task has no metadata.
	 */
	public Map<String, String> getMetadataForTask(QueuedTask qt) {
		return datastore.getMetadata(qt.getQueueId());
	}
	
	/**
	 * Get a map of all registered tasks; the keys of the map are the task client the task is registered to
	 * @return The map of registered tasks, keyed by Client.getDescription()
	 */
	public Map<String, Collection<Task>> getAllRegisteredTasks() {
		return datastore.getAllRegisteredTasks();
	}
}
