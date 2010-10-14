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

import java.io.IOException;
import java.util.Date;
import java.util.TimerTask;

import org.apache.log4j.Logger;

import com.google.code.sagetvaddons.sjq.server.network.AgentClient;
import com.google.code.sagetvaddons.sjq.shared.Client;

/**
 * @author dbattams
 *
 */
public final class AgentManager extends TimerTask {
	static private final Logger LOG = Logger.getLogger(AgentManager.class);
	
	@Override
	public void run() {
		DataStore ds = DataStore.get();
		for(Client c : ds.getAllClients()) {
			LOG.info("Pinging " + c);
			AgentClient agent = null;
			Client clnt = null;
			try {
				agent = new AgentClient(c);
				clnt = agent.ping();
				c.setState(Client.ClientState.ONLINE);
				c.setMaxResources(clnt.getMaxResources());
				c.setTasks(clnt.getTasks());
				c.setSchedule(clnt.getSchedule());
			} catch (IOException e) {
				LOG.warn("IO error with client '" + c.getHost() + ":" + c.getPort() + "'; marking OFFLINE!", e);
				c.setState(Client.ClientState.OFFLINE);
			} finally {
				if(agent != null)
					agent.close();
			}
			c.setLastUpdate(new Date());
			ds.saveClient(c);
		}
	}	
}
