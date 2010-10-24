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

import gkusnick.sagetv.api.API;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.TimerTask;

import org.apache.log4j.Logger;

import com.google.code.sagetvaddons.sjq.server.network.AgentClient;
import com.google.code.sagetvaddons.sjq.shared.Client;

/**
 * <p>A TimerTask that attempts to ping all registered task client agents and update their status</p>
 * <p>The plugin should register an instance of this class in a java.util.Timer to run periodically.</p>
 * @author dbattams
 * @version $Id$
 */
public final class AgentManager extends TimerTask {
	static private final Logger LOG = Logger.getLogger(AgentManager.class);
	static private final Collection<String> OLD_CLNTS = new ArrayList<String>();
	static private final Collection<String> DEAD_CLNTS = new ArrayList<String>();
	
	/**
	 * Perform the task; attempt to ping all registered task client agents
	 */
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
				String clntId = c.getHost() + ":" + c.getPort();
				DEAD_CLNTS.remove(clntId);
				if(clnt.getVersion() >= Config.get().getMinClientVersion()) {
					c.setState(Client.State.ONLINE);
					OLD_CLNTS.remove(clntId);
				} else {
					c.setState(Client.State.OFFLINE);
					if(!OLD_CLNTS.contains(clntId)) {
						OLD_CLNTS.add(clntId);
						API.apiNullUI.systemMessageAPI.PostSystemMessage(23000, 2, "The task client at " + clntId + " needs to be upgraded.  It will be marked as OFFLINE until you upgrade it. [" + clnt.getVersion() + " < " + Config.get().getMinClientVersion() + "]", Config.get().getSysMsgProps());
					}
				}
				c.setMaxResources(clnt.getMaxResources());
				c.setTasks(clnt.getTasks());
				c.setSchedule(clnt.getSchedule());
			} catch (IOException e) {
				String clntId = c.getHost() + ":" + c.getPort();
				LOG.warn("IO error with client '" + c.getHost() + ":" + c.getPort() + "'; marking OFFLINE!", e);
				c.setState(Client.State.OFFLINE);
				if(!DEAD_CLNTS.contains(clntId)) {
					DEAD_CLNTS.add(clntId);
					API.apiNullUI.systemMessageAPI.PostSystemMessage(23000, 2, "The task client at " + clntId + " is not responding due to: '" + e.getMessage() + "'; it has been marked as OFFLINE, please investigate.", Config.get().getSysMsgProps());
				}
			} finally {
				if(agent != null)
					agent.close();
			}
			c.setLastUpdate(new Date());
			ds.saveClient(c);
		}
	}	
}
