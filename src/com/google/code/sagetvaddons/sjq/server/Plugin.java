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

import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import sage.SageTVPlugin;
import sage.SageTVPluginRegistry;

import com.google.code.sagetvaddons.sjq.listener.Listener;

public final class Plugin implements SageTVPlugin {
	static private final Logger LOG = Logger.getLogger(Plugin.class);

	private Timer timer;
	private Thread agent;
	
	public Plugin(SageTVPluginRegistry reg) {
		PropertyConfigurator.configure("plugins/sjq/sjq.log4j.properties");
		timer = null;
		agent = null;
	}
	
	@Override
	public void destroy() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String getConfigHelpText(String arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getConfigLabel(String arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String[] getConfigOptions(String arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String[] getConfigSettings() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getConfigType(String arg0) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getConfigValue(String arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String[] getConfigValues(String arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void resetConfig() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setConfigValue(String arg0, String arg1) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setConfigValues(String arg0, String[] arg1) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void start() {
		if(timer != null)
			timer.cancel();
		timer = new Timer(true);
		timer.schedule(new TimerTask() {

			@Override
			public void run() {
				TaskQueue.get().startTasks();
			}
			
		}, 15000, 30000);
		timer.schedule(new AgentManager(), 15000, 120000);
		
		if(agent != null)
			agent.interrupt();
		agent = new Thread() {
			@Override
			public void run() {
				try {
					Thread.sleep(5500);
					LOG.info("Starting server agent...");
					new Listener("com.google.code.sagetvaddons.sjq.server.commands", Config.get().getPort()).init();
					LOG.warn("Server agent has stopped!");
				} catch (Exception e) {
					LOG.fatal("Server agent has stopped unexpectedly!", e);
				}
			}
		};
		agent.start();
	}

	@Override
	public void stop() {
		if(timer != null) {
			timer.cancel();
			timer = null;
		}
		if(agent != null) {
			agent.interrupt();
			agent = null;
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void sageEvent(String arg0, Map arg1) {
		// TODO Auto-generated method stub
		
	}

}
