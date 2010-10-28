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
import it.sauronsoftware.cron4j.Scheduler;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import sage.SageTVPlugin;
import sage.SageTVPluginRegistry;

import com.google.code.sagetvaddons.sjq.listener.Listener;

/**
 * SageTVPlugin implementation for SJQv4
 * @author dbattams
 * @version $Id$
 */
public final class Plugin implements SageTVPlugin {
	static private final Logger LOG = Logger.getLogger(Plugin.class);
	
	static private final String REC_STARTED = "RecordingStarted";
	static private final String NEW_SEGMENT = "RecordingSegmentAdded";
	static private final String MEDIA_IMPORTED = "MediaFileImported";
	static private final String SYS_MSG_POSTED = "SystemMessagePosted";
	static private String[] EVENTS = new String[] {REC_STARTED, NEW_SEGMENT, MEDIA_IMPORTED, SYS_MSG_POSTED};
	
	/**
	 * The location of the SJQv4 crontab file to be used; relative to the base install dir of SageTV
	 */
	static public final File CRONTAB = new File("plugins/sjq/crontab");
	
	private Timer timer;
	private Thread agent;
	private Scheduler crontab;
	
	/**
	 * Constructor
	 * @param reg The plugin registry
	 */
	public Plugin(SageTVPluginRegistry reg) {
		PropertyConfigurator.configure("plugins/sjq/sjq.log4j.properties");
		timer = null;
		agent = null;
		if(!CRONTAB.exists()) {
			try {
				FileUtils.touch(CRONTAB);
			} catch (IOException e) {
				LOG.error("Unable to create crontab file!", e);
			}
		}
		crontab = new Scheduler();
		crontab.setDaemon(true);
		crontab.addTaskCollector(new CronTaskCollector());
		API.apiNullUI.configuration.SetServerProperty("sjq4/enginePort", String.valueOf(Config.get().getPort()));
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
		// Create the timer thread, which will run the agent pinger and the task queue threads periodically
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
		timer.schedule(new ActiveTaskManager(), 45000, 60000);
		LOG.info("SJQ timer thread has been started!");
		
		// Start the server agent
		if(agent != null)
			agent.interrupt();
		agent = new Thread() {
			@Override
			public void run() {
				try {
					Thread.sleep(5500);
					new Listener("com.google.code.sagetvaddons.sjq.server.commands", Config.get().getPort()).init();
					LOG.warn("SJQ server agent has stopped!");
				} catch (Exception e) {
					LOG.fatal("SJQ server agent has stopped unexpectedly!", e);
				}
			}
		};
		agent.start();
		LOG.info("Server agent has started!");
		
		// Start the crontab
		if(!crontab.isStarted())
			crontab.start();
		LOG.info("Server crontab has started!");

		SageTVPluginRegistry reg = (SageTVPluginRegistry)API.apiNullUI.pluginAPI.GetSageTVPluginRegistry();
		for(String event : EVENTS)
			reg.eventSubscribe(this, event);
	}

	@Override
	public void stop() {
		// Kill everything we started in start()
		if(timer != null) {
			timer.cancel();
			timer = null;
			LOG.info("SJQ timer thread has been stopped!");
		}
		if(agent != null) {
			agent.interrupt();
			agent = null;
			LOG.info("SJQ server agent has been stopped!");
		}
		if(crontab.isStarted()) {
			LOG.info("Stopping SJQ crontab...");
			crontab.stop();
			LOG.info("SJQ crontab has been stopped!");
		}
		
		SageTVPluginRegistry reg = (SageTVPluginRegistry)API.apiNullUI.pluginAPI.GetSageTVPluginRegistry();
		for(String event : EVENTS)
			reg.eventUnsubscribe(this, event);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void sageEvent(String arg0, Map arg1) {
		LOG.info("Event received: " + arg0);
		TaskLoader loader = null;
		if(arg0.matches(REC_STARTED + "|" + NEW_SEGMENT))
			loader = new TvRecordingTaskLoader(API.apiNullUI.mediaFileAPI.Wrap(arg1.get("MediaFile")));
		else if(arg0.equals(MEDIA_IMPORTED))
			loader = new ImportedMediaTaskLoader(API.apiNullUI.mediaFileAPI.Wrap(arg1.get("MediaFile")));
		else if(arg0.equals(SYS_MSG_POSTED))
			loader = new SystemMessageTaskLoader(API.apiNullUI.systemMessageAPI.Wrap(arg1.get("SystemMessage")));
		else
			LOG.warn("Event ignored: " + arg0);
		if(loader != null)
			loader.load();
	}
}
