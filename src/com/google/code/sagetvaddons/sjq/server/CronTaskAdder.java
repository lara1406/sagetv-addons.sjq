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

import it.sauronsoftware.cron4j.Task;
import it.sauronsoftware.cron4j.TaskExecutionContext;

import java.io.IOException;
import java.util.Map;

import org.apache.log4j.Logger;

/**
 * @author dbattams
 *
 */
final class CronTaskAdder extends Task {
	static private final Logger LOG = Logger.getLogger(CronTaskAdder.class);
	
	private String taskId;
	private Map<String, String> env;
	
	/**
	 * 
	 */
	public CronTaskAdder(String taskId, Map<String, String> env) {
		this.taskId = taskId;
		this.env = env;
	}

	/* (non-Javadoc)
	 * @see it.sauronsoftware.cron4j.Task#execute(it.sauronsoftware.cron4j.TaskExecutionContext)
	 */
	@Override
	public void execute(TaskExecutionContext arg0) throws RuntimeException {
		try {
			long id = TaskQueue.get().addTask(taskId, env);
			LOG.info("Task " + id + " of type '" + taskId + "' added to queue via crontab!");
		} catch (IOException e) {
			LOG.error("IOError", e);
		}
	}
}
