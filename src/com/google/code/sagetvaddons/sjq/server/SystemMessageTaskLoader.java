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

import org.apache.log4j.Logger;

import com.google.code.sagetvaddons.metadata.Factory;
import com.google.code.sagetvaddons.sjq.utils.TaskList;

import gkusnick.sagetv.api.SystemMessageAPI.SystemMessage;

final public class SystemMessageTaskLoader implements TaskLoader {
	static private final Logger LOG = Logger.getLogger(SystemMessageTaskLoader.class);
	
	private SystemMessage msg;
	
	SystemMessageTaskLoader(SystemMessage msg) {
		this.msg = msg;
	}
	
	@Override
	public void load() {
		String[] tasks = TaskList.getList(DataStore.get().getSetting(Plugin.OPT_SYSMSG_TASKS, ""));
		for(String task : tasks) {
			try {
				long id = TaskQueue.get().addTask(task, Factory.getMap(msg));
				LOG.info("Added task '" + task + "' to queue for system message! [" + id + "]");
			} catch (IOException e) {
				LOG.error("Unable to add task to queue!");
			}
		}
	}

}
