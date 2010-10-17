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

import it.sauronsoftware.cron4j.SchedulingPattern;
import it.sauronsoftware.cron4j.TaskCollector;
import it.sauronsoftware.cron4j.TaskTable;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

/**
 * A concrete cron4j TaskCollector; this collector reads and processees the SJQv4 crontab file
 * @author dbattams
 * @version $Id$
 */
final class CronTaskCollector implements TaskCollector {
	static private final Logger LOG = Logger.getLogger(CronTaskCollector.class);

	/* (non-Javadoc)
	 * @see it.sauronsoftware.cron4j.TaskCollector#getTasks()
	 */
	@Override
	public TaskTable getTasks() {
		TaskTable tbl = new TaskTable();
		File crontab = Plugin.CRONTAB;
		if(crontab.canRead()) {
			try {
				for(Object line : FileUtils.readLines(crontab)) {
					String[] fields = line.toString().trim().split("\\s");
					if(fields.length < 6 || fields[0].startsWith("#"))
						continue;
					StringBuilder sched = new StringBuilder();
					for(int i = 0; i < 5; ++i)
						sched.append(fields[i] + " ");
					sched.deleteCharAt(sched.length() - 1);
					if(!SchedulingPattern.validate(sched.toString()))
						continue;
					String taskId = fields[5];
					Map<String, String> env = new HashMap<String, String>();
					for(int i = 6; i < fields.length; ++i) {
						String[] pair = fields[i].split("=", 2);
						if(pair.length == 2)
							env.put(pair[0], pair[1]);
						else
							env.put(pair[0], "1");
					}
					tbl.add(new SchedulingPattern(sched.toString()), new CronTaskAdder(taskId, env));
				}
			} catch (IOException e) {
				LOG.error("IOError", e);
			}
		} else
			LOG.warn("Unable to read '" + crontab.getAbsolutePath() + "'; file does not exist or is not readable by SJQ!");
		return tbl;
	}

}
