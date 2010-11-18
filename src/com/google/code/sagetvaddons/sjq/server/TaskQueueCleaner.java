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

import java.util.TimerTask;

final class TaskQueueCleaner extends TimerTask {

	@Override
	public void run() {
		DataStore ds = DataStore.get();
		ds.cleanCompletedTasks(Integer.parseInt(ds.getSetting(Plugin.OPT_KEEP_COMPLETED_DAYS, Plugin.getDefaultVal(Plugin.OPT_KEEP_COMPLETED_DAYS))), Integer.parseInt(ds.getSetting(Plugin.OPT_KEEP_FAILED_DAYS, Plugin.getDefaultVal(Plugin.OPT_KEEP_FAILED_DAYS))), Integer.parseInt(ds.getSetting(Plugin.OPT_KEEP_SKIPPED_DAYS, Plugin.getDefaultVal(Plugin.OPT_KEEP_SKIPPED_DAYS))));
	}
}
