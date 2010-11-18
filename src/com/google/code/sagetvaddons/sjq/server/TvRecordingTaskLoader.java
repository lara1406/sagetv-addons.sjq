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
import gkusnick.sagetv.api.MediaFileAPI.MediaFile;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Logger;

import com.google.code.sagetvaddons.metadata.Factory;
import com.google.code.sagetvaddons.sjq.utils.TaskList;

final public class TvRecordingTaskLoader implements TaskLoader {
	static private final Logger LOG = Logger.getLogger(TvRecordingTaskLoader.class);

	static private final String PROP_PREFIX = "SJQ4_";
	
	private String eventId;
	private MediaFile mf;

	TvRecordingTaskLoader(String eventId, MediaFile mf) {
		this.mf = mf;
		this.eventId = eventId;
	}

	@Override
	public void load() {
		if(mf.IsTVFile()) {
			String[] manTasks, favTasks, genTasks;
			Set<String> allTasks = new HashSet<String>();
			if(mf.GetMediaFileAiring().IsManualRecord())
				manTasks = TaskList.getList(mf.GetMediaFileAiring().GetManualRecordProperty(PROP_PREFIX + eventId));
			else
				manTasks = new String[0];
			if(mf.GetMediaFileAiring().IsFavorite())
				favTasks = TaskList.getList(mf.GetMediaFileAiring().GetFavoriteForAiring().GetFavoriteProperty(PROP_PREFIX + eventId));
			else
				favTasks = new String[0];
			genTasks = TaskList.getList(DataStore.get().getSetting(eventId, ""));
			for(Object task : ArrayUtils.addAll(genTasks, ArrayUtils.addAll(manTasks, favTasks)))
				allTasks.add(task.toString());
			if(allTasks.size() > 0) {
				Map<String, String> map = Factory.getMap(API.apiNullUI.mediaFileAPI.Unwrap(mf));
				TaskQueue queue = TaskQueue.get();
				for(String task : allTasks) {
					try {
						long id = queue.addTask(task, map);
						LOG.info("Added task '" + task + "' to queue! [" + id + "]");
					} catch (IOException e) {
						LOG.error("Failed to add task '" + task + "' to queue for MediaFile '" + mf.GetMediaFileID() + "'");
					}
				}
			}
		} else
			LOG.error("Received a non-tv media file, ignoring! [" + mf.GetMediaFileID() + "/" + mf.GetMediaTitle() + "]");
	}	
}
