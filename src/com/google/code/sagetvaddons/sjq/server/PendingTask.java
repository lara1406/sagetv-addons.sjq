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

import java.util.Date;

/**
 * Details about a pending task; a task is Pending when it's in WAITING or RETURNED state
 * @author dbattams
 *
 */
public final class PendingTask {
	private long qId;
	private String taskId;
	private Date created;
	
	PendingTask(long qId, String taskId, Date created) {
		this.qId = qId;
		this.taskId = taskId;
		this.created = created;
	}

	/**
	 * @return the qId
	 */
	public long getQid() {
		return qId;
	}

	/**
	 * @param qId the qId to set
	 */
	public void setQid(long qId) {
		this.qId = qId;
	}

	/**
	 * @return the taskId
	 */
	public String getTaskId() {
		return taskId;
	}

	/**
	 * @param taskId the taskId to set
	 */
	public void setTaskId(String taskId) {
		this.taskId = taskId;
	}

	/**
	 * @return the created
	 */
	public Date getCreated() {
		return created;
	}

	/**
	 * @param created the created to set
	 */
	public void setCreated(Date created) {
		this.created = created;
	}
}