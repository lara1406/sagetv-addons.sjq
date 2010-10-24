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
package com.google.code.sagetvaddons.sjq.utils;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;

/**
 * Static utility methods to parse and edit task lists
 * @author dbattams
 * @version $Id$
 */
public final class TaskList {

	static private final char LIST_SEP = ',';
	
	/**
	 * Add a new task to the given list
	 * @param taskId The task to add to the list
	 * @param list The list to add to; a delimited string
	 * @return The new list with the new task added UNLESS that task was already in the list
	 */
	static public String addTask(String taskId, String list) {
		Object[] tasks = getList(list);
		if(!ArrayUtils.contains(tasks, taskId))
			tasks = ArrayUtils.add(tasks, taskId);
		return StringUtils.join(tasks, LIST_SEP);
	}
	
	/**
	 * Remove a task from a list
	 * @param taskId The task to be removed
	 * @param list The list to remove the task from
	 * @return The updated list with the task removed if it was in the list, otherwise returns a copy of list
	 */
	static public String removeTask(String taskId, String list) {
		return StringUtils.join(ArrayUtils.removeElement(getList(list), taskId), LIST_SEP);
	}

	/**
	 * Check if a task is in a list
	 * @param taskId The task to check for
	 * @param list The list to check
	 * @return True if taskId exists in the delimited list or false otherwise
	 */
	static public boolean containsTask(String taskId, String list) {
		return ArrayUtils.contains(getList(list), taskId);
	}
	
	/**
	 * Return an array of task ids for the given delimited list of tasks
	 * @param input The delimited string of tasks
	 * @return An array of strings, each element representing a task id from the given list
	 */
	static public String[] getList(String input) {
		if(input != null && input.length() > 0)
			return input.split("\\s*" + LIST_SEP + "\\s*");
		return new String[0];
	}
	
	private TaskList() {}
}
