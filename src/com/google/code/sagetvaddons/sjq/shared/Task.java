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
package com.google.code.sagetvaddons.sjq.shared;

import java.io.Serializable;

/**
 * A Task represents a unit of work to be performed
 * @author dbattams
 * @version $Id$
 */
public class Task implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	/**
	 * The default task id
	 */
	static public final String DEFAULT_ID = null;
	/**
	 * The default number of resources required to perform a task
	 */
	static public final int DEFAULT_REQ_RES = 100;
	/**
	 * The default max number of instances of a task that can run in parallel on a single task client
	 */
	static public final int DEFAULT_MAX_INST = 1;
	/**
	 * The default task schedule, in crontab format; this value means that the task is always enabled
	 */
	static public final String DEFAULT_SCHED = "* * * * *";
	/**
	 * The default exe name for a task
	 */
	static public final String DEFAULT_EXE = null;
	/**
	 * The default exe command line args
	 */
	static public final String DEFAULT_EXE_ARGS = "";
	/**
	 * The default max execution time for a task, in seconds
	 */
	static public final long DEFAULT_MAX_TIME = 86400L;
	/**
	 * Not implemented
	 */
	static public final float DEFAULT_MAX_TIME_RATIO = 1.0F;
	/**
	 * The default minimum successful return code for the exe
	 */
	static public final int DEFAULT_MIN_RC = 0;
	/**
	 * The default maximum successful return code for the exe
	 */
	static public final int DEFAULT_MAX_RC = DEFAULT_MIN_RC;
	/**
	 * The default test script for a task
	 */
	static public final String DEFAULT_TEST = null;
	
	private String id;
	private int requiredResources;
	private int maxInstances;
	private String schedule;
	private String executable;
	private String exeArguments;
	private long maxTime;
	private float maxTimeRatio;
	private int minReturnCode;
	private int maxReturnCode;
	private String test;
	private String testArgs;
	
	/**
	 * Default constructor; set state to default values
	 */
	public Task() {
		id = DEFAULT_ID;
		requiredResources = DEFAULT_REQ_RES;
		maxInstances = DEFAULT_MAX_INST;
		schedule = DEFAULT_SCHED;
		executable = DEFAULT_EXE;
		exeArguments = DEFAULT_EXE_ARGS;
		maxTime = DEFAULT_MAX_TIME;
		maxTimeRatio = DEFAULT_MAX_TIME_RATIO;
		minReturnCode = DEFAULT_MIN_RC;
		maxReturnCode = DEFAULT_MAX_RC;
		test = DEFAULT_TEST;
		testArgs = "";
	}
	
	/**
	 * Constructor
	 * @param id The task id
	 * @param requiredResources The number of resources required to run this task
	 * @param maxInstances The max instances of this task that can run in parallel
	 * @param schedule The task ENABLED schedule; crontab format
	 * @param executable The exe path; relative paths are relative to the base install dir of task client
	 * @param exeArguments The command line args for the exe
	 * @param maxTime The maximum number of seconds to run this task
	 * @param maxTimeRatio Not implemented; just pass 1.0
	 * @param minReturnCode The min successful return code for the exe
	 * @param maxReturnCode The max successful return code for the exe
	 * @param testExe The path of the pretest script to execute or null if one shouldn't be run
	 * @param testArgs Test args to pass to the pretest script
	 */
	public Task(String id, int requiredResources, int maxInstances,
			String schedule, String executable, String exeArguments,
			long maxTime, float maxTimeRatio, int minReturnCode,
			int maxReturnCode, String testExe, String testArgs) {
		this.id = id;
		this.requiredResources = requiredResources;
		this.maxInstances = maxInstances;
		this.schedule = schedule;
		this.executable = executable;
		this.exeArguments = exeArguments;
		this.maxTime = maxTime;
		this.maxTimeRatio = maxTimeRatio;
		this.minReturnCode = minReturnCode;
		this.maxReturnCode = maxReturnCode;
		this.test = testExe;
		this.testArgs = testArgs;
	}

	/**
	 * @return the id
	 */
	public String getId() {
		return id;
	}
	/**
	 * @param id the id to set; cannot be null, cannot be zero length, cannot contain spaces, restrict to numbers and letters
	 */
	public void setId(String id) {
		this.id = id;
	}
	/**
	 * @return the requiredResources
	 */
	public int getRequiredResources() {
		return requiredResources;
	}
	/**
	 * @param requiredResources the requiredResources to set
	 */
	public void setRequiredResources(int requiredResources) {
		this.requiredResources = requiredResources;
	}
	/**
	 * @return the maxInstances
	 */
	public int getMaxInstances() {
		return maxInstances;
	}
	/**
	 * @param maxInstances the maxInstances to set
	 */
	public void setMaxInstances(int maxInstances) {
		this.maxInstances = maxInstances;
	}
	/**
	 * @return the schedule
	 */
	public String getSchedule() {
		return schedule;
	}
	/**
	 * @param schedule the schedule to set
	 */
	public void setSchedule(String schedule) {
		this.schedule = schedule;
	}
	/**
	 * @return the executable
	 */
	public String getExecutable() {
		return executable;
	}
	/**
	 * @param executable the executable to set; cannot be null nor zero length
	 */
	public void setExecutable(String executable) {
		this.executable = executable;
	}
	/**
	 * @return the exeArguments
	 */
	public String getExeArguments() {
		return exeArguments;
	}
	/**
	 * @param exeArguments the exeArguments to set
	 */
	public void setExeArguments(String exeArguments) {
		this.exeArguments = exeArguments;
	}
	
	@Override
	public String toString() {
		StringBuilder str = new StringBuilder();
		str.append("Task[id=" + id);
		str.append(", reqRes=" + requiredResources);
		str.append(", maxInst=" + maxInstances);
		str.append(", sched=" + schedule);
		str.append(", maxTime=" + maxTime);
		str.append(", maxTimeRatio=" + maxTimeRatio);
		str.append(", rc=" + minReturnCode + "-" + maxReturnCode);
		str.append(", exe=" + executable);
		str.append(", exeArgs=" + exeArguments);
		str.append(", test=" + test);
		str.append(", testArgs=" + testArgs + "]");
		return str.toString();
	}

	/**
	 * @return the maxTime
	 */
	public long getMaxTime() {
		return maxTime;
	}

	/**
	 * @param maxTime the maxTime to set, in seconds; must be greater than zero
	 */
	public void setMaxTime(long maxTime) {
		this.maxTime = maxTime;
	}

	/**
	 * @return the maxTimeRatio
	 */
	public float getMaxTimeRatio() {
		return maxTimeRatio;
	}

	/**
	 * @param maxTimeRatio the maxTimeRatio to set; currently not implemented, but will be; value must be greater than zero
	 */
	public void setMaxTimeRatio(float maxTimeRatio) {
		this.maxTimeRatio = maxTimeRatio;
	}

	/**
	 * @return the minReturnCode
	 */
	public int getMinReturnCode() {
		return minReturnCode;
	}

	/**
	 * @param minReturnCode the minReturnCode to set; cannot be negative
	 */
	public void setMinReturnCode(int minReturnCode) {
		this.minReturnCode = minReturnCode;
	}

	/**
	 * @return the maxReturnCode
	 */
	public int getMaxReturnCode() {
		return maxReturnCode;
	}

	/**
	 * @param maxReturnCode the maxReturnCode to set; cannot be negative, must be >= minReturnCode
	 */
	public void setMaxReturnCode(int maxReturnCode) {
		this.maxReturnCode = maxReturnCode;
	}

	/**
	 * @return the test
	 */
	public String getTest() {
		return test;
	}

	/**
	 * @param test the test to set
	 */
	public void setTest(String test) {
		this.test = test;
	}

	/**
	 * @return the testArgs
	 */
	public String getTestArgs() {
		return testArgs;
	}

	/**
	 * @param testArgs the testArgs to set
	 */
	public void setTestArgs(String testArgs) {
		this.testArgs = testArgs;
	}
}

