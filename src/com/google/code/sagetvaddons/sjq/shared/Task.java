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
	static public final String DEFAULT_SCHED = "ON";
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
	private boolean showIcon;
	private boolean genSysMsgOnFailure;
	
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
		genSysMsgOnFailure = false;
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
	 * @param showIcon Boolean flag for STVi to determine if this task should be displayed in STV header
	 * @param genSysMsgOnFailure If true, generate a system message if this task fails
	 */
	public Task(String id, int requiredResources, int maxInstances,
			String schedule, String executable, String exeArguments,
			long maxTime, float maxTimeRatio, int minReturnCode,
			int maxReturnCode, String testExe, String testArgs, boolean showIcon, boolean genSysMsg) {
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
		this.showIcon = showIcon;
		this.genSysMsgOnFailure = genSysMsg;
	}

	/**
	 * Set the sys msg on failure flag
	 * @param b The value of the flag
	 */
	public void setGenSysMsgOnFailure(boolean b) {
		genSysMsgOnFailure = b;
	}
	
	/**
	 * Get the sys msg on failure flag
	 * @return The value of the flag
	 */
	public boolean getGenSysMsgOnFailure() {
		return genSysMsgOnFailure;
	}
	
	/**
	 * Set the show icon status for this task
	 * @param b The value of the status flag
	 */
	public void setShowIcon(boolean b) {
		showIcon = b;
	}
	
	/**
	 * Get the status flag value for this instance
	 * @return The boolean value of the flag
	 */
	public boolean isShowIcon() {
		return showIcon;
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
	 * @param maxInstances the maxInstances to set; cannot be negative
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
	 * @param schedule the schedule to set; cannot be null, cannot be zero length
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

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((exeArguments == null) ? 0 : exeArguments.hashCode());
		result = prime * result
				+ ((executable == null) ? 0 : executable.hashCode());
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		result = prime * result + maxInstances;
		result = prime * result + maxReturnCode;
		result = prime * result + (int) (maxTime ^ (maxTime >>> 32));
		result = prime * result + Float.floatToIntBits(maxTimeRatio);
		result = prime * result + minReturnCode;
		result = prime * result + requiredResources;
		result = prime * result
				+ ((schedule == null) ? 0 : schedule.hashCode());
		result = prime * result + ((test == null) ? 0 : test.hashCode());
		result = prime * result
				+ ((testArgs == null) ? 0 : testArgs.hashCode());
		return result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof Task)) {
			return false;
		}
		Task other = (Task) obj;
		if (exeArguments == null) {
			if (other.exeArguments != null) {
				return false;
			}
		} else if (!exeArguments.equals(other.exeArguments)) {
			return false;
		}
		if (executable == null) {
			if (other.executable != null) {
				return false;
			}
		} else if (!executable.equals(other.executable)) {
			return false;
		}
		if (id == null) {
			if (other.id != null) {
				return false;
			}
		} else if (!id.equals(other.id)) {
			return false;
		}
		if (maxInstances != other.maxInstances) {
			return false;
		}
		if (maxReturnCode != other.maxReturnCode) {
			return false;
		}
		if (maxTime != other.maxTime) {
			return false;
		}
		if (Float.floatToIntBits(maxTimeRatio) != Float
				.floatToIntBits(other.maxTimeRatio)) {
			return false;
		}
		if (minReturnCode != other.minReturnCode) {
			return false;
		}
		if (requiredResources != other.requiredResources) {
			return false;
		}
		if (schedule == null) {
			if (other.schedule != null) {
				return false;
			}
		} else if (!schedule.equals(other.schedule)) {
			return false;
		}
		if (test == null) {
			if (other.test != null) {
				return false;
			}
		} else if (!test.equals(other.test)) {
			return false;
		}
		if (testArgs == null) {
			if (other.testArgs != null) {
				return false;
			}
		} else if (!testArgs.equals(other.testArgs)) {
			return false;
		}
		return true;
	}
}
