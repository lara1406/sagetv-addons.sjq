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

/**
 * Utility class that allows easy testing of Client and Task schedules
 * @author dbattams
 * @version $Id$
 */
final class CronUtils {

	/**
	 * Test a given scheduling pattern against the current system time
	 * @param pattern The pattern to test
	 * @return True if the given pattern matches the current time or false otherwise; an invalid pattern will also return false
	 */
	static boolean matches(String pattern) {
		if("ON".equals(pattern.trim().toUpperCase()))
			return true;
		else if("OFF".equals(pattern.trim().toUpperCase()))
			return false;
		else
			return SchedulingPattern.validate(pattern) && new SchedulingPattern(pattern).match(System.currentTimeMillis());
	}
	
	private CronUtils() {}
}
