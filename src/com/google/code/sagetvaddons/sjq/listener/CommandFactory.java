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
package com.google.code.sagetvaddons.sjq.listener;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Constructor;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

public final class CommandFactory {
	static private final Logger LOG = Logger.getLogger(CommandFactory.class);
	
	@SuppressWarnings("unchecked")
	static public final Command get(String name, String cmdPkg, ObjectInputStream in, ObjectOutputStream out) {
		try {
			Class<Command> cls = (Class<Command>)Class.forName(cmdPkg + "." + StringUtils.capitalize(name.toLowerCase()));
			Constructor<Command> ctor = cls.getConstructor(ObjectInputStream.class, ObjectOutputStream.class);
			return ctor.newInstance(in, out);
		} catch (Exception e) {
			LOG.error("Error", e);
		}
		return null;
	}
}
