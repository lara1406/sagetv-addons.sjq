/*
 *      Copyright 2011 Battams, Derek
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
package com.google.code.sagetvaddons.sjq.server.commands;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import com.google.code.sagetvaddons.sjq.listener.Command;
import com.google.code.sagetvaddons.sjq.listener.NetworkAck;
import com.google.code.sagetvaddons.sjq.server.TaskQueue;

/**
 * <p>Provides the ability to dynamic exe args as set by the task's test script
 * <p><pre>
 *    R: long (task id)
 *    W: boolean (true if override exists or false if none is defined)
 *    W: String (args, read but ignored if boolean above = false)
 * </pre></p>
 * @author dbattams
 * @version $Id$
 */
public final class Getargs extends Command {

	/**
	 * @param in
	 * @param out
	 */
	public Getargs(ObjectInputStream in, ObjectOutputStream out) {
		super(in, out);
		// TODO Auto-generated constructor stub
	}

	/* (non-Javadoc)
	 * @see com.google.code.sagetvaddons.sjq.listener.Command#execute()
	 */
	@Override
	public void execute() throws IOException {
		long taskId = getIn().readLong();
		String args = TaskQueue.get().getExeArgs(taskId);
		boolean hasArgs = true;
		if(args == null) {
			args = "";
			hasArgs = false;
		}
		getOut().writeBoolean(hasArgs);
		getOut().writeUTF(args);
		getOut().writeObject(NetworkAck.get(NetworkAck.OK));
		getOut().flush();
	}
}
