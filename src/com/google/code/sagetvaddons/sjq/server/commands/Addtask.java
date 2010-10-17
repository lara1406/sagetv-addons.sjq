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
package com.google.code.sagetvaddons.sjq.server.commands;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Map;

import com.google.code.sagetvaddons.sjq.listener.Command;
import com.google.code.sagetvaddons.sjq.listener.NetworkAck;
import com.google.code.sagetvaddons.sjq.server.TaskQueue;

/**
 * <p>Provides the ability to add a new task to the task queue via the tcp socket</p>
 * <p><pre>
 *    R: Map<String, String> (env)
 *    R: String (taskId)
 *    W: ACK + qId
 * </pre></p>
 * @author dbattams
 * @version $Id$
 */
public final class Addtask extends Command {

	/**
	 * @param in
	 * @param out
	 */
	public Addtask(ObjectInputStream in, ObjectOutputStream out) {
		super(in, out);
		// TODO Auto-generated constructor stub
	}

	/* (non-Javadoc)
	 * @see com.google.code.sagetvaddons.sjq.listener.Command#execute()
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void execute() throws IOException {
		try {
			Map<String, String> env = (Map<String, String>)getIn().readObject();
			String taskId = getIn().readUTF();
			long qId = TaskQueue.get().addTask(taskId, env);
			getOut().writeObject(NetworkAck.get(NetworkAck.OK + qId));
			getOut().flush();
		} catch (ClassNotFoundException e) {
			throw new IOException(e);
		}
	}

}
