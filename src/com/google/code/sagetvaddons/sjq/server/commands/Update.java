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

import com.google.code.sagetvaddons.sjq.listener.Command;
import com.google.code.sagetvaddons.sjq.listener.NetworkAck;
import com.google.code.sagetvaddons.sjq.server.TaskQueue;
import com.google.code.sagetvaddons.sjq.shared.QueuedTask;

/**
 * <p>Provides the ability to update a QueuedTask in the data store</p>
 * <p><pre>
 *    R: QueuedTask
 *    W: ACK
 * </pre></p>
 * @author dbattams
 * @version $Id$
 */
public class Update extends Command {

	/**
	 * @param in
	 * @param out
	 */
	public Update(ObjectInputStream in, ObjectOutputStream out) {
		super(in, out);
		// TODO Auto-generated constructor stub
	}

	/* (non-Javadoc)
	 * @see com.google.code.sagetvaddons.sjq.listener.Command#execute()
	 */
	@Override
	public void execute() throws IOException {
		try {
			QueuedTask qt = (QueuedTask)getIn().readObject();
			TaskQueue.get().updateTask(qt);
			getOut().writeObject(NetworkAck.get(NetworkAck.OK));
		} catch (ClassNotFoundException e) {
			getOut().writeObject(NetworkAck.get(NetworkAck.ERR + e.getMessage()));
			throw new IOException(e);
		}
	}

}
