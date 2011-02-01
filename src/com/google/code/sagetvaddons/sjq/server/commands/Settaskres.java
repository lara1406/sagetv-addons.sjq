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
import com.google.code.sagetvaddons.sjq.shared.QueuedTask;

/**
 * <p>Provides the ability to dynamically set the total used resources for an actively running task
 * <p><pre>
 *    R: QueuedTask
 *    R: int (new used resources)
 *    W: ACK
 * </pre></p>
 * @author dbattams
 * @version $Id$
 */
public final class Settaskres extends Command {

	/**
	 * @param in
	 * @param out
	 */
	public Settaskres(ObjectInputStream in, ObjectOutputStream out) {
		super(in, out);
		// TODO Auto-generated constructor stub
	}

	/* (non-Javadoc)
	 * @see com.google.code.sagetvaddons.sjq.listener.Command#execute()
	 */
	@Override
	public void execute() throws IOException {
		QueuedTask qt;
		try {
			qt = (QueuedTask)getIn().readObject();
		} catch (ClassNotFoundException e) {
			throw new IOException(e);
		}
		int usedRes = getIn().readInt();
		boolean result = TaskQueue.get().setTaskResources(qt, usedRes);
		getOut().writeObject(NetworkAck.get(result ? NetworkAck.OK : NetworkAck.ERR));
		getOut().flush();
	}
}
