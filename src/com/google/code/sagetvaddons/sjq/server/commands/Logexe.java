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
import com.google.code.sagetvaddons.sjq.server.DataStore;
import com.google.code.sagetvaddons.sjq.shared.QueuedTask;

/**
 * @author dbattams
 *
 */
public final class Logexe extends Command {

	/**
	 * @param in
	 * @param out
	 */
	public Logexe(ObjectInputStream in, ObjectOutputStream out) {
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
			String log = getIn().readUTF();
			NetworkAck ack = null;
			if(DataStore.get().logOutput(qt, "TASK", log))
				ack = NetworkAck.get(NetworkAck.OK);
			else
				ack = NetworkAck.get(NetworkAck.ERR + "Failed to write log to data store!");
			getOut().writeObject(ack);
			getOut().flush();
		} catch (ClassNotFoundException e) {
			throw new IOException(e);
		}
	}

}
