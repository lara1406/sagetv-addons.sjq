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
import com.google.code.sagetvaddons.sjq.network.AgentClient;
import com.google.code.sagetvaddons.sjq.shared.Client;

/**
 * @author dbattams
 *
 */
public class Killall extends Command {
	
	/**
	 * @param in
	 * @param out
	 */
	public Killall(ObjectInputStream in, ObjectOutputStream out) {
		super(in, out);
	}

	/* (non-Javadoc)
	 * @see com.google.code.sagetvaddons.sjq.listener.Command#execute()
	 */
	@Override
	public void execute() throws IOException {
		try {
			Client clnt = (Client)getIn().readObject();
			boolean result;
			if(clnt != null) {
				AgentClient ac = null;
				try {
					ac = new AgentClient(clnt);
					ac.killAll();
					result = true;
				} catch(IOException e) {
					result = false;
				} finally {
					if(ac != null)
						ac.close();
				}
			} else
				result = false;
			getOut().writeObject(NetworkAck.get(result ? NetworkAck.OK : NetworkAck.ERR));
			getOut().flush();
		} catch (ClassNotFoundException e) {
			throw new IOException(e);
		}
	}
}
