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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.log4j.Logger;


public abstract class Command {
	static private final Logger LOG = Logger.getLogger(Command.class);

	private ObjectInputStream in;
	private ObjectOutputStream out;

	public Command(ObjectInputStream in, ObjectOutputStream out) {
		this.out = out;
		this.in = in;
	}

	/**
	 * @return the in
	 */
	public ObjectInputStream getIn() {
		return in;
	}

	/**
	 * @return the out
	 */
	public ObjectOutputStream getOut() {
		return out;
	}

	public NetworkAck readAck() throws IOException {
		return NetworkAck.get(in.readUTF());
	}
	
	abstract public void execute() throws IOException;
}
