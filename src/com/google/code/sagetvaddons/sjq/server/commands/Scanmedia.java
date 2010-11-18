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

import gkusnick.sagetv.api.API;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.log4j.Logger;

import com.google.code.sagetvaddons.sjq.listener.Command;
import com.google.code.sagetvaddons.sjq.listener.NetworkAck;

/**
 * <p>Trigger a delayed media import scan</p>
 * <p><pre>
 *    W: ACK
 * </pre></p>
 * @author dbattams
 * @version $Id$
 */
public final class Scanmedia extends Command {
	static private final Logger LOG = Logger.getLogger(Scanmedia.class);
	static private final Timer TIMER = new Timer(true);
	static private final class MediaScanner extends TimerTask {
		@Override
		public void run() {
			synchronized(Scanmedia.class) {
				if(task != null) {
					task.cancel();
					task = null;
				}
			}
			API.apiNullUI.global.RunLibraryImportScan(false);
			LOG.info("Media scan started!");
		}
	}
	static private MediaScanner task = null;
	
	/**
	 * @param in
	 * @param out
	 */
	public Scanmedia(ObjectInputStream in, ObjectOutputStream out) {
		super(in, out);
		// TODO Auto-generated constructor stub
	}

	/* (non-Javadoc)
	 * @see com.google.code.sagetvaddons.sjq.agent.commands.Command#execute()
	 */
	@Override
	public void execute() throws IOException {
		getOut().writeUTF(NetworkAck.OK);
		getOut().flush();
		synchronized(Scanmedia.class) {
			if(task == null) {
				task = new MediaScanner();
				TIMER.schedule(task, 1200000);
				LOG.info("Media scan scheduled for 20 minutes from now!");
			}
		}
	}
}
