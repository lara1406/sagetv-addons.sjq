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
import java.net.Socket;

import org.apache.log4j.Logger;


/**
 * @author dbattams
 *
 */
final class Handler implements Runnable {
	static private final Logger LOG = Logger.getLogger(Handler.class);

	static private final String CMD_QUIT = "QUIT";
	
	private Socket sock;
	private ObjectInputStream in;
	private ObjectOutputStream out;
	private String cmdPkg;

	Handler(Socket sock, String cmdPkg) throws IOException {
		this.sock = sock;
		this.out = new ObjectOutputStream(sock.getOutputStream());
		this.out.flush();
		this.in = new ObjectInputStream(sock.getInputStream());
		this.cmdPkg = cmdPkg;
	}

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		try {
			String cmdName = null;
			Command cmd = null;
			while(cmdName == null || !cmdName.toUpperCase().equals(CMD_QUIT)) {
				cmdName = in.readUTF();
				LOG.info("CMD: " + cmdName + " :: PEER: " + sock.getInetAddress());
				cmd = CommandFactory.get(cmdName, cmdPkg, in, out);
				if(cmd != null) {
					if(!cmdName.toUpperCase().equals(CMD_QUIT)) {
						out.writeUTF(NetworkAck.OK);
						out.flush();
					}
					cmd.execute();
				} else {
					out.writeUTF(NetworkAck.ERR + "Unrecognized command [" + cmdName + "]");
					out.flush();
				}
			}
		} catch (IOException e) {
			LOG.error("IOError", e);
		} finally {
			try {
				if(in != null)
					in.close();
				if(out != null)
					out.close();
				if(sock != null)
					sock.close();
			} catch(IOException e) {
				LOG.error("IOError", e);
			}
		}
	}
}
