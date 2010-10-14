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

public class ListenerClient {
	static private final Logger LOG = Logger.getLogger(ListenerClient.class);

	private String host;
	private int port;
	private Socket sock;
	private ObjectInputStream in;
	private ObjectOutputStream out;
	private boolean isClosed;
	private boolean isValid;

	public ListenerClient(String host, int port) throws IOException {
		isClosed = false;
		isValid = true;
		this.host = host;
		this.port = port;
		sock = new Socket(this.host, this.port);
		out = new ObjectOutputStream(sock.getOutputStream());
		out.flush();
		in = new ObjectInputStream(sock.getInputStream());
	}

	public NetworkAck sendCmd(String cmd) throws IOException {
		if(!isValid)
			throw new IllegalStateException("A previous failed has invalidated this client's connection!");
		out.writeUTF(cmd);
		out.flush();
		return NetworkAck.get(in.readUTF());
	}

	public Object readObj() throws IOException {
		if(!isValid)
			throw new IllegalStateException("A previous failed has invalidated this client's connection!");
		try {
			return in.readObject();
		} catch (ClassNotFoundException e) {
			throw new IOException(e);
		}
	}
	
	public void sendAck(NetworkAck ack) throws IOException {
		if(!isValid)
			throw new IllegalStateException("A previous failed has invalidated this client's connection!");
		out.writeUTF((ack.isOk() ? NetworkAck.OK : NetworkAck.ERR) + ack.getMsg());
	}
	
	@Override
	protected void finalize() {
		try {
			close();
		} finally {
			try { super.finalize(); } catch(Throwable t) { LOG.error("FinalizeError", t); }
		}
	}

	public String getRemoteHost() {
		return sock.getInetAddress().getHostAddress();
	}
	
	public String getLocalHost() {
		return sock.getLocalAddress().getHostAddress();
	}
	
	public void close() {
		if(isValid && !isClosed) {
			isClosed = true;
			try {
				if(out != null && in != null) {
					out.writeUTF("QUIT");
					out.flush();
					NetworkAck ack = NetworkAck.get(in.readUTF());
					if(ack.isOk())
						LOG.info("Disconnected from " + host + ":" + port);
					else
						LOG.warn("Error disconnecting from " + host + ":" + port + ": " + ack.getMsg());
				}
				if(in != null)
					in.close();
				if(out != null)
					out.close();
				if(sock != null)
					sock.close();
			} catch(IOException e) {
				LOG.warn("IOError", e);
			}
		}
	}

	/**
	 * @return the in
	 */
	protected final ObjectInputStream getIn() {
		return in;
	}

	/**
	 * @param in the in to set
	 */
	protected final void setIn(ObjectInputStream in) {
		this.in = in;
	}

	/**
	 * @return the out
	 */
	protected final ObjectOutputStream getOut() {
		return out;
	}

	/**
	 * @param out the out to set
	 */
	protected final void setOut(ObjectOutputStream out) {
		this.out = out;
	}
	
	protected final void setIsValid(boolean isValid) {
		this.isValid = isValid;
	}
	
	public final boolean isValid() { return isValid; }
}
