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

import java.io.Serializable;

public final class NetworkAck implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	static public final String OK = "OK |";
	static public final String ERR = "ERR|";
	
	static public final NetworkAck get(String msg) {
		if(msg.startsWith(OK)) {
			return new NetworkAck(msg.substring(OK.length()), true);
		} else	
			return new NetworkAck(msg.substring(ERR.length()), false);
	}
	
	private String msg;
	private boolean isOk;
	
	public NetworkAck() {}
	
	private NetworkAck(String msg, boolean isOk) {
		this.msg = msg;
		this.isOk = isOk;
	}

	/**
	 * @return the msg
	 */
	public String getMsg() {
		return msg;
	}

	/**
	 * @param msg the msg to set
	 */
	public void setMsg(String msg) {
		this.msg = msg;
	}

	/**
	 * @return the isOk
	 */
	public boolean isOk() {
		return isOk;
	}

	/**
	 * @param isOk the isOk to set
	 */
	public void setOk(boolean isOk) {
		this.isOk = isOk;
	}
}
