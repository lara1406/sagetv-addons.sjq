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
package com.google.code.sagetvaddons.sjq.server;

import org.apache.log4j.PropertyConfigurator;

import com.google.code.sagetvaddons.sjq.listener.Listener;

public final class Agent {
	public static void main(String[] args) throws Exception {
		PropertyConfigurator.configure("sjq.log4j.properties");
		Config cfg = Config.get();
		Listener listener = new Listener("com.google.code.sagetvaddons.sjq.server.commands", cfg.getPort());
		listener.init();
	}

}
