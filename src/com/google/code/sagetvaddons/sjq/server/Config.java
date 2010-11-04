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

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

/**
 * A singleton implementation that stores all of the config options for the server plugin
 * @author dbattams
 * @version $Id$
 */
public final class Config {
	static private final Logger LOG = Logger.getLogger(Config.class);
	static private final int MIN_CLNT_VER = 1208;
	static private final String DEFAULT_PROPS = "plugins/sjq/sjq4.properties";
	static private final String REFERENCE_PROPS = "plugins/sjq/sjq4.properties.ref";
	static private final int DEFAULT_PORT = 23347;
	
	static private Config INSTANCE = null;
	
	static private Properties sysMsgProps = null;
	
	/**
	 * Get the Config singleton; load it from the given props file if it hasn't already been loaded
	 * @param propsPath The props file to read the settings from; only used if not previously created
	 * @return The Config singleton
	 */
	static public final Config get(String propsPath) {
		if(INSTANCE == null)
			INSTANCE = new Config(propsPath);
		return INSTANCE; 
	}
	
	/**
	 * Get the Config singleton; create it from the default props file if it hasn't been loaded yet
	 * @return The Config singleton
	 */
	static public final Config get() {
		return get(DEFAULT_PROPS);
	}
	
	private Properties props;
	private int port;
		
	private Config(String propsPath) {
		port = DEFAULT_PORT;
		
		File propsFile = new File(propsPath);
		if(!propsFile.exists()) {
			LOG.warn("Unable to find specified props file! [" + propsFile.getAbsolutePath() + "]");
			LOG.warn("Checking for default props file...");
			propsFile = new File(DEFAULT_PROPS);
			if(!propsFile.exists()) {
				LOG.warn("Unable to find default props file! [" + propsFile.getAbsolutePath() + "]");
				LOG.warn("Creating default props file...");
				File refFile = new File(REFERENCE_PROPS);
				if(!refFile.exists())
					throw new RuntimeException("Reference props file missing!  Your installation appears to be corrupted!");
				try {
					FileUtils.copyFile(refFile, propsFile);
				} catch (IOException e) {
					throw new RuntimeException("Unable to create default props file!", e);
				}
			}
		}
		props = new Properties();
		try {
			props.load(new FileReader(propsFile));
		} catch (IOException e) {
			throw new RuntimeException("Cannot read props file! [" + propsFile.getAbsolutePath() + "]", e);
		}
		parseProps();
	}
	
	private void parseProps() {
		for(Object k : props.keySet()) {
			if(k.toString().toUpperCase().equals("PORT"))
				port = Integer.parseInt(props.get(k).toString());
			else
				LOG.warn("Unrecognized property skipped! [" + k + "]");
		}
	}
	/**
	 * Returns the port that the server agent is listening on
	 * @return the port
	 */
	public int getPort() {
		return port;
	}
	
	/**
	 * Get the minimum task client versioni required to speak with this version of the engine
	 * @return The minimum client version
	 */
	public int getMinClientVersion() {
		return MIN_CLNT_VER;
	}
	
	public Properties getSysMsgProps() {
		synchronized(Config.class) {
			if(sysMsgProps == null) {
				sysMsgProps = new Properties();
				sysMsgProps.setProperty("typename", "SJQv4 Message");
			}
		}
		return sysMsgProps;
	}
}
