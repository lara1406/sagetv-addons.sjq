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

import gkusnick.sagetv.api.API;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.StringReader;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.spec.RSAPublicKeySpec;
import java.util.Properties;

import javax.crypto.Cipher;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;


/**
 * Keep the honest people honest and the people who really want to crack this software, well go nuts!
 * This version should take more than 30 seconds to crack. :)  Basically, you've got three choices if
 * you don't want to donate:
 * 
 *  1) Grab the source, strip the licensing code from it, rebuild, and deploy
 *  2) Attempt to crack by reverse engineering the class files and removing/modifying the license code
 *  3) Attempt to crack the encryption and generate a valid license file on your own
 *
 *  If any of those options are more appealing to you than donating the $7.50 for a license then go nuts! :)
 *  
 *  I've done you the favour of listing the options from easiest/less time consuming to the most difficult/most time consuming.
 *  
 * @author dbattams
 *
 */
final class License {
	static private final Logger LOG = Logger.getLogger(License.class);

	static private final File LIC_FILE = new File("plugins/sagetv-addons.lic");
	static private final InputStream KEY_FILE = License.class.getResourceAsStream("/com/google/code/sagetvaddons/sjq/server/resources/sagetv-addons.pub");
	
	static private final License INSTANCE = new License();
	static final License get() { return INSTANCE; }

	private boolean isLicensed;
	private PublicKey key;

	private License() {
		if(!LIC_FILE.exists())
			isLicensed = false;
		else {
			try {
				String data = FileUtils.readFileToString(LIC_FILE, "UTF-8");
				initKey();
				Cipher cipher = Cipher.getInstance("RSA");
				cipher.init(Cipher.DECRYPT_MODE, key);
				Charset utf8 = Charset.forName("UTF-8");
				String propsData = new String(cipher.doFinal(Base64.decodeBase64(data.getBytes(utf8))), utf8);
				Properties props = new Properties();
				props.load(new StringReader(propsData));
				String licEmail = props.getProperty("email");
				String regEmail = DataStore.get().getSetting(Plugin.OPT_EMAIL);
				if(licEmail == null || licEmail.length() == 0) {
					LOG.error("Licensed email is invalid! Your license file is corrupted!");
					isLicensed = false;
				} else if(regEmail == null || regEmail.length() == 0) {
					LOG.error("Registered email is invalid! You must register your email address via the SJQv4 plugin configuration screen.");
					isLicensed = false;
				} else if(licEmail.toLowerCase().equals(regEmail.toLowerCase()))
					isLicensed = true;
				else {
					LOG.error("The email in the license does not match the registered email address!");
					isLicensed = false;
				}
			} catch(Exception e) {
				LOG.error("LicenseReadError", e);
				isLicensed = false;
			}
		}
		if(isLicensed)
			LOG.info("License found and verified; all features of software enabled!");
		else {
			StringBuilder msg = new StringBuilder("License file is invalid or not found; some features of this software have been disabled:\n");
			msg.append("\tOnly one task client can be registered.\n");
			msg.append("\tOnly one registered task per task client.\n");
			LOG.info(msg.toString());
			API.apiNullUI.systemMessageAPI.PostSystemMessage(23000, 1, msg.toString() + "\nPlease consider making a donation.", Config.get().getSysMsgProps());
		}
	}

	public boolean isLicensed() { return isLicensed; }

	private void initKey() {
		if(KEY_FILE != null) {
			ObjectInputStream oin = null;
			BigInteger mod, exp;
			try {
				oin = new ObjectInputStream(new BufferedInputStream(KEY_FILE));
				mod = (BigInteger) oin.readObject();
				exp = (BigInteger) oin.readObject();
				RSAPublicKeySpec keySpec = new RSAPublicKeySpec(mod, exp);
				KeyFactory fact = KeyFactory.getInstance("RSA");
				key = fact.generatePublic(keySpec);
			} catch (Exception e) {
				throw new RuntimeException("Serialization error!", e);
			} finally {
				if(oin != null)
					try { oin.close(); } catch(IOException e) { LOG.error("IOError", e); }
			}
		} else
			throw new RuntimeException("Unable to find public key file!");
	}
}
