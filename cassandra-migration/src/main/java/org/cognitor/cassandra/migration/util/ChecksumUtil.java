package org.cognitor.cassandra.migration.util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class ChecksumUtil {

	public static String encryptSHA512(String script){
		try {
			MessageDigest md = MessageDigest.getInstance("SHA-512");
			byte[] bytes = md.digest(script.getBytes());
			StringBuilder sb = new StringBuilder();
			for(int i=0; i< bytes.length ;i++){
				sb.append(Integer.toString((bytes[i] & 0xff) + 0x100, 16).substring(1));
			}
			
			return sb.toString();
		} 
		catch (NoSuchAlgorithmException e){
			e.printStackTrace();
			return null;
		}
	}
}
