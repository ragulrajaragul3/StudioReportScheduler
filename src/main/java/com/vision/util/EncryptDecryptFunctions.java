package com.vision.util;

import java.util.Base64;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

public class EncryptDecryptFunctions {

	static final String SECRET = "Spiral Architect";

	public static String passwordDecrypt(String ciphertext) {
		try {
			byte[] secret = (SECRET.hashCode() + "").substring(0, 8).getBytes();
			Cipher des = Cipher.getInstance("DES");
			des.init(Cipher.DECRYPT_MODE, new SecretKeySpec(secret, "DES"));
			byte[] plaintext = des.doFinal(Base64.getDecoder().decode(ciphertext));
			return new String(plaintext);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return ciphertext;
	}

	public static String passwordEncrypt(String plaintext) {
		try {
			byte[] secret = (SECRET.hashCode() + "").substring(0, 8).getBytes();
			Cipher des = Cipher.getInstance("DES");
			des.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(secret, "DES"));
			byte[] ciphertext = des.doFinal(plaintext.getBytes());
			return Base64.getEncoder().encodeToString(ciphertext);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return plaintext;
	}

}
