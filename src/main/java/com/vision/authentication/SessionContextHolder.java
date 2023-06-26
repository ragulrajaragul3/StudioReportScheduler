package com.vision.authentication;

import java.security.KeyPair;
import java.security.spec.RSAPrivateKeySpec;
import java.security.spec.RSAPublicKeySpec;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;

import javax.servlet.http.HttpSession;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.vision.util.Constants;
import com.vision.vb.VisionUsersVb;

import jcifs.util.Base64;

@Component
public class SessionContextHolder {
	private static HashMap<String, LocalSession> sessionHolder = new HashMap<String, LocalSession>();
	private static HashMap<String, TokenProps> tokenHolder = new HashMap<String, TokenProps>();
	private static InheritableThreadLocalSecurityContextHolder strategy = new InheritableThreadLocalSecurityContextHolder();
	private static HashMap<String, ConnectionHolder> tempTokenHolder = new HashMap<String, ConnectionHolder>();
	
	public synchronized static void addOrUpdate(String sessionId, HttpSession session) {
		if (sessionHolder.containsKey(sessionId)) {
			LocalSession localSession = sessionHolder.get(sessionId);
			localSession.setLastAccessedTime(session.getLastAccessedTime());
			if (session.getAttribute("userDetails") != null)
				localSession.setUserDetails((VisionUsersVb) session.getAttribute("userDetails"));
		} else {
			LocalSession localSession = new LocalSession(sessionId, session);
			sessionHolder.put(sessionId, localSession);
		}
	}

	public synchronized static boolean isSessionExpired(String sessionId) {
		if (sessionHolder.containsKey(sessionId)) {
			LocalSession localSession = sessionHolder.get(sessionId);
			return localSession.isSessionExpired();
		}
		return true;
	}

	public synchronized static VisionUsersVb getUserDetails(String sessionId) {
		if (sessionHolder.containsKey(sessionId)) {
			LocalSession localSession = sessionHolder.get(sessionId);
			return localSession.getUserDetails();
		} else {
			return null;
		}
	}

	public synchronized static void expireSession(String sessionId) {
		if (sessionHolder.containsKey(sessionId)) {
			sessionHolder.remove(sessionId);
		}
	}

	public synchronized static void expireAllSessions() {
		if (sessionHolder != null && !sessionHolder.isEmpty()) {
			sessionHolder.clear();
		}
		if (strategy != null) {
			strategy = null;
		}
	}

	public synchronized static String encodedSessionId(String sessionId) {
		if (sessionHolder.containsKey(sessionId)) {
			LocalSession localSession = sessionHolder.get(sessionId);
			return localSession.encode();
		}
		return null;
	}

	public static void setContext(VisionUsersVb context) {
		strategy.setContext(context);
	}

	public static VisionUsersVb getContext() {
		return strategy.getContext();
	}

	public synchronized static int addToken(String token, String connectionId) {
		if (!tokenHolder.containsKey(token)) {
			TokenProps tokenProps = new TokenProps(token, connectionId);
			tokenHolder.put(token, tokenProps);
			return Constants.SUCCESSFUL_OPERATION;
		} else {
			return Constants.ERRONEOUS_OPERATION;
		}
	}

	public synchronized static int updateToken(String token, String connectionId, String newToken) {
		if (tokenHolder.containsKey(token)) {
			TokenProps tokenProps = tokenHolder.get(token);
			Calendar cal = Calendar.getInstance();
			tokenHolder.remove(token);
			tokenProps.setToken(newToken);
			tokenProps.setUpdatedDate(cal.getTime());
			cal.setTime(tokenProps.getNxtExpireDate());
			cal.add(Calendar.MINUTE, 30);
			tokenProps.setNxtExpireDate(cal.getTime());
			tokenHolder.put(newToken, tokenProps);
			return Constants.SUCCESSFUL_OPERATION;
		} else {
			return Constants.ERRONEOUS_OPERATION;
		}
	}

	public static boolean isTokenAvailable(String token) {
		if (tokenHolder.containsKey(token)) {
			return true;
		} else {
			return false;
		}
	}

	public static boolean isTokenValidWthConnectionId(byte[] tokenByte, String connectionId) {
		if (tokenByte != null) {
			String token = new String(tokenByte);
			if (tokenHolder.containsKey(token)
					&& connectionId.equalsIgnoreCase(tokenHolder.get(token).getConnectionId())
					&& !isTokenExpiredByTime(token)) {
				return true;
			} else {
				return false;
			}
		} else {
			return false;
		}
	}

	public static boolean isTokenValidWthConnectionId(String token, String connectionId) {
		if (token != null) {
			if (tokenHolder.containsKey(token)
					&& connectionId.equalsIgnoreCase(tokenHolder.get(token).getConnectionId())
					&& !isTokenExpiredByTime(token)) {
				return true;
			} else {
				return false;
			}
		} else {
			return false;
		}
	}
	
	public static TokenProps getTokenProps(String token) {
		if (tokenHolder.containsKey(token)) {
			return tokenHolder.get(token);
		} else {
			return null;
		}
	}
	
	public static boolean isTokenExpiredByTime(String token) {
		if (tokenHolder.containsKey(token)) {
			Calendar cal = Calendar.getInstance();
			Date expireDate = tokenHolder.get(token).getNxtExpireDate();
			/*if(cal.getTime().after(expireDate)) {
				return true;
			} else {
				return false;
			}*/
			return false;
		} else {
			return true;
		}
		
	}
	
	public synchronized static int addTokenForConnectionId(String tmpToken, String connectionId, KeyPair keyPair, RSAPublicKeySpec rsaPublicKeySpec, RSAPrivateKeySpec rsaPrivateKeySpec, byte[] encryptedData) {
		if (!tempTokenHolder.containsKey(tmpToken)) {
			ConnectionHolder ch = new ConnectionHolder(connectionId, keyPair, rsaPublicKeySpec, rsaPrivateKeySpec, encryptedData);
			tempTokenHolder.put(tmpToken, ch);
			return Constants.SUCCESSFUL_OPERATION;
		} else {
			return Constants.ERRONEOUS_OPERATION;
		}
	}
	
	public synchronized static String getConnectionIdFromTempToken(String tmpToken) {
		if (!tempTokenHolder.containsKey(tmpToken)) {
			return null;
		} else {
			return tempTokenHolder.get(tmpToken).getConnectionId();
		}
	}
	
	public synchronized static KeyPair getKeyPairFromTempToken(String tmpToken) {
		if (!tempTokenHolder.containsKey(tmpToken)) {
			return null;
		} else {
			return tempTokenHolder.get(tmpToken).getKeyPair();
		}
	}
	
	public synchronized static ConnectionHolder getconnectionClassFromTempToken(String tmpToken) {
		if (!tempTokenHolder.containsKey(tmpToken)) {
			return null;
		} else {
			return tempTokenHolder.get(tmpToken);
		}
	}
	
	public synchronized static String getConnectionIdFromToken(byte[] tokenByte) {
		String token = new String(tokenByte);
		if (!tokenHolder.containsKey(token)) {
			return null;
		} else {
			return tokenHolder.get(token).getConnectionId();
		}
	}
	
	public synchronized static String getConnectionIdFromToken(String token) {
		if (!tokenHolder.containsKey(token)) {
			return null;
		} else {
			return tokenHolder.get(token).getConnectionId();
		}
	}
	
	public synchronized static int removeTempTokenForConnectionId(String tmpToken) {
		if (!tempTokenHolder.containsKey(tmpToken)) {
			return Constants.ERRONEOUS_OPERATION;
		} else {
			tempTokenHolder.remove(tmpToken);
			return Constants.SUCCESSFUL_OPERATION;
		}
	}
}

class InheritableThreadLocalSecurityContextHolder {
	// ~ Static fields/initializers
	private static ThreadLocal<VisionUsersVb> contextHolder = new InheritableThreadLocal<VisionUsersVb>();
	// ~ Methods

	public void clearContext() {
		contextHolder.set(null);
	}

	public VisionUsersVb getContext() {
		if (contextHolder.get() == null) {
			return null;
		}
		return (VisionUsersVb) contextHolder.get();
	}

	public void setContext(VisionUsersVb context) {
		contextHolder.set(context);
	}
}

class TokenProps {
	private String token;
	private String connectionId;
	private Date creationDate;
	private Date nxtExpireDate;
	private Date updatedDate;

	public TokenProps(String token, String connectionId) {
		this.token = token;
		this.connectionId = connectionId;
		Calendar cal = Calendar.getInstance();
		Date date = cal.getTime();
		this.creationDate = this.updatedDate = date;
		cal.add(Calendar.MINUTE, 30);
		this.nxtExpireDate = cal.getTime();
		/*SimpleDateFormat sdf = new SimpleDateFormat("dd-M-yyyy hh:mm:ss");
		System.out.println("creationDate:"+sdf.format(creationDate));
		System.out.println("updatedDate:"+sdf.format(updatedDate));
		System.out.println("nxtExpireDate:"+sdf.format(nxtExpireDate));*/
	}

	public String getToken() {
		return token;
	}

	public void setToken(String token) {
		this.token = token;
	}

	public String getConnectionId() {
		return connectionId;
	}

	public void setConnectionId(String connectionId) {
		this.connectionId = connectionId;
	}

	public Date getCreationDate() {
		return creationDate;
	}

	public void setCreationDate(Date creationDate) {
		this.creationDate = creationDate;
	}

	public Date getNxtExpireDate() {
		return nxtExpireDate;
	}

	public void setNxtExpireDate(Date nxtExpireDate) {
		this.nxtExpireDate = nxtExpireDate;
	}

	public Date getUpdatedDate() {
		return updatedDate;
	}

	public void setUpdatedDate(Date updatedDate) {
		this.updatedDate = updatedDate;
	}
}

class LocalSession {
	private String id;
	private KeyPair keyPair;
	private long lastAccessedTime;
	private long maxInactiveInterval;
	@Autowired
	private VisionUsersVb userDetails;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public KeyPair getKeyPair() {
		return keyPair;
	}

	public void setKeyPair(KeyPair keyPair) {
		this.keyPair = keyPair;
	}

	public long getLastAccessedTime() {
		return lastAccessedTime;
	}

	public void setLastAccessedTime(long lastAccessedTime) {
		this.lastAccessedTime = lastAccessedTime;
	}

	public VisionUsersVb getUserDetails() {
		return userDetails;
	}

	public void setUserDetails(VisionUsersVb userDetails) {
		this.userDetails = userDetails;
	}

	public long getMaxInactiveInterval() {
		return maxInactiveInterval;
	}

	public void setMaxInactiveInterval(long maxInactiveInterval) {
		this.maxInactiveInterval = maxInactiveInterval;
	}

	public String encode() {
		return Base64.encode(getId().getBytes());
	}

	public LocalSession(String sessionId, HttpSession session) {
		this.id = sessionId;
		this.lastAccessedTime = session.getLastAccessedTime();
		this.userDetails = (VisionUsersVb) session.getAttribute("userDetails");
		this.maxInactiveInterval = session.getMaxInactiveInterval();
		this.keyPair = (KeyPair) session.getAttribute("keyPair");
	}

	public boolean isSessionExpired() {
//		return (maxInactiveInterval * 60 * 1000) < (System.currentTimeMillis() - lastAccessedTime);
		return false;
	}
}

class ConnectionHolder{
	String connectionId;
	KeyPair keyPair;
	RSAPublicKeySpec rsaPublicKeySpec;
	RSAPrivateKeySpec rsaPrivateKeySpec;
	byte[] encryptedData;
	public ConnectionHolder(String connectionId, KeyPair keyPair, RSAPublicKeySpec rsaPublicKeySpec, RSAPrivateKeySpec rsaPrivateKeySpec, byte[] encryptedData) {
		this.connectionId = connectionId;
		this.keyPair = keyPair;
		this.rsaPublicKeySpec = rsaPublicKeySpec;
		this.rsaPrivateKeySpec = rsaPrivateKeySpec;
		this.encryptedData = encryptedData;
	}
	public String getConnectionId() {
		return connectionId;
	}
	public void setConnectionId(String connectionId) {
		this.connectionId = connectionId;
	}
	public KeyPair getKeyPair() {
		return keyPair;
	}
	public void setKeyPair(KeyPair keyPair) {
		this.keyPair = keyPair;
	}
	public RSAPublicKeySpec getRsaPublicKeySpec() {
		return rsaPublicKeySpec;
	}
	public void setRsaPublicKeySpec(RSAPublicKeySpec rsaPublicKeySpec) {
		this.rsaPublicKeySpec = rsaPublicKeySpec;
	}
	public RSAPrivateKeySpec getRsaPrivateKeySpec() {
		return rsaPrivateKeySpec;
	}
	public void setRsaPrivateKeySpec(RSAPrivateKeySpec rsaPrivateKeySpec) {
		this.rsaPrivateKeySpec = rsaPrivateKeySpec;
	}
	public byte[] getEncryptedData() {
		return encryptedData;
	}
	public void setEncryptedData(byte[] encryptedData) {
		this.encryptedData = encryptedData;
	}
}