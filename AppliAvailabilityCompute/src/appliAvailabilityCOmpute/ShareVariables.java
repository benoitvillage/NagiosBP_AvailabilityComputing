package appliAvailabilityCOmpute;

import java.io.IOException;
import java.util.Date;
import java.util.Properties;

public class ShareVariables {

	public String host ;
	public String user ;
	public String password;
	public Date date;
	public int beginMinute;
	public int endMinute;
	public int epochBegin;
	public int epochEnd;
	public int chargementId;
	public int validatorCounter = 0;
	public long maskMax = 0x0FFFFFFFFFFFFFFFL;
	public int internOutageEventId = 0;
	public int internDowntimeEventId = 0;
	

	public int getInternDowntimeEventId() {
		return this.internDowntimeEventId;
	}
	
	public void setInternDowntimeEventId(int internDowntimeEventId) {
		this.internDowntimeEventId = internDowntimeEventId;
	}
	
	public int getInternOutageEventId() {
		return this.internOutageEventId;
	}
	
	public void setInternOutageEventId(int internOutageEventId) {
		this.internOutageEventId = internOutageEventId;
	}

	
	
	public int getValidatorCounter() {
		this.validatorCounter++;
		return validatorCounter;
	}

	public ShareVariables()
	{
		this.initShareVariable();
	}
	
	public void initShareVariable() {
		
		   
	    try {
	    	
	    	Properties configFile = new Properties();
			configFile.load(this.getClass().getClassLoader().getResourceAsStream("AppliAvailabilityCompute.properties"));
			
			this.setHost(configFile.getProperty("HOST"));
			this.setUser(configFile.getProperty("USER"));
			this.setPassword(configFile.getProperty("PASSWORD"));
			
			System.out.println(this.host);
			System.out.println(this.user);
			System.out.println(this.password);
			
		
	    } catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


		
	}
	
	public String getHost() {
		return host;
	}
	public void setHost(String host) {
		this.host = host;
	}
	public String getUser() {
		return user;
	}
	public void setUser(String user) {
		this.user = user;
	}
	public String getPassword() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}
	public Date getDate() {
		return date;
	}
	public void setDate(Date date) {
		this.date = date;
	}
	public int getBeginMinute() {
		return beginMinute;
	}
	public void setBeginMinute(int beginMinute) {
		this.beginMinute = beginMinute;
	}
	public int getEndMinute() {
		return endMinute;
	}
	public void setEndMinute(int endMinute) {
		this.endMinute = endMinute;
	}
	public int getEpochBegin() {
		return epochBegin;
	}
	public void setEpochBegin(int epochBegin) {
		this.epochBegin = epochBegin;
	}
	public int getEpochEnd() {
		return epochEnd;
	}
	public void setEpochEnd(int epochEnd) {
		this.epochEnd = epochEnd;
	}
	public int getChargementId() {
		return chargementId;
	}
	public void setChargementId(int chargementId) {
		this.chargementId = chargementId;
	}
	
	public long getMaskMax() {
		return maskMax;
	}

	
}
