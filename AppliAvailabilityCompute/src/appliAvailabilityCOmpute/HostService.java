package appliAvailabilityCOmpute;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;


public class HostService {

	private String source;
	private int hostId;
	private int serviceId;
	private boolean downtimeState;
	private int state;
	private boolean previousState;
	private boolean previousDowntime;
	private int availability;
	private MyConnection myConnection;
	private boolean isDowntime;
	private ArrayList<Integer> listDateMinute;
	private long downtimeMinute;
	private long availabilityMinute;
	private ShareVariables shareVariables;
	private ValidatorList vList;
	private int validatorId;
	
	public HostService(MyConnection myConnection, ShareVariables shareVariables, ValidatorList vList) {
		this.myConnection = myConnection;
		this.shareVariables = shareVariables;
		this.vList = vList;
	}

	public void hostServiceInit(int validatorId, String hostServiceSource, int hostId, int serviceId, boolean isDowntime,
			boolean previousState, boolean previousDowntime, int availability, int state) {
		// TODO Auto-generated method stub
		
		this.source = hostServiceSource;
		this.hostId = hostId;
		this.serviceId = serviceId;
		this.isDowntime = isDowntime;
		this.previousState = previousState;
		this.previousDowntime = previousDowntime;
		this.availability = availability;
		this.state = state;
		this.validatorId = validatorId;
	}

	public void computeAvailabilityMinute(int minutes) {
		// TODO Auto-generated method stub
		
		//If resultset is empty a take into account the previous state
			
		ResultSet rs = this.myConnection.getHostServiceLogMinute(minutes);
		int resultSetSize = this.isResultSetSize(rs);
		
		
		if(resultSetSize == 0)	
		{
			if(!previousState) {
				this.availability = -60;
				this.state = 0;
				this.availabilityMinute = this.shareVariables.getMaskMax();
			}
			else {
				this.availability = 0;
				this.state = 1;
				this.availabilityMinute = (long) 0;
			}
		}
		//If result has one record, I analyse it
		else if (resultSetSize == 1)
		{
			
			this.availability = this.getAvailabilityFromLogs(rs);
			/*System.out.println("Nous avons un résultat pour le host " + this.hostId + " avec"
					+ " une availability de " + this.availability + " state : " + this.state);
			 */
		}
		//If resultset has more than one record I use another method
		else if (resultSetSize > 1)
		{
			this.availability = this.getAvailabilityFromComplexLogs(rs);
			/*System.out.println("I have several records for the same minute Host : " + this.hostId + ""
					+ " service : " + this.serviceId + " minute : " +minutes); */
		}
		
		try {
			this.myConnection.closeResultSet();
			rs.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private int getAvailabilityFromComplexLogs(ResultSet rs) {
		// TODO Auto-generated method stub
	
		int[] stateTab = new int [12];
		stateTab = this.initializeStateTab(stateTab);
		int availability = 0;
		this.availabilityMinute = 0;
		boolean existFirstSecondOutage = false;
		ArrayList<Integer> listeSecondeEvent = new ArrayList<Integer>();
		int previousBit = 0;

		
		try {
			
			HashMap <Integer,Integer> listStatesPerSecond = new HashMap<Integer,Integer>();
			
			while (rs.next()) {	
				
				/*if(rs.getInt("second_event") == 0 && rs.getInt("state") == 0) {
					existFirstSecondOutage = true;
				}*/
				
				this.availabilityMinute |= this.convertValueToMinuteLong(rs.getInt("fln_availability"));
				
				listStatesPerSecond.put(rs.getInt("second_event"), rs.getInt("state"));
				listeSecondeEvent.add(rs.getInt("second_event"));
			}
			//System.out.println(Long.toBinaryString(this.availabilityMinute));
			
			this.state = this.getStateFromLastBit();
			if(this.state == 0)
				availability = -this.countNbBit(this.availabilityMinute);
			else availability = this.countNbBit(this.availabilityMinute);
			
			
			/*if(existFirstSecondOutage)
			{
					this.availabilityMinute = this.putBitAtPosition(this.availabilityMinute,59);
					previousBit = 1;
			}
			
			for(int i=1; i < 60; i++)
			{
				if(listeSecondeEvent.contains(i))
				{
					if(listStatesPerSecond.get(i) == 0) {
						this.availabilityMinute = this.putBitAtPosition(this.availabilityMinute,60 - i);
						previousBit = 1;
					}
					else previousBit = 0;
				}
				else if(previousBit == 1) 
					this.availabilityMinute = this.putBitAtPosition(this.availabilityMinute,60 - i);
			}*/
			
			//availability = this.fillstateTab(stateTab);
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return availability;
		}
		return availability;
	}
	
	private int getStateFromLastBit() {
		// TODO Auto-generated method stub
		int tmpState = 1;
		if(this.getBitPosition(this.availabilityMinute,0) == 1)
			tmpState = 0;
		
		return tmpState;
	}
	
	public Long getBitPosition(Long value, int position)
	{
		Long intermediaire1;
		Long intermediaire2;
		Long un = (long) 1;
		
		intermediaire1 = un << position;
		intermediaire2 = value;
		intermediaire2 &= intermediaire1;
		intermediaire2 >>= position;
		
		return intermediaire2;
	}

	private int countNbBit(long value) {
		// TODO Auto-generated method stub
		
		//System.out.println(Long.toHexString(maxMask));
		//System.out.println(Long.toBinaryString(maxMask));
		int availabilityMinute = 0;
		long k = 1;
		long intermediaire;
		long intermediaire2;
		
		//System.out.println(Long.toString(maxMask,2));
		
		for(int i = 0; i < 60; i++)
		{
			intermediaire =  k << i;
			intermediaire2 = value;
			intermediaire2 &= intermediaire; 
			availabilityMinute += intermediaire2 >> i;

		}
		
		return availabilityMinute;
	}

	private long putBitAtPosition(long value, int position) {
		// TODO Auto-generated method stub
		
			value |= 1 << position;
		
		return value;
	}

	private int fillstateTab(int[] stateTab) {
		
		int firstAvailability;
		int totalAvailability = 0;
		
		if(this.previousState)
			firstAvailability = 0;
		else firstAvailability = -5;
		
		//We take care first 5 seconds period
		if(stateTab[0] == -1){
			totalAvailability += firstAvailability;
			if(this.previousState)
				this.state = 1;
			else this.state = 0;
		}
		else{
			totalAvailability += stateTab[0];
			if(stateTab[0] == -5)
				this.state = 0;
			else this.state = 1;
		}
		
		for(int i = 1; i < 12; i++)
		{
			if(stateTab[i] == -1)
				totalAvailability += stateTab[i-1];
			else {
				totalAvailability += stateTab[i];
				if(stateTab[i] == -5)
					this.state = 0;
				else this.state = 1;
			}
		}
		
		if(this.state==1 && totalAvailability < 0)
			totalAvailability = -totalAvailability;
		
		return totalAvailability;
	}


	private int[] initializeStateTab(int[] stateTab) {
		// TODO Auto-generated method stub
		
		for(int i = 0; i < 12; i++)
		{
			stateTab[i] = -1;
		}
		
		return stateTab;
	}

	private int getAvailabilityFromLogs(ResultSet rs) {
		// TODO Auto-generated method stub
		
		int availability = 0;
		
		try {
			while (rs.next()) {
				
				//case current state ok but previous state nok
				if(rs.getInt("state") == 1 && !this.previousState) {
					availability = rs.getInt("fln_availability");
					this.state = 1;
					this.availabilityMinute = this.convertValueToMinuteLong(availability);
				}
				//case current state ok and previous state ok
				else if(rs.getInt("state") == 1 && this.previousState){
					availability = 0;
					this.state = 1;
					this.availabilityMinute = 0;
				}
				//case current state nok and previous state nok
				else if(rs.getInt("state") == 0 && !this.previousState){
					availability = -60;
					this.state = 0;
					this.availabilityMinute = this.shareVariables.getMaskMax();
				}
				//case current state nok and previous state ok
				else if(rs.getInt("state") == 0 && this.previousState){
					availability = rs.getInt("fln_availability");
					this.state=0;
					this.availabilityMinute = this.convertValueToMinuteLong(availability);

				}
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return availability;
		}
		
		
		return availability;
	}

	private int isResultSetSize(ResultSet rs) {
		// TODO Auto-generated method stub
		int resultSetSize  = 0;
		
		try {
			
			while (rs.next() && resultSetSize < 3) {
				resultSetSize++;  
			}
			
			rs.beforeFirst();
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return 0;
		}
		
		return resultSetSize;
	}

	public void getHostServiceDowntimeState(int minute) {
		
		ResultSet resultSet = this.myConnection.getDowntime(minute);
		String currentDowntime;
		int secondeDowntime = -1;
		int downtimeFlag = -1;
		this.downtimeMinute = 0;
		
		
		try {
			
			while (resultSet.next()) {
				  
				currentDowntime = resultSet.getString("fdo_message_type_label");
				secondeDowntime =  resultSet.getInt("fdo_seconde");
				 
				if(currentDowntime.equals("STARTED")) {
					downtimeFlag = 1;
				}
				else downtimeFlag = 0; 
			}
			
			if(downtimeFlag == -1)
				this.isDowntime = this.previousDowntime;
			else if(downtimeFlag == 0)
				this.isDowntime = false;
			else {
				this.isDowntime = true;
				secondeDowntime = -60 + secondeDowntime;
			}
			
			if(this.isDowntime && this.previousDowntime)
			{
				this.downtimeMinute = this.shareVariables.getMaskMax();
			}
			else if(this.isDowntime && !this.previousDowntime){
				this.downtimeMinute= convertValueToMinuteLong(secondeDowntime);
			}
			else if(!this.isDowntime && this.previousDowntime)
			{
				this.downtimeMinute = convertValueToMinuteLong(secondeDowntime);
			}
			else if(this.isDowntime && this.previousDowntime)
			{
				this.downtimeMinute = 0;
			}
			
			this.myConnection.closeResultSet();
		
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
	}

	public long convertValueToMinuteLong(int value)
	{
		long myLong = 0;
		long k = 1;
		
		if(value > 0) {
			for(int i = 59; i >=value ; i--)
			{
				myLong |= k << i;
			}
		} else if(value < 0)
		{
			for(int i = -value-1; i >= 0; i--)
			{
				myLong |= k << i;
			}
		}
		//System.out.println(Long.toBinaryString(myLong));
		return myLong;
	}
	
	public void insertHostServiceAvailability(int minute) {
		
		int previousStateInt;
		if(this.previousState)
			previousStateInt = 1;
		else previousStateInt=0;
		
		if(this.availability != 0 || (this.isDowntime) 
			|| this.state != previousStateInt){
			
			/*System.out.println("Je vais insérer pour le host " + this.hostId + " l'availability "
									+ this.availability + " pour un state = " + this.state + " et un"
											+ " downtime = " + this.isDowntime + " minute : " + minute);*/
			this.vList.getHashMapValidator().get(this.validatorId).setAvailabilityMinute(this.availabilityMinute);
			this.vList.getHashMapValidator().get(this.validatorId).setAvailabilityDownMinute(this.availabilityMinute & ~(this.downtimeMinute));
			this.vList.getHashMapValidator().get(this.validatorId).computeDowntime();
			this.vList.getHashMapValidator().get(this.validatorId).setDowntimeDuration(this.downtimeMinute);
			this.vList.getHashMapValidator().get(this.validatorId).setIsDowntime(this.isDowntime);
			this.vList.getHashMapValidator().get(this.validatorId).insertMinute(minute);
			this.vList.getHashMapValidator().get(this.validatorId).addListAvailabilityMinute(minute,this.availabilityMinute);
			this.vList.getHashMapValidator().get(this.validatorId).addListAvailabilityDownMinute(minute,(this.availabilityMinute & ~(this.downtimeMinute)));
			this.vList.getHashMapValidator().get(this.validatorId).addListDowntimeMinute(minute,this.downtimeMinute);
			this.vList.getHashMapValidator().get(this.validatorId).addListEpochEvent(minute);
			
		}

	}

	public void saveCurrentContext() {
		// TODO Auto-generated method stub
		this.previousDowntime = this.isDowntime;
		if(this.state == 0)
			this.previousState = false;
		else this.previousState = true;
	}

	public void initializeDateMinuteStateArray() {
		// TODO Auto-generated method stub
		this.listDateMinute = new ArrayList<Integer>();
	}

	public void addDateMinuteState(int dateMinute) {
		// TODO Auto-generated method stub
		if(!existDateMinute(dateMinute))
			this.listDateMinute.add(dateMinute);
	}

	private boolean existDateMinute(int dateMinute) {
		// TODO Auto-generated method stub
		
		boolean existDateMinute=false;
		
		for(int i = 0; i < this.listDateMinute.size(); i++)
		{
			if(this.listDateMinute.get(i) == dateMinute)  
				existDateMinute=true;
		}
		
		return existDateMinute;
	}

	public void compute(ShareVariables shareVariables, int computeCase) {
		// TODO Auto-generated method stub
		
		Collections.sort(this.listDateMinute);
	
		/*if(this.hostId == 147)
			System.out.print("");*/
		
		
		for(int i = shareVariables.getEpochBegin(); i <= shareVariables.getEpochEnd(); i += 60){
			
			if(computeCase == 1){
			
				if(existDateMinute(i)) {
					this.computeAvailabilityMinute(i);
					this.getHostServiceDowntimeState(i);
					//System.out.println(Long.toBinaryString(this.availabilityMinute));
				}
				else this.computeFromPreviousValue();
				//System.out.println(Long.toBinaryString(this.availabilityMinute));
				
				this.insertHostServiceAvailability(i);
				this.saveCurrentContext();
					
			}
			else if(computeCase == 2) {
				
				if(existDateMinute(i)) {
					this.getHostServiceDowntimeState(i);
				}
				else this.computeFromPreviousValue();
				
				this.insertHostServiceAvailability(i);
				this.saveCurrentContext();
			}
			else if(computeCase == 3) {
				
				if(existDateMinute(i)) {
					this.computeAvailabilityMinute(i);
				}
				else this.computeFromPreviousValue();
				
				this.insertHostServiceAvailability(i);
				this.saveCurrentContext();
				
			}
		}
	}

	private void computeFromPreviousValue() {
		// TODO Auto-generated method stub
		if(!this.previousState){
			this.state = 0;
			this.availability = -60;
			this.availabilityMinute = this.shareVariables.getMaskMax();
		}
		else {
			this.state = 1;
			this.availability = 0;
			this.availabilityMinute = 0;
		}
		
		if(this.previousDowntime) {
			this.isDowntime = true;
			this.downtimeMinute = this.shareVariables.getMaskMax();
		}
		else {
			this.isDowntime = false;
			this.downtimeMinute = 0;
		}
	}

}
