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
	private long downtimeMinute = (long) 0;
	private long availabilityMinute;
	private ShareVariables shareVariables;
	private ValidatorList vList;
	private int validatorId;
	/**
	 * 0 state is OK
	 * 1 outage not inherited from Hoststatus
	 * 2 outage inherited from Hoststatus
	 */
	private int hostStatusStateFlag;
	private int previousHostStatusStateFlag;
	
	/**
	 * Attribute added 2016-25-03 by benoit village
	 * This attribute contains host service last minutes downtime bit
	 * before hoststatus downtime minute application. This attribute
	 * allows to stop downtime when inherited from hoststatus.
	 */
	private long previousHostServiceDowntimeBit;
	/**
	 * This attribute contains host service last minutes outage bit
	 * before hoststatus availability minute application. This attribute
	 * allows to stop outage when inherited from hoststatus.
	 */
	private long previousHostServiceOutageBit;
	
	public HostService(MyConnection myConnection, ShareVariables shareVariables, ValidatorList vList) {
		this.myConnection = myConnection;
		this.shareVariables = shareVariables;
		this.vList = vList;
	}

	public void hostServiceInit(int validatorId, String hostServiceSource, int hostId, int serviceId, boolean isDowntime,
			boolean previousState, boolean previousDowntime, int availability, int state, int hostStatusStateFlag,
			int previousHostStatusStateFlag, int previousHostServiceDowntimeBit, long previousHostServiceOutageBit) {
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
		this.hostStatusStateFlag = hostStatusStateFlag;
		this.previousHostStatusStateFlag = previousHostStatusStateFlag;
		this.previousHostServiceDowntimeBit = previousHostServiceDowntimeBit;
		/**Code added 2016-25-03 by Benoit Village*/
		this.previousHostServiceOutageBit = previousHostServiceOutageBit;

	}

	public void computeAvailabilityMinute(int minutes) {
		// TODO Auto-generated method stub
		
		//If resultset is empty, we take into account the previous state		
		ResultSet rs = this.myConnection.getHostServiceLogMinute(minutes);
		int resultSetSize = this.isResultSetSize(rs);
		
		//Warning
		//if(minutes == 1421276460 && this.hostId == 5 && this.serviceId == 6){
		//	System.out.println("Voici le nombre d'event sur la minute 1421276460 pour hostservice 5 6 : " + resultSetSize);
			/*try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}*/
		//}
		
		
		if(resultSetSize == 0)	
		{
			this.availabilityMinute = 0;
			
			/** Block commented 20160325 by Benoit Village
			if(!previousState && this.previousHostServiceOutageBit==1) {
				this.availability = -60;
				this.state = 0;
				this.availabilityMinute = this.shareVariables.getMaskMax();
			}
			*/
			
			/** Begin Block added 2016-03-25 by Benoit Village*/
			if(!previousState && this.previousHostServiceOutageBit==1) {
				this.availability = -60;
				this.state = 0;
				this.availabilityMinute = this.shareVariables.getMaskMax();
			}
			/** End Block added 2016-03-25 by Benoit Village*/
			else {
				this.availability = 0;
				this.state = 1;
				this.availabilityMinute = (long) 0;
			}
			
			/**Begin Block added 2016-03-25 by Benoit Village*/
			this.previousHostServiceOutageBit = this.getBitPosition(availabilityMinute, 0);
			/**End Block added 2016-03-25 by Benoit Village*/
			
			this.hostStatusStateFlag = this.previousHostStatusStateFlag;
			
			/**BEGIN Block added 2016-03-18 by Benoit Village*/
			
			int tmpUnavailability = this.countNbBit(this.availabilityMinute);
			//Take into account Hoststatus state in service compute
			this.computeAvailabilityWithHostStatus(minutes);
			int unavailability = this.countNbBit(this.availabilityMinute);
			
			if(this.getStateFromLastBit() == 0){
				this.availability = 0 - unavailability;
				this.state = 0;
			}
			else {
				this.availability = unavailability;
				this.state = 1;
			}
			
			if(tmpUnavailability != unavailability)
				this.hostStatusStateFlag = 1;
			
			/**END Block added 2016-03-18 by Benoit Village*/
		}
		//If result has one record, I analyse it
		else if (resultSetSize == 1)
		{
			
			this.availability = this.getAvailabilityFromLogs(rs);
			/*System.out.println("Nous avons un résultat pour le host " + this.hostId + " avec"
					+ " une availability de " + this.availability + " state : " + this.state);
			 */
			
			/**BEGIN Block added 2016-03-18 by Benoit Village*/
			
			int tmpUnavailability = this.countNbBit(this.availabilityMinute);
			//Take into account Hoststatus state in service compute
			this.computeAvailabilityWithHostStatus(minutes);
			int unavailability = this.countNbBit(this.availabilityMinute);
			if(this.getStateFromLastBit() == 0)
				this.availability = 0 - unavailability;
			else this.availability = unavailability;
			
			if(tmpUnavailability != unavailability)
				this.hostStatusStateFlag = 1;
			
			/**END Block added 2016-03-18 by Benoit Village*/
		}
		//If resultset has more than one record I use another method
		else if (resultSetSize > 1)
		{
			this.availability = this.getAvailabilityFromComplexLogs(rs);
			/*System.out.println("I have several records for the same minute Host : " + this.hostId + ""
					+ " service : " + this.serviceId + " minute : " +minutes); */
			
			/**BEGIN Block added 2016-03-18 by Benoit Village*/
			
			//System.out.println(Long.toBinaryString(this.availabilityMinute));
			
			int tmpUnavailability = this.countNbBit(this.availabilityMinute);
			//Take into account Hoststatus state in service compute
			this.computeAvailabilityWithHostStatus(minutes);
			int unavailability = this.countNbBit(this.availabilityMinute);
			if(this.getStateFromLastBit() == 0)
				this.availability = 0 - unavailability;
			else this.availability = unavailability;
			
			if(unavailability != tmpUnavailability)
				this.hostStatusStateFlag = 1;
			else this.hostStatusStateFlag = 0;
			
			/*System.out.println(Long.toBinaryString(this.availabilityMinute));
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}*/
			/**END Block added 2016-03-18 by Benoit Village*/
		}
		
		
		try {
			this.myConnection.closeResultSet();
			rs.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Method added 2016-03-21 by Benoit Village
	 * 
	 * When we compute downtime for host service
	 * we need to apply HostStatus downtime mask
	 * Hosststatus downtime mask is reachable
	 * through HostService send list
	 * @param minute
	 */
	private void computeDowntimeWithHostStatus(int minute) {
		// TODO Auto-generated method stub
		ArrayList<Integer> listSender = this.vList.getHashMapValidator().get(this.validatorId).getListSender();
		int nbSenders = listSender.size();
		
		Validator hostStatusValidator;
		int hostStatusValidatorId;
		long hostStatusValidatorDowntimeMinute;

		if(nbSenders == 1) {
			hostStatusValidatorId = listSender.get(0);
			hostStatusValidator = this.vList.getHashMapValidator().get(hostStatusValidatorId);
			//On test que la minute actuelle existe dans le dico de minutes du hoststatus
			if(hostStatusValidator.listDowntimeMinute.containsKey(minute)){
				hostStatusValidatorDowntimeMinute = hostStatusValidator.listDowntimeMinute.get(minute);
				this.downtimeMinute |= hostStatusValidatorDowntimeMinute;
				//if(this.hostId==5 && this.serviceId==6 && minute == 1421278200)
				//	System.out.println(Long.toBinaryString(hostStatusValidatorDowntimeMinute));
			}	
		}
		else if(nbSenders == 0)
		{
			
		}
		else System.out.println("Le Host Service " + this.hostId + " " + this.serviceId + 
				" est lié à plus d'un sender " + nbSenders + " minute " + minute);
	}

	
	/**
	 * Method added 2016-03-18 by Benoit Village
	 * 
	 * When we compute availability for host service
	 * we need to apply HostStatus availability mask
	 * Hosststatus availability mask is reachable
	 * through HostService send list
	 * @param minutes 
	 */
	private void computeAvailabilityWithHostStatus(int minutes) {
		// TODO Auto-generated method stub
		ArrayList<Integer> listSender = this.vList.getHashMapValidator().get(this.validatorId).getListSender();
		int nbSenders = listSender.size();
		
		Validator hostStatusValidator;
		int hostStatusValidatorId;
		long hostStatusValidatorAvalabilityMinute;

		if(nbSenders == 1) {
			hostStatusValidatorId = listSender.get(0);
			hostStatusValidator = this.vList.getHashMapValidator().get(hostStatusValidatorId);
			//On test que la minute actuelle existe dans le dico de minutes du hoststatus
			if(hostStatusValidator.listAvailabilityMinute.containsKey(minutes)){
				hostStatusValidatorAvalabilityMinute = hostStatusValidator.listAvailabilityMinute.get(minutes);
				this.availabilityMinute |= hostStatusValidatorAvalabilityMinute;
				//if(this.hostId==5 && this.serviceId==6)
				//	System.out.println(Long.toBinaryString(hostStatusValidatorAvalabilityMinute));
			}			
		}
		else if(nbSenders == 0)
		{
			
		}
		else System.out.println("Le Host Service " + this.hostId + " " + this.serviceId + 
				" est lié à plus d'un sender " + nbSenders + " minute " + minutes);

	}

	private int getAvailabilityFromComplexLogs(ResultSet rs) {
		// TODO Auto-generated method stub
	
		//lastBitValue contains value of the last bit computed
		int lastBitValue;
		int currentBitValue;
		int bitFrom=0;
		int bitTo=0;
		this.availabilityMinute = 0;
		this.availability = 0;
		
		//lastBitValue is initialized with last bit of previous state
		if(!this.previousState)
			lastBitValue=1;
		else lastBitValue=0;
		
		this.setCurrentBitValueOnAvailability(lastBitValue,bitFrom);
		
		//We browse all event occured during minute
		try {
			while (rs.next()) {
				
				if(rs.getInt("state") == 1)
					currentBitValue = 0;
				else currentBitValue = 1;
				
				bitTo=rs.getInt("second_event");
				this.setCurrentBitValueOnAvailability(currentBitValue,bitTo);
				this.computeMaskBetweenLastAndCurrentBit(bitFrom,bitTo,lastBitValue);
				
				lastBitValue = currentBitValue;
				bitFrom = bitTo;
				
			}
			
			this.setCurrentBitValueOnAvailability(lastBitValue,59);
			this.computeMaskBetweenLastAndCurrentBit(bitFrom,59,lastBitValue);
			
			
			this.state = this.getStateFromLastBit();
			if(this.state == 0)
				availability = -this.countNbBit(this.availabilityMinute);
			else availability = this.countNbBit(this.availabilityMinute);
			
			/**Line added 2016-03-25 by Benoit Village*/
			this.previousHostServiceOutageBit = this.getBitPosition(availabilityMinute, 0);	
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		/**
		int hostStatusStateFlagTmp = -1;
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
				
				//if(rs.getInt("second_event") == 0 && rs.getInt("state") == 0) {
				//	existFirstSecondOutage = true;
				//}
				
				if(hostStatusStateFlagTmp != 1) {
					if(rs.getInt("isHostStatus") == 0)
						hostStatusStateFlagTmp = 0;
					else hostStatusStateFlagTmp = 1;
				}
					
				
				this.availabilityMinute |= this.convertValueToMinuteLong(rs.getInt("fln_availability"));
				
				listStatesPerSecond.put(rs.getInt("second_event"), rs.getInt("state"));
				listeSecondeEvent.add(rs.getInt("second_event"));
			}
			//System.out.println(Long.toBinaryString(this.availabilityMinute));
			
			if(hostStatusStateFlagTmp >= 0)
			this.hostStatusStateFlag = hostStatusStateFlagTmp;
			else hostStatusStateFlag = 0;
			
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
			}*\/
			
			//availability = this.fillstateTab(stateTab);
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return availability;
		}**/
		return availability;
	}
	
	/**
	 * @param bitValue
	 * @param bitPosition
	 * This method place bitValue at bitPosition on availabilityMinute
	 */
	private void setCurrentBitValueOnAvailability(int bitValue, int bitPosition) {
		// TODO Auto-generated method stub
		int position = 59 - bitPosition;
		Long un = (long) 1;
		
		if(bitValue == 1)
		{
			this.availabilityMinute |= un << position;
		}
	}

	/**
	 * 
	 * @param bitFrom
	 * @param bitTo
	 * @param lastBitValue
	 * @param currentBitValue
	 * @return 
	 * @return long mask with
	 */
	private void computeMaskBetweenLastAndCurrentBit(int bitFrom, int bitTo, int bitValue) {
		// TODO Auto-generated method stub
		
		int position;
		Long un;
		
		if(bitTo - bitFrom > 1 && bitValue == 1)
		{
			for(int i = bitFrom + 1; i < bitTo; i++)
			{
				un = (long) 1;
				position = 59-i;
				this.availabilityMinute |= un << position;
				
			}
		}
	}

	private int getStateFromLastBit() {
		// TODO Auto-generated method stub
		int tmpState = 1;
		if(this.getBitPosition(this.availabilityMinute,0) == 1)
			tmpState = 0;
		
		return tmpState;
	}
	
	/**
	 * 
	 * @return 1 if first bit is 1
	 */
	private int getDowntimeFromLastBit() {
		// TODO Auto-generated method stub
		int tmpDowntime = 0;
		if(this.getBitPosition(this.downtimeMinute,0) == 1)
			tmpDowntime = 1;
		
		return tmpDowntime;
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
				
				/**
				 * Block commented 2016-03-21 by Benoit Village
				 
				this.hostStatusStateFlag = rs.getInt("isHostStatus");
				*/
				//case current state ok but previous state nok
				if(rs.getInt("state") == 1 && !this.previousState) {
					
					/**Block commented 2016-03-18 by Benoit Village
					 * if(this.hostStatusStateFlag == 1){
						availability = -60;
						this.state = 0;
						this.availabilityMinute = this.shareVariables.getMaskMax();
					}
					else {
						availability = rs.getInt("fln_availability");
						this.state = 1;
						this.availabilityMinute = this.convertValueToMinuteLong(availability);
					}*/
					
					/**Block added 2016-03-18 by Benoit Village*/
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
				
				/**Block added 2016-03-25 by Benoit Village*/
				this.previousHostServiceOutageBit = this.getBitPosition(availabilityMinute, 0);				
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
			
			if(downtimeFlag == -1) {
				this.isDowntime = this.getPreviousHostServiceDowntimeBit();
				secondeDowntime = 0;
			}
			else if(downtimeFlag == 0){
				this.isDowntime = false;
				secondeDowntime = 60 - secondeDowntime;
			}
			else {
				this.isDowntime = true;
				secondeDowntime = -60 + secondeDowntime;
			}
			
			if(this.isDowntime && this.previousDowntime)
			{
				this.downtimeMinute = this.shareVariables.getMaskMax();
				this.previousHostServiceDowntimeBit = (long) 1;
			}
			else if(this.isDowntime && !this.previousDowntime){
				this.downtimeMinute= convertValueToMinuteLong(secondeDowntime);
				this.previousHostServiceDowntimeBit = (long) 1;
			}
			else if(!this.isDowntime && this.previousDowntime)
			{
				this.downtimeMinute = convertValueToMinuteLong(secondeDowntime);
				this.previousHostServiceDowntimeBit = this.getBitPosition(this.downtimeMinute,0);
			}
			else if(!this.isDowntime && !this.previousDowntime)
			{
				this.downtimeMinute = 0;
				this.previousHostServiceDowntimeBit = 0;
			}
			
			/*if(this.hostId==21 && this.serviceId==46 && minute == 1456959600)
			{
				System.out.println("Downtime minute avant filtre hoststatus : " + Long.toBinaryString(this.downtimeMinute));
				System.out.print("Downtimeflag : " + downtimeFlag);
				System.out.print("IsDowntime : " + this.isDowntime);
				System.out.print("Previous downtime : " + this.previousDowntime);
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}*/
			
			//Apply hoststatus mask
			this.computeDowntimeWithHostStatus(minute);
			
			//We take into account the potential new downtime value of the host service
			if(this.getDowntimeFromLastBit() == 1)
				this.isDowntime = true;
			else this.isDowntime = false;
			
			/*if(this.hostId==21 && this.serviceId==46 && minute == 1456959600)
			{
				System.out.println("Downtime minute avant filtre hoststatus : " + Long.toBinaryString(this.downtimeMinute));
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}*/
			
			this.myConnection.closeResultSet();
		
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
	}

	/**
	 * 
	 * @return true if last host service downtime bit = 1
	 * else 1.
	 */
	private boolean getPreviousHostServiceDowntimeBit() {
		// TODO Auto-generated method stub
		if(this.previousHostServiceDowntimeBit == 1)
		return true;
		else return false;
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
		
		int previousDowntimeInt;
		if(this.previousDowntime)
			previousDowntimeInt = 1;
		else previousDowntimeInt=0;

		if(previousStateInt == 1 && this.state == 0)
		{
			this.shareVariables.setInternOutageEventId(minute);
		}
		
		if(previousDowntimeInt == 0 && this.downtimeState)
		{
			this.shareVariables.setInternDowntimeEventId(minute);
		}
		
		
		if(this.availability != 0 || (this.countNbBit(this.downtimeMinute) > 0) 
			|| this.state != previousStateInt){
			
			/*System.out.println("Je vais insérer pour le host " + this.hostId + " l'availability "
									+ this.availability + " pour un state = " + this.state + " et un"
											+ " downtime = " + this.isDowntime + " minute : " + minute);*/
			this.vList.getHashMapValidator().get(this.validatorId).setAvailabilityMinute(this.availabilityMinute);
			this.vList.getHashMapValidator().get(this.validatorId).setAvailabilityDownMinute(this.availabilityMinute & ~(this.downtimeMinute));
			this.vList.getHashMapValidator().get(this.validatorId).computeDowntime();
			this.vList.getHashMapValidator().get(this.validatorId).setDowntimeDuration(this.downtimeMinute);
			this.vList.getHashMapValidator().get(this.validatorId).setIsDowntime(this.isDowntime);
			this.vList.getHashMapValidator().get(this.validatorId).setPreviousHostServiceOutageBit (this.previousHostServiceOutageBit);
			this.vList.getHashMapValidator().get(this.validatorId).setPreviousHostServiceDowntimeBit (this.previousHostServiceDowntimeBit);
			this.vList.getHashMapValidator().get(this.validatorId).setHostStatusStateFlag(this.hostStatusStateFlag);
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
		this.previousHostStatusStateFlag = this.hostStatusStateFlag;
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
				else this.computeFromPreviousValue(i);
				//System.out.println(Long.toBinaryString(this.availabilityMinute));
				
				this.insertHostServiceAvailability(i);
				this.saveCurrentContext();
					
			}
			else if(computeCase == 2) {
				
				if(existDateMinute(i)) {
					this.getHostServiceDowntimeState(i);
				}
				else this.computeFromPreviousValue(i);
				
				this.insertHostServiceAvailability(i);
				this.saveCurrentContext();
			}
			else if(computeCase == 3) {
								
				if(existDateMinute(i)) {
					this.computeAvailabilityMinute(i);
				}
				else {
						this.computeFromPreviousValue(i);
					}
				

				
				this.insertHostServiceAvailability(i);
				this.saveCurrentContext();
				
			}
		}
	}

	private void computeFromPreviousValue(int minutes) {
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
		
		/**BEGIN Block added 2016-03-29 by Benoit Village*/
		
		int tmpUnavailability = this.countNbBit(this.availabilityMinute);
		//Take into account Hoststatus state in service compute
		this.computeAvailabilityWithHostStatus(minutes);
		int unavailability = this.countNbBit(this.availabilityMinute);
		if(this.getStateFromLastBit() == 0){
			this.availability = 0 - unavailability;
			this.state = 0;
		}
		else {
			this.availability = unavailability;
			this.state = 1;
		}
		
		if(tmpUnavailability != unavailability)
			this.hostStatusStateFlag = 1;
		
		//Apply hoststatus mask
		this.computeDowntimeWithHostStatus(minutes);
		
		//We take into account the potential new downtime value of the host service
		if(this.getDowntimeFromLastBit() == 1)
			this.isDowntime = true;
		else this.isDowntime = false;
		
		/**END Block added 2016-03-29 by Benoit Village*/
	}

}
