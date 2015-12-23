package appliAvailabilityCOmpute;

import java.util.ArrayList;
import java.util.HashMap;

import org.omg.PortableServer.THREAD_POLICY_ID;


public class Validator {

	/**eor_dwh.d_application.dap_id*/
	private int idApplication = -1;
	
	/**eor_dwh.d_host.dho_id*/
	private int idHost;
	
	/**eor_dwh.d_service.dse_id*/
	private int idService;

	/**ma1_eon, ma1_sog, ma1_sol*/
	private String source;
	
	/**ET, OU, MIN(x)*/
	private String type;
	
	/**core_infrastructure, customer_access...*/
	private String category;

	/**Min value if bp validator type is min*/
	private int min;

	
	/**Group of UnitState which are state and 
	 * downtime context on 5 second period*/
	private ArrayList<UnitState> unitStateGroup = new ArrayList<UnitState>();

	private ArrayList<Integer> listSender = new ArrayList<Integer>();

	private ArrayList<Integer> listListener = new ArrayList<Integer>();

	private int numberSenderComputed = 0;
	
	HashMap<Integer,Long> listAvailabilityMinute = new HashMap<Integer,Long>();
	HashMap<Integer,Long> listAvailabilityDownMinute = new HashMap<Integer,Long>();
	HashMap<Integer,Long> listDowntimeMinute = new HashMap<Integer,Long>();
	ArrayList<Integer> listEpochEvent = new ArrayList<Integer>();

	private ShareVariables shareVariables;
	
	private long availabilityMinute = 0;
	private long availabilityDownMinute = 0;
	private int effectiveDowntimeDuration = 0;

	private ValidatorList vList;

	private int unavailability;

	private int unavailabilityDown;

	private MyConnection myConnection;

	private int downtimeDuration;

	private boolean isDowntime;

	private boolean areEventsOnPeriod;

	/**
	 * Only for host service validator
	 * 0 state is OK
	 * 1 outage not inherited from Hoststatus
	 * 2 outage inherited from Hoststatus
	 */
	private int hostStatusStateFlag = 0;
	
	
	
	//Constructor for application validator
	public Validator(int idApplication, String type, String category,String source, ShareVariables shareVariable, ValidatorList vList, int min, MyConnection myConnection)
	{
		this.idApplication = idApplication;
		this.type = type;
		this.category = category;
		this.source = source;
		this.min = min;
		this.shareVariables = shareVariable;
		this.vList = vList;
		this.myConnection = myConnection;
		this.areEventsOnPeriod = true;
		this.initUnitStateGroup();
	}
	
	//Constructor for host service validator
	public Validator(int idHost, int idService, String source, ShareVariables shareVariable, ValidatorList vList, MyConnection myConnection)
	{
		this.idHost = idHost;
		this.idService = idService;
		this.source = source;
		this.vList = vList;
		this.areEventsOnPeriod = true;
		this.myConnection = myConnection;
		
		this.initUnitStateGroup();
	}

	/**
	 * This method initialize the 12 unit state
	 * of the validator
	 */
	private void initUnitStateGroup() {
		// TODO Auto-generated method stub
		
		for(int i = 0; i < 11; i++)
		{
			UnitState us = new UnitState();
			this.unitStateGroup.add(us);
		}
		
	}

	public void displayValidator() {
		// TODO Auto-generated method stub
		if(this.idApplication == -1)
		{
			System.out.println("Validator HS - Host : " + this.idHost + " Service : " 
									+ this.idService + 
									" Source : " + this.source);
		}
		else System.out.println("Validator BP - BP : " + this.idApplication 
									+ " Category : " + this.category 
									+ " Type : " +this.type + " source " + this.source);
	}
	
	

	/**This method allows to inform all validator listener availability is computed
	 * @param vList 
	 * @param computationPlan */
	public void informListener(ValidatorList vList, ComputationPlan computationPlan) {

		int validatorId;
		
		for(int i = 0; i < this.listListener.size(); i++)
		{
			validatorId = this.listListener.get(i);
			vList.getHashMapValidator().get(validatorId).incrementComputedSenderNumber(vList, validatorId, computationPlan);
		}
		
	}

	private void incrementComputedSenderNumber(ValidatorList vList, int validatorId, ComputationPlan computationPlan) {

		this.numberSenderComputed++;
		
		if(this.numberSenderComputed == this.listSender.size())
			computationPlan.addValidatorNextPhase(validatorId);
			
	}
	
	public void compute() {

		this.computeAreEventOnPeriod();
		if(this.areEventsOnPeriod)
		{
			for(int i = this.shareVariables.getEpochBegin(); i <= this.shareVariables.getEpochEnd(); i+=60)
			{	
				this.availabilityDownMinute = 0;
				this.availabilityMinute = 0;
				
				this.computeMinute(i);
				this.insertMinute(i);
			}
		}
		
		
	}
	



	private void computeAreEventOnPeriod() {
		// TODO Auto-generated method stub
		boolean areEvents = false;
		int validatorId;
		
		for(int i=0; i < this.listSender.size(); i++)
		{
			validatorId = this.listSender.get(i);
			if(this.vList.getHashMapValidator().get(validatorId).getAreEventsOnPeriod())
			{
				areEvents= true;
			}
		}
		
		this.areEventsOnPeriod = areEvents;
	}


	public void insertMinute(int minute) {
		
		this.computeUnavailability();
		long isOutageTmp =  this.availabilityMinute & (long) 1;
		int isOutage=0;
		
		if (isOutageTmp == (long) 1)
			isOutage=1;
		
		if(this.idApplication == -1)
			this.myConnection.insertHSMinute(minute, this.idHost, this.idService, this.source, this.unavailability, this.unavailabilityDown, this.downtimeDuration,this.effectiveDowntimeDuration, this.isDowntime, isOutage, this.hostStatusStateFlag);
		
		if(this.idApplication != -1 && (this.downtimeDuration > 0 || this.unavailability != 0) )
			this.myConnection.insertAppliMinute(minute, this.idApplication, this.category, "Global", this.source, this.unavailability, this.unavailabilityDown, this.effectiveDowntimeDuration, isOutage);
		
		
		if(this.unavailability != 0) {
			
			this.addListAvailabilityMinute(minute,this.availabilityMinute);
			this.addListAvailabilityDownMinute(minute,this.availabilityDownMinute);
			this.addListEpochEvent(minute);
		
		}
		
		
	}

	private void computeUnavailability() {
		// TODO Auto-generated method stub
		
		this.unavailability=0;
		this.unavailabilityDown=0;
		
		if(this.availabilityMinute == 0x0FFFFFFFFFFFFFFFL && availabilityDownMinute  == 0x0FFFFFFFFFFFFFFFL) {
			this.unavailability=60;
			this.unavailabilityDown=60;
		}
		else if(this.availabilityMinute == 0 && this.availabilityDownMinute == 0)
		{
			this.unavailability=0;
			this.unavailabilityDown=0;
		}
		else {
			this.unavailability += countNbBit(this.availabilityMinute);
			this.unavailabilityDown += countNbBit(this.availabilityDownMinute);
			
		}
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

	private void computeMinute(int minute) {
		
		//Si validator de type ET
		if(this.type.equals("ET"))
			this.computeANDMinute(minute);
		else if(this.type.equals("OU"))
			this.computeORMinute(minute);
		else this.computeMINMinute(minute);
		
		this.computeDowntime();
		
	}

	public void computeDowntime() {

		long effectiveDowntime = this.availabilityMinute ^ this.availabilityDownMinute;
		
		this.effectiveDowntimeDuration = this.countNbBit(effectiveDowntime);
		
	}

	private void computeANDMinute(int minute) {
		
		int validatorId;
		for(int i = 0; i < this.listSender.size(); i++)
		{
			validatorId = this.listSender.get(i);
			this.availabilityMinute |= this.vList.getHashMapValidator().get(validatorId).getAvailabilityMinute(minute);
			this.availabilityDownMinute |= this.vList.getHashMapValidator().get(validatorId).getAvailabilityDownMinute(minute);
		}
	}
	

	private void computeORMinute(int minute) {

		int validatorId;
		
		if(this.listSender.size() > 0)
		{
			validatorId=this.listSender.get(0);
			this.availabilityMinute = this.vList.getHashMapValidator().get(validatorId).getAvailabilityMinute(minute);
			this.availabilityDownMinute = this.vList.getHashMapValidator().get(validatorId).getAvailabilityDownMinute(minute);
		}
		
		for(int i = 0; i < this.listSender.size(); i++)
		{
			validatorId = this.listSender.get(i);
			this.availabilityMinute &= this.vList.getHashMapValidator().get(validatorId).getAvailabilityMinute(minute);
			this.availabilityDownMinute &= this.vList.getHashMapValidator().get(validatorId).getAvailabilityDownMinute(minute);
		}
	}
	
	private void computeMINMinute(int minute) {

		int validatorId;
		int nbTrueSeconde;
		int nbTrueSecondeDown;

		long availabilityMinute;
		long intermediaire1;
		long intermediaire2;
		long availabilityMinuteDown;
		
		for(int i=59; i >=0; i--)
		{
			nbTrueSeconde=0;
			nbTrueSecondeDown=0;
			
			for(int j=0; j<this.listSender.size();j++)
			{
				validatorId = this.listSender.get(j);
				availabilityMinute = this.vList.getHashMapValidator().get(validatorId).getAvailabilityMinute(minute);
				availabilityMinuteDown = this.vList.getHashMapValidator().get(validatorId).getAvailabilityMinute(minute);
				
				if(getBitPosition(availabilityMinute,i) == 0)
					nbTrueSeconde++;
				
				if(getBitPosition(availabilityMinuteDown,i) == 0)
					nbTrueSecondeDown++;
			}
			
			if(nbTrueSeconde < this.min)
			{
				this.availabilityMinute |= 1<<i;
			}
			if(nbTrueSecondeDown < this.min)
			{
				this.availabilityDownMinute |= 1<<i;
			}
		}
		
		
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

	private long getAvailabilityMinute(int minute) {
		// TODO Auto-generated method stub
		
		long result;
		
		if(this.listEpochEvent.contains(minute))
		{
			result = this.listAvailabilityMinute.get(minute);
		}
		else result = 0;
		
		
		return result;
	}
	
	private long getAvailabilityDownMinute(int minute) {
		// TODO Auto-generated method stub
		
		long result;
		
		if(this.listEpochEvent.contains(minute))
		{
			result = this.listAvailabilityDownMinute.get(minute);
		}
		else result = 0;
		
		
		return result;
	}
	
	public void setDowntimeDuration(long downtimeMinute) {
		// TODO Auto-generated method stub
		
		this.downtimeDuration = this.countNbBit(downtimeMinute);

	}

	public void addListAvailabilityMinute(int minute, long availabilityMinute) {
		// TODO Auto-generated method stub
		this.listAvailabilityMinute.put(minute, availabilityMinute);
	}

	public void addListAvailabilityDownMinute(int minute, long availabilityDownMinute) {
		// TODO Auto-generated method stub
		this.listAvailabilityDownMinute.put(minute, availabilityDownMinute);
	}

	public void addListDowntimeMinute(int minute, long downtimeMinute) {
		// TODO Auto-generated method stub
		this.listDowntimeMinute.put(minute,downtimeMinute);
	}

	public void addListEpochEvent(int minute) {
		// TODO Auto-generated method stub
		this.listEpochEvent.add(minute);
	}

	public void setAvailabilityMinute(long availabilityMinute) {
		// TODO Auto-generated method stub
		this.availabilityMinute= availabilityMinute;
	}

	public void setAvailabilityDownMinute(long availabilityDownMinute) {
		// TODO Auto-generated method stub
		this.availabilityDownMinute = availabilityDownMinute;
	}

	public void setIsDowntime(boolean isDowntime) {
		// TODO Auto-generated method stub
		this.isDowntime = isDowntime;
	}


	public int getIdHost() {
		return idHost;
	}

	public int getIdService() {
		return idService;
	}

	public String getSource() {
		return source;
	}

	public String getType() {
		return type;
	}

	public String getCategory() {
		return category;
	}
	
	public int getIdApplication() {
		return idApplication;
	}

	public void addSender(int idSender) {
		// TODO Auto-generated method stub
		this.listSender.add(idSender);
	}

	public void addListener(int idListender) {
		// TODO Auto-generated method stub
		this.listListener.add(idListender);
	}

	public void displaySender(HashMap<Integer, Validator> hashMapValidator) {
		
		System.out.println("Liste de tous mes sender ");
		for(int i = 0; i < this.listSender.size(); i++)
		{
			hashMapValidator.get(this.listSender.get(i)).displayValidator();
		}
		
	}
	
	public void displayListener(HashMap<Integer, Validator> hashMapValidator) {
		
		System.out.println("Les de tous mes listener");
		for(int i = 0; i < this.listListener.size(); i++)
		{
			hashMapValidator.get(this.listListener.get(i)).displayValidator();
		}
		
	}

	public void setAreEventsOnPeriod(boolean areEventsOnPeriod) {
		// TODO Auto-generated method stub
		this.areEventsOnPeriod = areEventsOnPeriod;
	}
	private boolean getAreEventsOnPeriod() {
		// TODO Auto-generated method stub
		return this.areEventsOnPeriod;
	}

	public void setHostStatusStateFlag(int hostStatusStateFlag) {
		// TODO Auto-generated method stub
		this.hostStatusStateFlag = hostStatusStateFlag;
	}

	
	
	
}
