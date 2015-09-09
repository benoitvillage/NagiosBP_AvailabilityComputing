package appliAvailabilityCOmpute;

import java.util.ArrayList;


public class ComputationPlan {

	private ArrayList<ArrayList<Integer>> cPlanList = new ArrayList<ArrayList<Integer>>();
	private ArrayList<Integer> listValidatorNextPhase = new ArrayList<Integer>();
	private ValidatorList vList;
	private HostServiceList myHSList;
	private MyConnection myConnection;
	private ShareVariables shareVariable;
	
	public ComputationPlan(ValidatorList vList, HostServiceList myHSList, MyConnection myConnection, ShareVariables shareVariable) {

		this.vList = vList;
		this.myHSList = myHSList;
		this.myConnection = myConnection;
		this.shareVariable = shareVariable;
		initComputationPlanList();
		computeExecutionPlan();
	}



	private void initComputationPlanList() {
		
		this.cPlanList.add(this.vList.getListHSValidator());
		
	}
	
	private void computeExecutionPlan() {

		int i = 0;
		int validatorId = -1;
		boolean endComputation = false;
		
		while(!endComputation)
		{
			for(int j = 0; j < this.cPlanList.get(i).size(); j++)
			{
				validatorId = this.cPlanList.get(i).get(j);
				this.vList.getHashMapValidator().get(validatorId).informListener(vList, this);
			}
			
			if(this.listValidatorNextPhase.size() > 0)
			{
				this.cPlanList.add(this.listValidatorNextPhase);
				this.listValidatorNextPhase = new ArrayList<Integer>();
				i++;
			}
			else endComputation = true;
		}
		
	}

	public void addValidatorNextPhase(int validatorId) {

		this.listValidatorNextPhase.add(validatorId);
		
	}
	
	public void displayComputationPlan(ValidatorList vList)
	{
		int validatorId = 0;
		
		for(int i = 0; i < this.cPlanList.size(); i ++){
			
			System.out.println("Phase " + i);
		
			for(int j = 0; j < this.cPlanList.get(i).size(); j++)
			{
				validatorId = this.cPlanList.get(i).get(j);
				vList.getHashMapValidator().get(validatorId).displayValidator();
			}
			
		}
	}



	public void executeComputation() {
		
		int validatorId = 0;
		
		for(int i = 0; i < this.cPlanList.size(); i++) {
			
			//warning
			System.out.println("Phase " + i);
			
			if(i == 0)
			{
				this.computeHSPhase(i);
			}
			else{
				
				for(int j=0; j < this.cPlanList.get(i).size(); j++)
				{
					validatorId = this.cPlanList.get(i).get(j);
					this.vList.getHashMapValidator().get(validatorId).compute();
				}
			
			}
		}
		
	}



	private void computeHSPhase(int i) {
		// TODO Auto-generated method stub
		
		HostService hs = new HostService(this.myConnection,this.shareVariable,this.vList);
		int hostId;
		int serviceId;
		String hostServiceSource;
		boolean isDowntime;
		boolean previousState;
		boolean previousDowntime;
		int availability;
		int state =  0;
		boolean executeHSStateLookup;
		boolean executeHSDowntimeLookup;
		int validatorId;
		
		for(int j = 0; j < this.cPlanList.get(i).size(); j++)
		{
				validatorId = this.cPlanList.get(i).get(j);
				hostServiceSource = this.vList.getHashMapValidator().get(validatorId).getSource();
				hostId = this.vList.getHashMapValidator().get(validatorId).getIdHost();
				serviceId = this.vList.getHashMapValidator().get(validatorId).getIdService();
				isDowntime = false;				
				previousState = this.myConnection.getHSPreviousState(hostId,serviceId);
				previousDowntime = this.myConnection.getHSPreviousDowntime(hostId,serviceId);
				availability = 0;
				state = 1;
				
				hs.initializeDateMinuteStateArray();
				executeHSStateLookup = this.myConnection.testHSOutage(hostId,serviceId,hs);
				executeHSDowntimeLookup = this.myConnection.testHSDowntime(hostId,serviceId,hs);
				
				//warning
				if(hostId == 2 && serviceId == 4)
					System.out.print("");
					
				int k = this.shareVariable.getEpochBegin();
				//Compute for each minute
				
				if((!previousState || executeHSStateLookup) && (previousDowntime || executeHSDowntimeLookup)) {
					
					this.vList.getHashMapValidator().get(validatorId).setAreEventsOnPeriod(true);
					//initialisation de l'objet hs
					hs.hostServiceInit(validatorId,hostServiceSource,hostId,serviceId,isDowntime,previousState,previousDowntime, availability, state);
					
					//Créer table log temporaire 1j /1 host service
					this.myConnection.createTmpLogHSTable(hostId,serviceId);
										
					//Créer table log downtime temporaire 1j /1 host service
					this.myConnection.createTmpLogDowntimeHSTable(hostId,serviceId);
					
					hs.compute(this.shareVariable, 1);
					
					//System.out.println("I finish host " + hostId + " service " + serviceId +" which had downtime and outage");
				}
				else if(!(!previousState || executeHSStateLookup) && (previousDowntime || executeHSDowntimeLookup))
				{
					//System.out.println("Create tmp host service downtime table " + hostId + " service " + serviceId +" which had downtime and outage");
					
					//If there are not Outage event for this HS, there will be no computation on linked application
					this.vList.getHashMapValidator().get(validatorId).setAreEventsOnPeriod(false);
					//initialisation de l'objet hs
					hs.hostServiceInit(validatorId,hostServiceSource,hostId,serviceId,isDowntime,previousState,previousDowntime, availability, state);
					
					//Créer table log downtime temporaire 1j /1 host service
					this.myConnection.createTmpLogDowntimeHSTable(hostId,serviceId);
					
					hs.compute(this.shareVariable, 2);
				}
				else if((!previousState || executeHSStateLookup) && !(previousDowntime || executeHSDowntimeLookup))
				{
					this.vList.getHashMapValidator().get(validatorId).setAreEventsOnPeriod(true);
					//initialisation de l'objet hs
					hs.hostServiceInit(validatorId,hostServiceSource,hostId,serviceId,isDowntime,previousState,previousDowntime, availability, state);
					
					//Créer table log temporaire 1j /1 host service
					this.myConnection.createTmpLogHSTable(hostId,serviceId);
					
					hs.compute(this.shareVariable, 3);
					
					//System.out.println("I finish host " + hostId + " service " + serviceId +" which had no downtime but outage");
				}
				//If there are not Outage event for this HS, there will be no computation on linked application
				else this.vList.getHashMapValidator().get(validatorId).setAreEventsOnPeriod(false);
				
					//System.out.println("I finish host " + hostId + " service " + serviceId +" which had no downtime and no outage");
				
				this.myConnection.dropDayLogHSTable();
				this.myConnection.dropDayDowntimeLogHSTable();
		}
	}

	
}
