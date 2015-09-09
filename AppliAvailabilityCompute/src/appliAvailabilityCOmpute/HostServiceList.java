package appliAvailabilityCOmpute;

import java.util.ArrayList;

public class HostServiceList {

	
	private MyConnection myConnection;
	private ArrayList<ArrayList<String>> listHostService = new ArrayList<ArrayList<String>>();
	private ShareVariables shareVariables;

	public HostServiceList(MyConnection myConnection, ShareVariables shareVariables) {
		
		this.shareVariables = shareVariables;
		this.myConnection = myConnection;
		this.initializeHostServiceList();
		
	}

	/**
	 * This method
	 */
	private void initializeHostServiceList() {
		
		this.myConnection.getHostServiceList(this);
		
	}

	/**
	 * This method allows to add a host service in host service list
	 * @param hsAttributes
	 */
	public void addHostServiceToList(ArrayList<String> hsAttributes) {
		
		this.listHostService.add(hsAttributes);
	}

	/**
	 * In this method, we compute host service availability for each host_service
	 */
	/*public void computeHSList(ValidatorList vList) {
		
		HostService hs = new HostService(this.myConnection, this.shareVariables, vList);
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
		
		for(int i = 0; i < this.listHostService.size(); i++)
		{
			
				hostServiceSource = this.listHostService.get(i).get(0);
				hostId = Integer.parseInt(this.listHostService.get(i).get(1));
				serviceId = Integer.parseInt(this.listHostService.get(i).get(2));
				isDowntime = false;				
				previousState = this.myConnection.getHSPreviousState(hostId,serviceId);
				previousDowntime = this.myConnection.getHSPreviousDowntime(hostId,serviceId);
				availability = 0;
				state = 1;
				
				hs.initializeDateMinuteStateArray();
				executeHSStateLookup = this.myConnection.testHSOutage(hostId,serviceId,hs);
				executeHSDowntimeLookup = this.myConnection.testHSDowntime(hostId,serviceId,hs);
				int validatorId = 1;

				
				int k = this.shareVariables.getEpochBegin();
				//Compute for each minute
				
				int l;
				if(hostId == 2 && serviceId == 8)
					 l = 0;
				
				if((!previousState || executeHSStateLookup) && (previousDowntime || executeHSDowntimeLookup)) {
					//initialisation de l'objet hs
					hs.hostServiceInit(1,hostServiceSource,hostId,serviceId,isDowntime,previousState,previousDowntime, availability, state);
					
					//Créer table log temporaire 1j /1 host service
					this.myConnection.createTmpLogHSTable(hostId,serviceId);
										
					//Créer table log downtime temporaire 1j /1 host service
					this.myConnection.createTmpLogDowntimeHSTable(hostId,serviceId);
					
					hs.compute(this.shareVariables, 1);
					
					//System.out.println("I finish host " + hostId + " service " + serviceId +" which had downtime and outage");
				}
				else if(!(!previousState || executeHSStateLookup) && (previousDowntime || executeHSDowntimeLookup))
				{
					//System.out.println("Create tmp host service downtime table " + hostId + " service " + serviceId +" which had downtime and outage");

					//initialisation de l'objet hs
					hs.hostServiceInit(1,hostServiceSource,hostId,serviceId,isDowntime,previousState,previousDowntime, availability, state);
					
					//Créer table log downtime temporaire 1j /1 host service
					this.myConnection.createTmpLogDowntimeHSTable(hostId,serviceId);
					
					hs.compute(this.shareVariables, 2);
					
					//System.out.println("I finish host " + hostId + " service " + serviceId +" which had downtime but no outage");
				}
				else if((!previousState || executeHSStateLookup) && !(previousDowntime || executeHSDowntimeLookup))
				{
					//initialisation de l'objet hs
					hs.hostServiceInit(1,hostServiceSource,hostId,serviceId,isDowntime,previousState,previousDowntime, availability, state);
					
					//Créer table log temporaire 1j /1 host service
					this.myConnection.createTmpLogHSTable(hostId,serviceId);
					
					hs.compute(this.shareVariables, 3);
					
					//System.out.println("I finish host " + hostId + " service " + serviceId +" which had no downtime but outage");
				}
				
					//System.out.println("I finish host " + hostId + " service " + serviceId +" which had no downtime and no outage");
				
				this.myConnection.dropDayLogHSTable();
				this.myConnection.dropDayDowntimeLogHSTable();
		}
		
		
	}*/

	public ArrayList<ArrayList<String>> getListHostService() {
		return listHostService;
	}

	public void setListHostService(ArrayList<ArrayList<String>> listHostService) {
		this.listHostService = listHostService;
	}

	public void displayHSList() {
		
		String source;
		String host;
		String service;
		
		
		for(int i = 0; i< this.listHostService.size(); i++)
		{
			source = this.listHostService.get(i).get(0);
			host = this.listHostService.get(i).get(1);
			service = this.listHostService.get(i).get(2);
			
			System.out.println("Source : " + source + " host : " + host + " service : " + service ) ;
		}
		
		
	}
	
	
	
}