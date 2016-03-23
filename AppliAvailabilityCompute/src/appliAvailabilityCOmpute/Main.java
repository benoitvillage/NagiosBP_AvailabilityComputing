package appliAvailabilityCOmpute;


public class Main {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		if(args[0] != null && args[1] != null)
		{
			System.out.println("Epoch begin : " + args[0] + " epoch end : " + args[1]);
			//Initialize global varialbe from configuration file
			ShareVariables shareVariable = new ShareVariables();
			
			System.out.println("variables created (e.g." + shareVariable.getHost());
			
			//Create the connection
			MyConnection myConnection = new MyConnection(shareVariable);
			
			myConnection.setChargementId();
			
			System.out.println("Chargement id created " + shareVariable.getChargementId());
			
			
			//drop temp tables
			/**myConnection.dropDayLogStateUnavailability();
			myConnection.dropDayLogDownUnavailability();*/

			
			//We get date, begin minute and end minute from epoch_begin 
			//and epoch_end stored in argument
			myConnection.determineTimeScope(args[0], args[1]);
			
			System.out.println("Timescope determined Date : " + shareVariable.getDate() + " Time_beg : "
					+ shareVariable.getEpochBegin() + " Time_end : " + shareVariable.getEpochEnd());
			
			ValidatorList vList = new ValidatorList();
			
			//create validator
			myConnection.createValidator(vList);
			
			//vList.displayValidators();
			
			//affect listener and senderNumber for each validator
			myConnection.createValidatorLinks(vList);
			
			//vList.displayLinks();
			//test
			
			HostServiceList myHSList = new HostServiceList(myConnection, shareVariable);
			
		    ComputationPlan cPlan = new ComputationPlan(vList, myHSList,myConnection, shareVariable);
		    //cPlan.displayComputationPlan(vList);
		    
		    myConnection.dropDayLogTable();
			myConnection.dropDayDowntimeLogTable();
			myConnection.dropDayLogHSTable();
			myConnection.dropDayDowntimeLogHSTable();
			myConnection.dropDayLogUnavailability();
		    
			myConnection.createDayUnavailabilityTable();
			System.out.println("Temp log unavailability table created");
			//Create temporary one day log table in dwh
			myConnection.createDayLogTable();
			
			System.out.println("Temp log table created");
			
			//Create temporary one day downtime log table in dwh
			myConnection.createDayDowntimeLogTable();
			
			System.out.println("Temp log downtime table created");
		    
		    
		    
			cPlan.executeComputation();

			
		  //Delete temporary one day log table in dwh
			myConnection.dropDayLogTable();
			
			//Delete temporary one day downtime log table in dwh
			myConnection.dropDayDowntimeLogTable();
			
			//Delete temporary one day log table in dwh
			myConnection.dropDayLogHSTable();
			
			//Delete temporary one day downtime log table in dwh
			myConnection.dropDayDowntimeLogHSTable();
			
			//Delete temporary unavailability table in dwh
			myConnection.dropDayLogUnavailability();
		    
			myConnection.closeConnection();
			
		}
		else System.out.println ("usage : java - jar HostServiceAvailabilityCompute.jar epoch_begin epoch_end");
		//	System.out.println("Veuillez donner la date d'importation en paramètre");
		
	}

}
