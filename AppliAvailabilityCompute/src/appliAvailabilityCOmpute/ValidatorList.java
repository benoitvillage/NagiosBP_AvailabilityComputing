package appliAvailabilityCOmpute;

import java.util.ArrayList;
import java.util.HashMap;

public class ValidatorList {

	private HashMap <Integer,Validator> hashMapValidator = new HashMap <Integer,Validator>();
	private ArrayList<Integer> listHSValidator = new ArrayList<Integer>();
	private ArrayList<Integer> listHighBPValidator = new ArrayList<Integer>();
	private ArrayList<Integer> listBPValidator = new ArrayList<Integer>();
	
	
	/**
	 * Associate idValidator key with validator
	 * @param idValidator
	 * @param hostServiceValidator
	 */
	public void addValidator(int idValidator, Validator validator) {

		this.hashMapValidator.put(idValidator, validator);
		
	}

	/**
	 * Add a host service validator hashmap key in arraylist
	 * @param idValidator
	 */
	public void addHSValidatorKey(int idValidator) {
		// TODO Auto-generated method stub
		this.listHSValidator.add(idValidator);
	}

	/**
	 * Add an application validator hashmap key in arraylist
	 * @param idValidator
	 */
	public void addBPValidatorKey(int idValidator) {
		// TODO Auto-generated method stub
		this.listBPValidator.add(idValidator);
	}

	public void displayValidators() {
		// TODO Auto-generated method stub
		for(int i = 0; i < this.hashMapValidator.size(); i++)
		{
			this.hashMapValidator.get(i).displayValidator();
		}
	}

	public void addSenderListenerApp(int idListender, int idSender) {
		// TODO Auto-generated method stub
		
		int idValidator = 0;
		
		for(int i = 0; i < this.listBPValidator.size(); i++)
		{
			idValidator = this.listBPValidator.get(i);
			
			try {
				
				if(this.hashMapValidator.get(idValidator).getIdApplication() == idListender)
					this.hashMapValidator.get(idValidator).addSender(getAppliValidatorId(idSender));
				
			} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Aucun validator Appli trouvé pour le sender appliId " + idSender );
			}
			
			try {
				
				if(this.hashMapValidator.get(idValidator).getIdApplication() == idSender)
						this.hashMapValidator.get(idValidator).addListener(getAppliValidatorId(idListender));
				
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println("Aucun validator Appli trouvé pour le listender appliId " + idListender );
			}	
		
		}
	}

	private int getAppliValidatorId(int idAppli) throws Exception {
		// TODO Auto-generated method stub
		
		int idValidatorApplication = 0;
		int idValidator = 0;
		
		for(int i = 0; i < this.listBPValidator.size(); i++)
		{
			idValidator = this.listBPValidator.get(i);
			if(this.hashMapValidator.get(idValidator).getIdApplication() == idAppli)
				idValidatorApplication = idValidator;
		}
		
		if(idValidatorApplication == 0)
			throw new Exception();
		
		return idValidatorApplication;
	}

	public void addSenderListenerHSApp(int idHost, int idService, int idApplication) {
		// TODO Auto-generated method stub
		
		int idValidatorApplication = 0;
		int idValidator = 0;
		
		try {
		
			for(int i = 0; i < this.listBPValidator.size(); i++)
			{
				idValidator = this.listBPValidator.get(i);
				if(this.hashMapValidator.get(idValidator).getIdApplication() == idApplication) {
					
						this.hashMapValidator.get(idValidator).addSender(this.getHSValidatorId(idHost,idService));
					
					idValidatorApplication = idValidator;
				}
				
			}
		
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("Aucun validator hs n'a été trouvé pour idHost :" + idHost + " idService : " + idService);
		}
		
		try {
		
			if(idValidatorApplication == 0)
				throw new Exception();
			
			
			for(int i = 0; i < this.listHSValidator.size(); i++)
			{
				idValidator = this.listHSValidator.get(i);
				if(this.hashMapValidator.get(idValidator).getIdHost() == idHost
						&& this.hashMapValidator.get(idValidator).getIdService() == idService)
					this.hashMapValidator.get(idValidator).addListener(idValidatorApplication);
			}
		
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("Aucun validator appli n'a été trouvé pour idAppli :" + idApplication);
		}
		
		
		
	}

	private int getHSValidatorId(int idHost, int idService) throws Exception {
		// TODO Auto-generated method stub
		int idHSValidator = -1;
		int idValidator = -1;
		for(int i = 0; i < this.listHSValidator.size(); i++)
		{
			idValidator = this.listHSValidator.get(i);

			if(this.hashMapValidator.get(idValidator).getIdHost() == idHost
					&& this.hashMapValidator.get(idValidator).getIdService() == idService) 
				idHSValidator = idValidator;
		}
		
		if(idHSValidator == -1)
			throw new Exception();
		
		return idHSValidator;
	}

	public void displayLinks() {

		for(int i = 1; i < this.hashMapValidator.size(); i++)
		{
			//display for Keycopter portal application
			if(this.hashMapValidator.get(i).getIdApplication() == 147) {
				this.hashMapValidator.get(i).displayValidator();
				this.hashMapValidator.get(i).displaySender(this.hashMapValidator);
				this.hashMapValidator.get(i).displayListener(this.hashMapValidator);
			}
		}
		
		
	}
	
	public HashMap<Integer, Validator> getHashMapValidator() {
		return hashMapValidator;
	}

	public void setHashMapValidator(HashMap<Integer, Validator> hashMapValidator) {
		this.hashMapValidator = hashMapValidator;
	}

	public ArrayList<Integer> getListHSValidator() {
		return listHSValidator;
	}

	public void setListHSValidator(ArrayList<Integer> listHSValidator) {
		this.listHSValidator = listHSValidator;
	}

	public ArrayList<Integer> getListHighBPValidator() {
		return listHighBPValidator;
	}

	public void setListHighBPValidator(ArrayList<Integer> listHighBPValidator) {
		this.listHighBPValidator = listHighBPValidator;
	}

	public ArrayList<Integer> getListBPValidator() {
		return listBPValidator;
	}

	public void setListBPValidator(ArrayList<Integer> listBPValidator) {
		this.listBPValidator = listBPValidator;
	}


	
	
}
