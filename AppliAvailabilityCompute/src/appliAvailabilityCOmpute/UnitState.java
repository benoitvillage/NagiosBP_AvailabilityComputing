package appliAvailabilityCOmpute;

public class UnitState {

	private boolean isDowntime;
	private boolean state;
	private boolean stateDowntime;
	
	
	public UnitState() {
		this.isDowntime = false;
		this.state = true;
		this.stateDowntime = true;
	}


	public boolean isDowntime() {
		return isDowntime;
	}


	public void setDowntime(boolean isDowntime) {
		this.isDowntime = isDowntime;
	}


	public boolean isState() {
		return state;
	}


	public void setState(boolean state) {
		this.state = state;
	}


	public boolean isStateDowntime() {
		return stateDowntime;
	}


	public void setStateDowntime(boolean stateDowntime) {
		this.stateDowntime = stateDowntime;
	}
	
}
