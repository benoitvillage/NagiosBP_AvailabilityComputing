package appliAvailabilityCOmpute;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;


public class MyConnection {

	
	  private Connection connect_eor_dwh = null;
	  private Connection connect_eor_ods = null;
	  private Statement statement = null;
	  private PreparedStatement preparedStatement = null;
	  private ResultSet resultSet = null;
	  private String odsDatabase = "eor_ods";
	  private String dwhDatabase = "eor_dwh";
	  private ShareVariables shareVariable;
	  
	  public MyConnection (ShareVariables shareVariable) {
		  
		  this.shareVariable = shareVariable;
		  try {
			this.readDataBase();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		  
	  }
	  
	  public void readDataBase() throws Exception {
	    try {
	      // this will load the MySQL driver, each DB has its own driver
	      Class.forName("com.mysql.jdbc.Driver");
	      // setup the connection with the DB.
	     /* connect_eor_dwh = DriverManager
	          .getConnection("jdbc:mysql://" + this.shareVariable.getHost() + ":3306/eor_dwh?"
	              + "user=" + this.shareVariable.getUser() + "&password=" + 
	              this.shareVariable.getPassword());*/
	      
	      connect_eor_dwh = DriverManager
		          .getConnection("jdbc:mysql://localhost:3306/eor_dwh?user=eyesofreport&password=SaintThomas,2014");
	      	
	      connect_eor_ods = DriverManager
		          .getConnection("jdbc:mysql://localhost:3306/eor_ods?user=eyesofreport&password=SaintThomas,2014");
	      
	      
	      /*connect_eor_ods = DriverManager
		          .getConnection("jdbc:mysql://" + this.shareVariable.getHost() + ":3306/eor_ods?"
			              + "user=" + this.shareVariable.getUser() + "&password=" + 
			              this.shareVariable.getPassword());*/

	      
	    } catch (Exception e) {
	      throw e;
	    }

	  }

	public void determineTimeScope(String epoch_beg, String epoch_end) {
		
		try {
			preparedStatement = connect_eor_dwh
				      .prepareStatement("SELECT date_format(from_unixtime(?),'%Y-%m-%d') as myDate, "
				      					+ "date_format(from_unixtime(?),'%H')*60 + date_format(from_unixtime(?),'%i') as beginMin, "
				      					+ "date_format(from_unixtime(?),'%H')*60 + date_format(from_unixtime(?),'%i') as endMin,"
				      					+ "? as epoch_beg, ? as epoch_end");
		
			preparedStatement.setString(1, epoch_beg);
			preparedStatement.setString(2, epoch_beg);
			preparedStatement.setString(3, epoch_beg);
			preparedStatement.setString(4, epoch_end);
			preparedStatement.setString(5, epoch_end);
			preparedStatement.setString(6, epoch_beg);
			preparedStatement.setString(7, epoch_end);
			resultSet = preparedStatement.executeQuery();
			
			while (resultSet.next()) {
			      // it is possible to get the columns via name
			      // also possible to get the columns via the column number
			      // which starts at 1
			      // e.g., resultSet.getSTring(2);
				  this.shareVariable.setDate(resultSet.getDate("myDate"));
				  this.shareVariable.setBeginMinute(resultSet.getInt("beginMin"));
				  this.shareVariable.setEndMinute(resultSet.getInt("endMin"));
				  this.shareVariable.setEpochBegin(resultSet.getInt("epoch_beg"));
				  this.shareVariable.setEpochEnd(resultSet.getInt("epoch_end"));
			      
			    }
			
			this.resultSet.close();
			this.preparedStatement.close();
				
			
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void closeConnectionElements() {
		// TODO Auto-generated method stub
		
		try {
			this.resultSet.close();
			this.statement.close();
			this.preparedStatement.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public void setChargementId() {
		// TODO Auto-generated method stub
		// Create an instance of SimpleDateFormat used for formatting 
				// the string representation of date (month/day/year)
				DateFormat df = new SimpleDateFormat("yyyy-MM-dd");

				// Get the date today using Calendar object.
				Date today = Calendar.getInstance().getTime();        
				// Using DateFormat format method we can create a string 
				// representation of a date with the defined format.
				String reportDate = df.format(today);

				// Print what date is today!
				System.out.println("Report Date: " + reportDate);
				
				try {
					preparedStatement = connect_eor_dwh.prepareStatement("INSERT INTO d_chargement (chg_source, chg_etl_name, chg_date) values (?,?,?)");
					preparedStatement.setString(2, "ETL_DTM_COMPUTE_HOST_SERVICE_AVAILABILITY");
					preparedStatement.setString(1, this.dwhDatabase);
					preparedStatement.setString(3, reportDate);
					preparedStatement.executeUpdate();
					
					
					preparedStatement = connect_eor_dwh.prepareStatement("SELECT max(chg_id) + 1 as chg_id from d_chargement");
					resultSet = preparedStatement.executeQuery();
					
					
					while(resultSet.next())
					{
						shareVariable.setChargementId(resultSet.getInt("chg_id"));
					}
					
					this.resultSet.close();
					this.preparedStatement.close();
					
				} catch (SQLException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
	}

	public void createValidator(ValidatorList vList) {
		
		//Create validator from bp
		this.createBPValidator(vList);
		
		//Create Validator from bp_service
		this.createHSValidator(vList);
		
	}

	private void createHSValidator(ValidatorList vList) {
		// TODO Auto-generated method stub
		try {
			
			// resultSet gets the result of the SQL query
				this.preparedStatement = connect_eor_dwh
					      .prepareStatement("SELECT * from d_host_service");
				
				resultSet =	preparedStatement.executeQuery();

				int idHost;
				int idService;
				String source;
				int idValidator;
				Validator hostServiceValidator;
				
				while (resultSet.next()) {
	  
					
					idHost = resultSet.getInt("dhs_host");
					idService = resultSet.getInt("dhs_service");
					source = resultSet.getString("dhs_source");
					idValidator = this.shareVariable.getValidatorCounter();
					hostServiceValidator =  new Validator(idHost,idService,source, this.shareVariable, vList, this);
					vList.addValidator(idValidator, hostServiceValidator);
					vList.addHSValidatorKey(idValidator);

				      }
			
				this.resultSet.close();
				this.preparedStatement.close();
					

			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();

			}
	}

	/**
	 * This method create validator from ods.bp table
	 */
	private void createBPValidator(ValidatorList vList) {
		// TODO Auto-generated method stub
		try {
			
			// resultSet gets the result of the SQL query
				this.preparedStatement = connect_eor_dwh
					      .prepareStatement("SELECT *, 	CASE WHEN dap_min is null then -1 else dap_min END as dap_min_calc from d_application");
				
				resultSet =	preparedStatement.executeQuery();

				int idApplication;
				String source;
				String type;
				String category;
				int idValidator;
				Validator appliValidator;
				int min = 0;
				
				while (resultSet.next()) {
	  
					
					idApplication = resultSet.getInt("dap_id");
					source = resultSet.getString("dap_source");
					type = resultSet.getString("dap_type");
					category = resultSet.getString("dap_category");
					min = resultSet.getInt("dap_min_calc");
					idValidator = this.shareVariable.getValidatorCounter();
					appliValidator =  new Validator(idApplication,type,category,source, this.shareVariable, vList, min, this);
					vList.addValidator(idValidator, appliValidator);
					vList.addBPValidatorKey(idValidator);
					

				      }
			
				this.resultSet.close();
				this.preparedStatement.close();
					

			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();

			}
		
	}

	public void closeConnection() {
		// TODO Auto-generated method stub
		try {
			this.preparedStatement.close();
			this.resultSet.close();
			this.connect_eor_dwh.close();
			this.connect_eor_ods.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	/**
		 * This method create links from d_application_link
		 * for links between bp validator and from d_host_service_application
		 * for links between bp validator and hs validator
	 * @param vList 
		 */
		public void createValidatorLinks(ValidatorList vList) {
			// TODO Auto-generated method stub
			
			this.createApplicationLink(vList);
			
			this.createApplicationHSLinks(vList);
		}

		private void createApplicationHSLinks(ValidatorList vList) {
		// TODO Auto-generated method stub
			
			try {
				
				// resultSet gets the result of the SQL query
					this.preparedStatement = connect_eor_dwh
						      .prepareStatement("SELECT * from d_host_service_application");
					
					resultSet =	preparedStatement.executeQuery();

					int idHost;
					int idService;
					int idApplication;
					
					while (resultSet.next()) {
						
						idHost = resultSet.getInt("hsa_host");
						idService = resultSet.getInt("hsa_service");
						idApplication = resultSet.getInt("hsa_appli");
						
						//Look for master application validator
						vList.addSenderListenerHSApp(idHost,idService,idApplication);

					      }
				
					this.resultSet.close();
					this.preparedStatement.close();
						

				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();

				}
	}

		private void createApplicationLink(ValidatorList vList) {
			// TODO Auto-generated method stub
			try {
				
				// resultSet gets the result of the SQL query
					this.preparedStatement = connect_eor_dwh
						      .prepareStatement("SELECT * from d_application_link");
					
					resultSet =	preparedStatement.executeQuery();

					int idMasterApplication;
					int idLinkedApplication;
					
					while (resultSet.next()) {
						
						idMasterApplication = resultSet.getInt("dal_app_master_id");
						idLinkedApplication = resultSet.getInt("dal_app_link_id");
						
						//Look for master application validator
						vList.addSenderListenerApp(idMasterApplication,idLinkedApplication);

					      }
				
					this.resultSet.close();
					this.preparedStatement.close();
						

				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();

				}
		}
		
		public void dropDayLogTable() {
			
			try {
				
				preparedStatement = connect_eor_dwh
					      .prepareStatement("DROP TABLE IF EXISTS eor_dwh.f_tmp_log_day");
			
				preparedStatement.executeUpdate();

				this.preparedStatement.close();
				
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		
		public void dropDayDowntimeLogTable() {
			
			try {
				
				preparedStatement = connect_eor_dwh
					      .prepareStatement("DROP TABLE IF EXISTS eor_dwh.f_tmp_log_downtime_day");
			
				preparedStatement.executeUpdate();
				
				this.preparedStatement.close();
				
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		
		public void dropDayLogHSTable() {

			try {
				
				preparedStatement = connect_eor_dwh
					      .prepareStatement("DROP TABLE IF EXISTS eor_dwh.f_tmp_log_hs_day");
			
				preparedStatement.executeUpdate();
				
				this.preparedStatement.close();
				
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		
	}

	public void dropDayDowntimeLogHSTable() {

		try {
			
			preparedStatement = connect_eor_dwh
				      .prepareStatement("DROP TABLE IF EXISTS eor_dwh.f_tmp_log_down_hs_day");
		
			preparedStatement.executeUpdate();
			
			this.preparedStatement.close();
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public void dropDayLogUnavailability() {
		// TODO Auto-generated method stub
		try {
			
			preparedStatement = connect_eor_dwh.prepareStatement("DROP TABLE IF EXISTS f_tmp_unavailability_day;");
			preparedStatement.executeUpdate();
			
			this.preparedStatement.close();
			
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}
	
	//Create temporary unavailability table for first previous day
	//and previous downtime checking
	public void createDayUnavailabilityTable() {
		// TODO Auto-generated method stub
			
			try {			
				
				preparedStatement = connect_eor_dwh
					      .prepareStatement("CREATE TABLE f_tmp_unavailability_day ENGINE=MEMORY as "
					      		   +"(SELECT a.*, dse_name as fdu_service_name "
					      		   + "FROM f_dtm_hs_unavailability_minute a "
					      		   + "inner join d_service on dse_id = fdu_service " +
									"WHERE FDU_DATE between DATE_ADD(?,INTERVAL -1 DAY) and ? AND " +
									"UNIX_TIMESTAMP(DATE_FORMAT(CONCAT(FDU_DATE,' ',FDU_MINUTE DIV 60,':', MOD(FDU_MINUTE,60) ,':00'),'%Y-%m-%d %H:%i:%s') ) " +
									"between ? - 86400 AND ?)");
			
				preparedStatement.setDate(1, (java.sql.Date) this.shareVariable.getDate());
				preparedStatement.setDate(2, (java.sql.Date) this.shareVariable.getDate());
				preparedStatement.setInt(3,  this.shareVariable.getEpochBegin());
				preparedStatement.setInt(4,  this.shareVariable.getEpochBegin());
				preparedStatement.executeUpdate();
				
				this.preparedStatement.close();
				
				this.createIndexTmpUnavailabilityTable();
				
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		
	}
	
	private void createIndexTmpUnavailabilityTable() {
		// TODO Auto-generated method stub
		try {
			
			preparedStatement = connect_eor_dwh.prepareStatement("CREATE INDEX idx_fdu_date ON f_tmp_unavailability_day (FDU_DATE, FDU_HOST, FDU_SERVICE) USING BTREE;");
			preparedStatement.executeUpdate();
			
			this.preparedStatement.close();
			
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}
	
	/**
	 * In order to query a small amount of log data we create a temporary
	 * log table with only logs linked to targeted day
	 */
	public void createDayLogTable() {
		
		try {
			
			
			preparedStatement = connect_eor_dwh
				      .prepareStatement("CREATE TABLE eor_dwh.f_tmp_log_day ENGINE=MEMORY "
				      				  + "as (SELECT a.*, "
				      				  + "CASE WHEN fln_state_unif = 0 "
				      				  + "THEN -60 +  date_format(concat(fln_date, ' ',fln_time),'%s') "
				    		  		  + "ELSE 60 - date_format(concat(fln_date, ' ',fln_time),'%s') END as FLN_AVAILABILITY, "
				      				  + "date_format(from_unixtime(FLN_UNIX_TIME),'%H')*60 + "
				      				  + "date_format(from_unixtime(FLN_UNIX_TIME),'%i') as FLN_MINUTE, "
				      				  + "date_format(FLN_TIME,'%H') as FLN_HOUR, "
				      				  + "date_format(FLN_TIME,'%i') as FLN_HMINUTE, "
				      				  + "unix_timestamp(date_format(FROM_UNIXTIME(FLN_UNIX_TIME),'%Y-%m-%d %H:%i')) as FLN_DATE_MINUTE, "
				      				  + "dse_name as fln_service_name "
				      				  + "FROM f_dwh_logs_nagios a "
				      				  + "inner join d_service on dse_id = fln_service "
				      				      + "WHERE FLN_DATE between DATE_ADD(?,INTERVAL -1 DAY) and ? and "
				      				      + "FLN_UNIX_TIME between ( ? - 60) and  ? )");
		
			preparedStatement.setDate(1, (java.sql.Date) this.shareVariable.getDate());
			preparedStatement.setDate(2, (java.sql.Date) this.shareVariable.getDate());
			preparedStatement.setInt(3, this.shareVariable.getEpochBegin());
			preparedStatement.setInt(4, this.shareVariable.getEpochEnd());
			preparedStatement.executeUpdate();
			
			this.preparedStatement.close();
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		this.createTmpLogHSTableIdx();
		//this.deleteIndexDWHLogTable();
		
	}
	
	public void createDayDowntimeLogTable() {
		
		//this.createIndexDWHLogDowntimeTable();
		
		try {
			
			
			preparedStatement = connect_eor_dwh
				      .prepareStatement("CREATE TABLE eor_dwh.f_tmp_log_downtime_day ENGINE=MEMORY "
				      				  + "as (SELECT a.*, dse_name as FDO_SERVICE_NAME, "
				      				  + "met_type_label as FDO_MESSAGE_TYPE_LABEL, "
				      				  + "DATE_FORMAT(FDO_DATETIME,'%H') as FDO_HOUR, "
				      				  + "DATE_FORMAT(FDO_DATETIME,'%i') as FDO_HMINUTE, "
				      				  + "UNIX_TIMESTAMP(DATE_FORMAT(FDO_DATETIME,'%Y-%m-%d %H:%i')) as FDO_DATE_MINUTE, "
				      				  + "date_format(FDO_DATETIME,'%s') as FDO_SECONDE "
				      				  + "FROM f_dwh_logs_nagios_downtime a "
				      				  + "inner join d_service on dse_id = FDO_SERVICE_ID "
				      				  + "inner join d_message_type on a.fdo_message_type = d_message_type.met_id "
				      				      + "WHERE a.FDO_DATE between DATE_ADD(?,INTERVAL -1 DAY) and ? and "
				      				      + "a.FDO_UNIX_TIME between ? and ?)");
		
			preparedStatement.setDate(1, (java.sql.Date) this.shareVariable.getDate());
			preparedStatement.setDate(2, (java.sql.Date) this.shareVariable.getDate());
			preparedStatement.setInt(3, this.shareVariable.getEpochBegin());
			preparedStatement.setInt(4, this.shareVariable.getEpochEnd());
			preparedStatement.executeUpdate();
			
			this.preparedStatement.close();
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		this.createTmpLogDownHSTableIdx();
		//this.deleteIndexDWHLogDowntimeTable();
		
	}
	
	private void createTmpLogHSTableIdx() {
		// TODO Auto-generated method stub
		try {
			preparedStatement = connect_eor_dwh.prepareStatement("CREATE INDEX fln_host_index ON f_tmp_log_day (FLN_HOST) USING BTREE;");
			preparedStatement.executeUpdate();
			
			this.preparedStatement.close();
			
			preparedStatement = connect_eor_dwh.prepareStatement("CREATE INDEX fln_service_index ON f_tmp_log_day (FLN_SERVICE) USING BTREE;");
			preparedStatement.executeUpdate();
			
			this.preparedStatement.close();
			
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}
	
private void createTmpLogDownHSTableIdx() {
		
		try {
			preparedStatement = connect_eor_dwh.prepareStatement("CREATE INDEX fdo_host_index ON f_tmp_log_downtime_day (FDO_HOST_ID) USING BTREE;");
			preparedStatement.executeUpdate();
			
			this.preparedStatement.close();
			
			preparedStatement = connect_eor_dwh.prepareStatement("CREATE INDEX fdo_service_index ON f_tmp_log_downtime_day (FDO_SERVICE_ID) USING BTREE;");
			preparedStatement.executeUpdate();
			
			this.preparedStatement.close();
			
			
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}

	public void getHostServiceList(HostServiceList hostServiceList) {
		// TODO Auto-generated method stub
	
		
			try {
				statement = this.connect_eor_dwh.createStatement();
			
			// resultSet gets the result of the SQL query
			resultSet = statement.executeQuery("select * "
											 + "from d_host_service") ;
			
			while (resultSet.next()) {
			      // it is possible to get the columns via name
			      // also possible to get the columns via the column number
			      // which starts at 1
			      // e.g., resultSet.getSTring(2);
				  ArrayList<String> hsAttributes = new ArrayList<String>();
				  hsAttributes.add(resultSet.getString("dhs_source"));
				  hsAttributes.add(resultSet.getString("dhs_host"));
				  hsAttributes.add(resultSet.getString("dhs_service"));
			      
				  hostServiceList.addHostServiceToList(hsAttributes);
				  
			      //warning
			     // System.out.println("Je cr�e une application high level " + name);
			      
			      }
	
			this.resultSet.close();
			this.statement.close();
			
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		
	    
	}

		public boolean getHSPreviousState(int hostId, int serviceId) {
		
		boolean previousState = true;
		int nbRow = 0;
		
		try {
		
		// resultSet gets the result of the SQL query
			preparedStatement = connect_eor_dwh
				      .prepareStatement("select fdu_unavailability from f_tmp_unavailability_day "
											 + "where fdu_date = date_format(from_unixtime(? - 60),'%Y-%m-%d') "
											 + "and fdu_host = ? and (fdu_service = ? or fdu_service_name = 'Hoststatus') "
											 + "and fdu_minute = date_format(from_unixtime(? - 60),'%H')*60 + date_format(from_unixtime(? - 60),'%i') "
											 + "order by fdu_minute desc limit 1");
			
			int epochBegin = this.shareVariable.getEpochBegin();
			preparedStatement.setInt(1, epochBegin);
			preparedStatement.setInt(2, epochBegin);
			preparedStatement.setInt(3, hostId);
			preparedStatement.setInt(4, serviceId);
			preparedStatement.setInt(5, epochBegin);
			resultSet = preparedStatement.executeQuery();
	
	
			while (resultSet.next()) {
				nbRow ++; 
				if(resultSet.getInt("fdu_unavailability") < 0 ) 
				  previousState = false;
				else if (resultSet.getInt("fdu_unavailability") >= 0)
					previousState = true;
	
			      }
	
			if(nbRow == 0)
				previousState = true;
			
			this.resultSet.close();
			this.preparedStatement.close();
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return previousState;
	}				
		
	public boolean getHSPreviousDowntime(int hostId, int serviceId) {
		// TODO Auto-generated method stub
		boolean previousDowntime = false;
		
		try {
		
		// resultSet gets the result of the SQL query
			preparedStatement = connect_eor_dwh
				      .prepareStatement("select fdu_isDowntime from f_tmp_unavailability_day "
											 + "where fdu_host = ? and (fdu_service = ? or fdu_service_name = 'Hoststatus') order by fdu_date, fdu_minute desc limit 1");
			

			preparedStatement.setInt(1, hostId);
			preparedStatement.setInt(2, serviceId);
			resultSet = preparedStatement.executeQuery();


			while (resultSet.next()) {
				previousDowntime = resultSet.getBoolean("fdu_isDowntime");

			      }
			
			this.resultSet.close();
			this.preparedStatement.close();

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return previousDowntime;
		}
		
		return previousDowntime;
	}
	
	
	public boolean testHSOutage(Integer hostId, Integer serviceId, HostService hs) {
		// TODO Auto-generated method stub
		
		boolean isNotEmpty = false;
		
		try {			
			
			preparedStatement = connect_eor_dwh
				      .prepareStatement("SELECT distinct FLN_DATE_MINUTE FROM f_tmp_log_day a "
				      				      + "WHERE FLN_HOST = ? AND (FLN_SERVICE = ? OR FLN_SERVICE_NAME = 'Hoststatus')"
				      				      + "AND FLN_STATE_UNIF = 0 ");
			
			preparedStatement.setInt(1, hostId);
			preparedStatement.setInt(2, serviceId);
			resultSet = preparedStatement.executeQuery();
			
			while(resultSet.next())
			{
				isNotEmpty = true;
			}
			
			this.resultSet.close();
			this.preparedStatement.close();
			
			if(isNotEmpty) {
				
				preparedStatement = connect_eor_dwh
					      .prepareStatement("SELECT distinct FLN_DATE_MINUTE FROM f_tmp_log_day a "
					      				      + "WHERE FLN_HOST = ? AND (FLN_SERVICE = ? OR FLN_SERVICE_NAME = 'Hoststatus') ");
				
				preparedStatement.setInt(1, hostId);
				preparedStatement.setInt(2, serviceId);
				resultSet = preparedStatement.executeQuery();
				
				while(resultSet.next())
				{
					hs.addDateMinuteState(resultSet.getInt("FLN_DATE_MINUTE"));	
				}
				
				this.resultSet.close();
				this.preparedStatement.close();
				
			}
			
			
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return true;
		}
		
		return isNotEmpty;
	}
	
	public boolean testHSDowntime(Integer hostId, Integer serviceId, HostService hs) {
		// TODO Auto-generated method stub
		boolean isNotEmpty = false;
		
		try {			
			
			preparedStatement = connect_eor_dwh
				      .prepareStatement("SELECT * FROM f_tmp_log_downtime_day a "
				      				      + "WHERE FDO_HOST_ID = ? AND (FDO_SERVICE_ID = ? OR FDO_SERVICE_NAME = 'Hoststatus')");
			
			preparedStatement.setInt(1, hostId);
			preparedStatement.setInt(2, serviceId);
			resultSet = preparedStatement.executeQuery();
			
			while(resultSet.next())
			{
				isNotEmpty = true;
				hs.addDateMinuteState(resultSet.getInt("FDO_DATE_MINUTE"));
				
			}
			
			this.resultSet.close();
			this.preparedStatement.close();
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return true;
		}
		
		return isNotEmpty;
	}

	public void createTmpLogHSTable(Integer hostId, Integer serviceId) {
		// TODO Auto-generated method stub
		
		try {			
			preparedStatement = connect_eor_dwh
				      .prepareStatement("CREATE TABLE eor_dwh.f_tmp_log_hs_day engine=MEMORY "
				      				  + "as (SELECT * FROM f_tmp_log_day a "
				      				      + "WHERE FLN_HOST = ? AND (FLN_SERVICE = ? OR FLN_SERVICE_NAME = 'Hoststatus'))");
		
			preparedStatement.setInt(1, hostId);
			preparedStatement.setInt(2, serviceId);
			preparedStatement.executeUpdate();
			
			this.preparedStatement.close();
			
			preparedStatement = connect_eor_dwh
				      .prepareStatement("CREATE INDEX idx_tmp_fln_date on eor_dwh.f_tmp_log_hs_day (fln_date,fln_hour,fln_hminute) using btree");
			preparedStatement.executeUpdate();
			
			this.preparedStatement.close();
			
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public void createTmpLogDowntimeHSTable(Integer hostId, Integer serviceId) {
		// TODO Auto-generated method stub
		
		try {			
			preparedStatement = connect_eor_dwh
				      .prepareStatement("CREATE TABLE eor_dwh.f_tmp_log_down_hs_day engine=MEMORY "
				      				  + "as (SELECT * FROM f_tmp_log_downtime_day a "
				      				      + "WHERE FDO_HOST_ID = ? AND (FDO_SERVICE_ID = ? OR FDO_SERVICE_NAME = 'Hoststatus'))");
		
			preparedStatement.setInt(1, hostId);
			preparedStatement.setInt(2, serviceId);
			preparedStatement.executeUpdate();
			
			this.preparedStatement.close();
			
			preparedStatement = connect_eor_dwh
				      .prepareStatement("CREATE INDEX idx_tmp_fln_date on f_tmp_log_down_hs_day (fdo_date,fdo_hour, fdo_hminute) using btree");
			preparedStatement.executeUpdate();
			
			this.preparedStatement.close();
			
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
	public ResultSet getHostServiceLogMinute(int minutes) {
		// TODO Auto-generated method stub
		
		this.closeConnectionElements();
		
		try {
		
		// resultSet gets the result of the SQL query
			preparedStatement = connect_eor_dwh
				      .prepareStatement("select distinct fln_availability, fln_state_unif as state, "
						    		  		 + "FLN_UNIX_TIME - FLN_DATE_MINUTE as second_event "
				      						 + "from f_tmp_log_hs_day "
											 + "where fln_date_minute = ? "
											 + "order by fln_date, fln_time");
			
			preparedStatement.setInt(1, minutes);
			resultSet =	preparedStatement.executeQuery();
			
			return resultSet;

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		
	}
	
	public void closeResultSet() {
		
		try {
			this.preparedStatement.close();
			this.resultSet.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		
	}
	
	public ResultSet getDowntime(int minute) {

		String currentDowntime = "";
		int downtimeFlag = -1;

		this.closeConnectionElements();
		
		try {
		
		// resultSet gets the result of the SQL query
			preparedStatement = connect_eor_dwh
				      .prepareStatement("SELECT distinct fdo_message_type_label, fdo_seconde "
				      				  + "FROM f_tmp_log_down_hs_day "
				      				  + "where fdo_date = date_format(from_unixtime(?),'%Y-%m-%d') and "
				      				  + "fdo_date_minute = ? "
				      				  + "order by fdo_datetime desc limit 1");
			
			preparedStatement.setInt(1, minute);
			preparedStatement.setInt(2, minute);
			resultSet =	preparedStatement.executeQuery();
				

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return resultSet;
		}
		
		return resultSet;
	}


	public void insertHSMinute(int minute, int hostId, int serviceId, String source, int unavailability,
			int unavailabilityDown, int downtimeDuration, int effectiveDowntime, boolean isDowntime) {
		// TODO Auto-generated method stub
		int chg_id = shareVariable.getChargementId();
		
		try {
			preparedStatement = connect_eor_dwh.prepareStatement("insert into f_dtm_hs_unavailability_minute " +
					"(fdu_date, fdu_minute, fdu_source, fdu_host, fdu_service, fdu_unavailability, fdu_unavailabilityDown, fdu_downtimeDuration, fdu_downtimeEffectiveDuration, fdu_isDowntime, fdu_chg_id) " + 
					" values (date_format(from_unixtime(?),'%Y-%m-%d'),date_format(from_unixtime(?),'%H')*60 + date_format(from_unixtime(?),'%i'),?,?,?,?,?,?,?,?,?)");
			preparedStatement.setInt(1, minute);
			preparedStatement.setInt(2, minute);
			preparedStatement.setInt(3, minute);
			preparedStatement.setString(4, source);
			preparedStatement.setInt(5, hostId);
			preparedStatement.setInt(6, serviceId);
			preparedStatement.setInt(7, unavailability);
			preparedStatement.setInt(8, unavailabilityDown);
			preparedStatement.setInt(9, downtimeDuration);
			preparedStatement.setInt(10, effectiveDowntime);
			preparedStatement.setBoolean(11, isDowntime);
			preparedStatement.setInt(12, chg_id);
			preparedStatement.executeUpdate();
		
			this.resultSet.close();
			this.preparedStatement.close();
		
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public void insertAppliMinute(int minute, int applicationId, String category, String categoryAnalysis, String source,
			int unavailability, int unavailabilityDown, int effectiveDowntime) {
		// TODO Auto-generated method stub
		int chg_id = shareVariable.getChargementId();
		
		try {
			preparedStatement = connect_eor_dwh.prepareStatement("insert into f_dtm_application_unavailability_minute " +
					"(fdb_date, fdb_minute, fdb_source, fdb_application, fdb_category, fdb_category_analysis,fdb_unavailability, fdb_unavailability_down, fdb_downtimeEffectiveDuration, fdb_chg_id) " + 
					" values (date_format(from_unixtime(?),'%Y-%m-%d'),date_format(from_unixtime(?),'%H')*60 + date_format(from_unixtime(?),'%i'),?,?,?,?,?,?,?,?)");
			preparedStatement.setInt(1, minute);
			preparedStatement.setInt(2, minute);
			preparedStatement.setInt(3, minute);
			preparedStatement.setString(4, source);
			preparedStatement.setInt(5, applicationId);
			preparedStatement.setLong(6, this.getCategoryAnalysisId(category));
			preparedStatement.setLong(7, this.getCategoryAnalysisId(categoryAnalysis));
			preparedStatement.setInt(8, unavailability);
			preparedStatement.setInt(9, unavailabilityDown);
			preparedStatement.setInt(10, effectiveDowntime);
			preparedStatement.setInt(11, chg_id);
			preparedStatement.executeUpdate();
		
			this.resultSet.close();
			this.preparedStatement.close();
		
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	private long getCategoryAnalysisId(String categoryAnalysis) {
		// TODO Auto-generated method stub
		int category_id = -1;
	    ResultSet resultSet = null;
	    PreparedStatement preparedStatement = null; 
		// resultSet gets the result of the SQL query
	    
		
	    try {
	    	preparedStatement = connect_eor_dwh.prepareStatement("Select cat_id from d_category where cat_label = ?");
	    	preparedStatement.setString(1, categoryAnalysis);
		    resultSet = preparedStatement.executeQuery();
			
			while (resultSet.next()) {	
			      
				category_id = resultSet.getInt("cat_id");
  
			    }
			if(category_id == -1)
			{
				throw new Exception();
			}
			return category_id;
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("Category " + categoryAnalysis + "has not been find in table d_category_analysis");
			return -1;
		}
		finally{
			try {
				if(!(resultSet == null))
					resultSet.close();
				if(!(preparedStatement == null))
				preparedStatement.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	
}
