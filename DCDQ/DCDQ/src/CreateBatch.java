/************************************************************************************************
* Written by					:  	sai.ram.tatiraju@oracle.com
* Class Name					:  	Create Batch
* Class Description			    : 	This class will create a batch with the group assigned to it.
* Date Created					:  	20-May-2016
* Date Modified					: 	22-June-2016
* Modified by					: 	sai.ram.tatiraju@oracle.com
*************************************************************************************************/
import java.sql.Connection;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Calendar;


public class CreateBatch {


	public static String Fn_Batch_Creation( String arr, String folder, String infodom, String dbuser, String dbpassword)
	{
		System.out.println("**************Fn_Batch_Creation start**************");
		arr=arr.replace("grp", "");
		String init_time = new SimpleDateFormat("dd/MM/YYYY kk:mm:ss").format(Calendar.getInstance().getTime());
		
		String batch_name = "OFSCAPADQINFO_" + arr +"batch";
		String grp = arr + "grp";

		//************batch_execution_mapping Table***************//
		
		
		String sql_1 = "insert into batch_execution_mapping (V_GROUP_CODE, V_BATCH_ID, V_SRC_FRAMEWORK)";
		sql_1 = sql_1.concat("  values ('TESTGRP','"+batch_name+ "'  , 'ICC') ");
		
		//************batch_master Table***************//
		
		
		String sql_2 = "insert into batch_master(V_BATCH_ID,V_BATCH_DESCRIPTION,V_CREATED_BY,V_CREATED_DATE,";
		sql_2 = sql_2.concat("V_LAST_MODIFIED_BY,V_LAST_MODIFIED_DATE,V_BATCH_TYPE,V_SRC_FRAMEWORK,V_DSN_NAME,V_IS_SEQUENTIAL)");
		sql_2 = sql_2.concat("values('"+batch_name+"','"+batch_name+"','TESTUSER',");
		sql_2 = sql_2.concat("to_date('" + init_time + "', 'dd-mm-yyyy hh24:mi:ss'),");
		sql_2 = sql_2.concat("'TESTUSER',");
		sql_2 = sql_2.concat("to_date('" + init_time + "', 'dd-mm-yyyy hh24:mi:ss'),");
		sql_2 = sql_2.concat("'NE',");
		sql_2 = sql_2.concat("'DCDQ',");
		sql_2 = sql_2.concat("'"+infodom+"',");
		sql_2 = sql_2.concat("'Y')");
		
		
		//************batch_parameter_master Table***************//
		
		
		String sql_3_1 = "insert into batch_parameter_master (V_BATCH_ID, V_TASK_ID, I_PARAMETER_ORDER, V_PARAMETER_NAME, V_PARAMETER_VALUE)";
		sql_3_1 = sql_3_1.concat(" values ('" + batch_name + "', 'TASK1', 1, 'Datastore Type', 'EDW')" );
		
		
		String sql_3_2 = "insert into batch_parameter_master (V_BATCH_ID, V_TASK_ID, I_PARAMETER_ORDER, V_PARAMETER_NAME, V_PARAMETER_VALUE)";
		sql_3_2 = sql_3_2.concat(" values ('" + batch_name + "', 'TASK1', 2, 'Datastore Name', 'OFSCAPADQINFO')" );

	
		String sql_3_3 = "insert into batch_parameter_master (V_BATCH_ID, V_TASK_ID, I_PARAMETER_ORDER, V_PARAMETER_NAME, V_PARAMETER_VALUE)";
		sql_3_3 = sql_3_3.concat(" values ('" + batch_name + "', 'TASK1', 3 , 'IP Address', '10.184.154.20')" );

		String sql_3_4 = "insert into batch_parameter_master (V_BATCH_ID, V_TASK_ID, I_PARAMETER_ORDER, V_PARAMETER_NAME, V_PARAMETER_VALUE)";
		sql_3_4 = sql_3_4.concat(" values ('" + batch_name + "', 'TASK1', 4 , 'User Id', 'TESTUSER')" );
		
		String sql_3_5 = "insert into batch_parameter_master (V_BATCH_ID, V_TASK_ID, I_PARAMETER_ORDER, V_PARAMETER_NAME, V_PARAMETER_VALUE)";
		sql_3_5 = sql_3_5.concat(" values ('" + batch_name + "', 'TASK1', 5 , 'DQ Group Name', '" + grp + "')" );

		String sql_3_6 = "insert into batch_parameter_master (V_BATCH_ID, V_TASK_ID, I_PARAMETER_ORDER, V_PARAMETER_NAME, V_PARAMETER_VALUE)";
		sql_3_6 = sql_3_6.concat(" values ('" + batch_name + "', 'TASK1', 6 , 'Rejection Threshold', 'NULL')" );

		String sql_3_7 = "insert into batch_parameter_master (V_BATCH_ID, V_TASK_ID, I_PARAMETER_ORDER, V_PARAMETER_NAME, V_PARAMETER_VALUE)";
		sql_3_7 = sql_3_7.concat(" values ('" + batch_name + "', 'TASK1', 7 , 'Additional Parameters', 'NULL')" );
		
		
		String sql_3_8 = "insert into batch_parameter_master (V_BATCH_ID, V_TASK_ID, I_PARAMETER_ORDER, V_PARAMETER_NAME, V_PARAMETER_VALUE)";
		sql_3_8 = sql_3_8.concat(" values ('" + batch_name + "', 'TASK1', 8 , 'Fail if Threshold Breaches', 'N')" );
		

		
		//************batch_task_master Table***************//
		
		String sql_4 = "insert into batch_task_master (V_BATCH_ID, V_TASK_ID, V_COMPONENT_ID, V_TASK_STATUS, V_TASK_DESCRIPTION, V_CREATED_BY, V_LAST_MODIFIED_BY, V_CREATED_DATE, V_LAST_MODIFIED_DATE)";
		
		sql_4 =  sql_4.concat(" values ('" + batch_name + "', 'TASK1', 'RUN DQ RULE', 'N', 'TASK1', 'TESTUSER', ");

		sql_4 =  sql_4.concat("'TESTUSER', to_date('" + init_time + "', 'dd-mm-yyyy hh24:mi:ss'), to_date('" + init_time + "', 'dd-mm-yyyy hh24:mi:ss'))");
		
		
		
		//************batch_task_precedence_master Table***************//
		
		String sql_5 = "insert into batch_task_precedence_master (V_BATCH_ID, V_TASK_ID, V_PARENT_TASK_ID, V_PARENT_TASK_STATUS)";
		       sql_5 = sql_5.concat("values ('" + batch_name + "', 'TASK1', 'START', 'S')");  
		
		Connection conn =null;
		
		
		try {
			
			 conn = dbConnect.getConnect( dbuser , dbpassword);
			 
			 Statement st = conn.createStatement();
			 
			 st.executeUpdate(sql_1);
			 st.executeUpdate(sql_2);
			 st.executeUpdate(sql_4);
			 st.executeUpdate(sql_3_1);	st.executeUpdate(sql_3_2);	st.executeUpdate(sql_3_3);   st.executeUpdate(sql_3_4);		 
			 st.executeUpdate(sql_3_5);	st.executeUpdate(sql_3_6);	st.executeUpdate(sql_3_7);	st.executeUpdate(sql_3_8);	
			 st.executeUpdate(sql_5);
		
		
		}
		catch (Exception e) {
			
			e.printStackTrace();
		}
		
		
		System.out.println("**************Fn_Batch_Creation completed succesfully**************");
		System.out.println("**************************** Returning "+batch_name);
		return arr + "batch";
		
	}
	
	
	public static void main(String[] args) {
		
		Fn_Batch_Creation( "gencheck_2_grp",  "BIS1", "OFSCAPADQINFO ", "ofsaaconf5","password123");
		
	
	}
	
}
