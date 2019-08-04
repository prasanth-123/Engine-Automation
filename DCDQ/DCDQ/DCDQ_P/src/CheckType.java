

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class CheckType {
	
	
	
	
	
	
	public static String Fn_Check_Type(String DQ_id){
		
	Connection conn =null;

	String sql_1="select  F_RANGE_CHECK, F_DATA_LENGTH , F_LOV_CHECK , F_ISNULL_CHECK, V_BLANK_CHECK, F_LOOKUP_CHECK, "
               + " F_BUSS_CHECK, V_COL_REF_CHECK, F_DUP_CHECK from dq_check_master_rep t where V_DQ_CHECK_ID ='"+ DQ_id+ "'" ;
	
	String sql_2 = "select V_DQ_SRC_COL from dq_check_master_rep t where V_DQ_CHECK_ID ='"+ DQ_id+ "'" ;
	
	int i = 0;
	
	String  checks[] = {"RANGE" , "DATALENGTH" , "LOV" , "ISNULL" , "BLANK" , "LOOK_UP", "BUSS",  "COL_REF", "DUPLICATE" };
	String res = "";
	
	
	
	try {
		
		 conn = dbConnect.getConnect( "ofs802tmqacap" , "password123");
		 
		 Statement st1 = conn.createStatement();
		 Statement st2 = conn.createStatement();
		 
		 ResultSet rs_2 = st2.executeQuery(sql_2);
		 ResultSet rs_1 = st1.executeQuery(sql_1);
		 
		 
		rs_2.next();
		 
		
		if(rs_2.getString(1).equalsIgnoreCase("-1"))
		{
			res = null;
	    }
		 	 
	 
		 else
		 {
			 
			 
			 if(rs_1.next())
		 		 
			 for(i=1; i<10;i++ ){
				
				 if(rs_1.getString(i).equalsIgnoreCase("Y"))
				 {res = checks[i-1];
				 break;
				 }
			 }
		 	 
		 
		 } 
	}
	
	
	catch (Exception e) {
		
		e.printStackTrace();
	}
	
	
	 return res; 
	
	
	
	}
	
	
	
	
	
	
	
	
	
	
	
	
	public static String[] Fn_DQ_Names()
	{
		
		String[] arr= new String[100];;
		Connection conn =null;
		int i = 0;
		String sql= "select v_dq_check_id  from dq_check_master_rep t";

		
		try {
			
			 conn = dbConnect.getConnect( "ofs802tmqacap" , "password123");
			 
			 Statement st = conn.createStatement();
			 
			 ResultSet rs = st.executeQuery(sql);
			 
			
			 while (rs.next()) {
				 
				 i++;
				 arr[i-1] = rs.getString("v_dq_check_id");
			 }
		
		}
		catch (Exception e) {
			
			e.printStackTrace();
		}

		
		
		
		return arr;
		
	}
	
	
	
	
	
	
	
	
	
	
	public static String Fn_Group_Creation( String arr)
	{
		
		String init_time = new SimpleDateFormat("dd/MM/YYYY kk:mm:ss").format(Calendar.getInstance().getTime());
		
		String sql ="insert into DQ_GROUP_MAPPING (V_DQ_GROUP_ID, V_DQ_GROUP_DESC, V_DQ_CHECK_ID, OWNER,"
				+ " D_SAVED_DATE, INFODOM, D_MODIFIED_DATE, V_MODIFIED_USER, V_FOLDER_NAME, N_SEQUENCE,"
				+ " F_RULE_EXECUTED, F_ON_SOURCE)"
				+ "values('"
				+ arr +"_grp', '"
				+ arr +"_grp', '"
				+ arr +"', "
			    + "'TESTUSER',"
			    + "to_date('" + init_time + "', 'dd-mm-yyyy hh24:mi:ss'),"
				+"'OFSCAPADQINFO',"
				+ "to_date('" + init_time + "', 'dd-mm-yyyy hh24:mi:ss'),"
				+"'TESTUSER',"
				+"'INDIA',"
				+"1,"
				+"'N',"
				+"'N')";
		
		Connection conn =null;
	
		String grp_name = arr +"_grp";
		
		try {
			
			 conn = dbConnect.getConnect( "ofs802tmqacap" , "password123");
			 
			 Statement st = conn.createStatement();
			 
			 st.executeQuery(sql);
	
		
		}
		catch (Exception e) {
			
			e.printStackTrace();
		}
		
		return grp_name;
		
	}
	
	
	
	
	
	public static String Fn_Batch_Creation( String arr)
	{
		
		String init_time = new SimpleDateFormat("dd/MM/YYYY kk:mm:ss").format(Calendar.getInstance().getTime());
		
		String batch_name = arr +"_batch";

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
		sql_2 = sql_2.concat("'OFSCAPADQINFO',");
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
		sql_3_5 = sql_3_5.concat(" values ('" + batch_name + "', 'TASK1', 5 , 'DQ Group Name', '" + arr + "')" );

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
			
			 conn = dbConnect.getConnect( "ofsa802confqt" , "password123");
			 
			 Statement st = conn.createStatement();
			 
			 st.executeQuery(sql_1);
			 st.executeQuery(sql_2);
			 st.executeQuery(sql_4);
			 st.executeQuery(sql_3_1);	st.executeQuery(sql_3_2);	st.executeQuery(sql_3_3);   st.executeQuery(sql_3_4);		 
			 st.executeQuery(sql_3_5);	st.executeQuery(sql_3_6);	st.executeQuery(sql_3_7);	st.executeQuery(sql_3_8);	
			 st.executeQuery(sql_5);
		
		
		}
		catch (Exception e) {
			
			e.printStackTrace();
		}
		
		
		
		return batch_name;
		
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	

	public static void main(String[] args) {
	
	
		String id = "DQFSDWNU0473";
		
		System.out.println("output is " + Fn_Check_Type(id));
	}

}
