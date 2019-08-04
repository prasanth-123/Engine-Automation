/************************************************************************************************
* Written by					:  	sai.ram.tatiraju@oracle.com
* Class Name					:  	Check Type
* Class Description			    : 	This class will Return the check type of a particular rule.
* 									Based on the dq_check_id input it will return the check type.
* Date Created					:  	17-April-2016
* Date Modified					: 	21-June-2016
* Modified by					: 	sai.ram.tatiraju@oracle.com
*************************************************************************************************/

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public class CheckType {
	

	public static String Fn_Check_Type(String DQ_id,String dbuser,String dbpassword){
		
	System.out.println("**************Fn_Check_Type start**************");
		
	Connection conn =null;

	String sql_1="select  F_RANGE_CHECK, F_DATA_LENGTH , F_LOV_CHECK , F_ISNULL_CHECK, V_BLANK_CHECK, F_LOOKUP_CHECK, "
               + " F_BUSS_CHECK, V_COL_REF_CHECK, F_DUP_CHECK from dq_check_master t where V_DQ_CHECK_ID ='"+ DQ_id+ "'" ;
	
	String sql_2 = "select V_DQ_SRC_COL from dq_check_master t where V_DQ_CHECK_ID ='"+ DQ_id+ "'" ;
	
	String sql_3 = "select N_CHECK_TYPE from dq_check_master t where V_DQ_CHECK_ID ='"+ DQ_id+ "'" ; 
	
	
	String  checks[] = {"RANGE" , "DATALENGTH" , "LOV" , "ISNULL" , "BLANK" , "LOOK_UP", "BUSS",  "COL_REF", "DUPLICATE" , "GEN"};
	
	String res = "";
	
	try {
		
		 conn = dbConnect.getConnect( dbuser , dbpassword);
		 
		 Statement st1 = conn.createStatement();
		 Statement st2 = conn.createStatement();
		 Statement st3 = conn.createStatement();
		 

		 ResultSet rs_1 = st1.executeQuery(sql_1);
		 ResultSet rs_2 = st2.executeQuery(sql_2);
		 ResultSet rs_3 = st3.executeQuery(sql_3);
		 
		rs_2.next();
		rs_3.next(); 
		
		if(rs_2.getString(1).equalsIgnoreCase("-1")&&rs_3.getString(1).equalsIgnoreCase("1"))
		{
			res = "GEN";
	    }
		else
		{
			int count=0;
			 if(rs_1.next())
		 	 for(int i=1; i<10;i++ )
		 	 {
				
				 if(rs_1.getString(i).equalsIgnoreCase("Y"))
				 {
					 count=count+1;
					 
					 
					 if(count==1)
					 {
					 res = res + checks[i-1];
					 
					 }
					 
					 if(count>1)
					 {
						 res = res +";"+ checks[i-1]; 
						 System.out.println("result------->"+res);
						 Variables.noofcheck="multiple";
						 System.out.println("Nubmer of checks is: "+Variables.noofcheck); 
					 }
				 
				 }
			 }
		 } 
	
	conn.close();
	System.out.println("Connection closed");
	}
	catch (Exception e) {
		
		e.printStackTrace();
	}
	
	System.out.println("**************Fn_Check_Type end**************");
	System.out.println("**************************** Returning "+res);
	
	return res; 
	
	}

	public static void main(String[] args) {
	
	
		String check=Fn_Check_Type("DQ_2_Multiple","ayf_ofsaaatm","password123");
		
		
		System.out.println("output is " + check);
	}

}
