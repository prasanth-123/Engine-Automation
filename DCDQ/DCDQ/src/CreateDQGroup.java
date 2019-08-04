/************************************************************************************************
* Written by					:  	sai.ram.tatiraju@oracle.com
* Class Name					:  	Create DQGroup
* Class Description			    : 	This class will create the group with the DQ rule assigned to it.
* Date Created					:  	17-April-2016
* Date Modified					: 	21-June-2016
* Modified by					: 	sai.ram.tatiraju@oracle.com
*************************************************************************************************/
import java.sql.Connection;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Calendar;


public class CreateDQGroup {


	public static String Fn_Group_Creation( String DQName,String folder,String infodom, String dbuser, String dbpassword)
	{
		System.out.println("**************Fn_Group_Creation starting**************");
		String init_time = new SimpleDateFormat("dd/MM/YYYY kk:mm:ss").format(Calendar.getInstance().getTime());
		
		String sql ="insert into DQ_GROUP_MAPPING (V_DQ_GROUP_ID, V_DQ_GROUP_DESC, V_DQ_CHECK_ID, OWNER,"
				+ " D_SAVED_DATE, INFODOM, D_MODIFIED_DATE, V_MODIFIED_USER, V_FOLDER_NAME, N_SEQUENCE,"
				+ " F_RULE_EXECUTED, F_ON_SOURCE)"
				+ "values('"
				+ DQName +"_grp', '"
				+ DQName +"_grp', '"
				+ DQName +"', "
			    + "'TESTUSER',"
			    + "to_date('" + init_time + "', 'dd-mm-yyyy hh24:mi:ss'),"
				+"'"+infodom+"',"
				+ "to_date('" + init_time + "', 'dd-mm-yyyy hh24:mi:ss'),"
				+"'TESTUSER',"
				+"'"+folder+"',"
				+"1,"
				+"'N',"
				+"'N')";
		
		Connection conn =null;
	
		String grp_name = DQName +"_grp";
		
		try {
			
			 conn = dbConnect.getConnect( dbuser , dbpassword);
			 
			 Statement st = conn.createStatement();
			 
			 st.executeUpdate(sql);
	
		
		}
		catch (Exception e) {
			
			e.printStackTrace();
		}
	
		System.out.println("**************Fn_Group_Creation completed succesfully**************");
		return grp_name;
		
	}

	public static void main(String[] args) {
		Fn_Group_Creation("gencheck_2", "BIS1", "OFSCAPADQINFO", "ofsatmcap5", "password123");

	}

}
