/************************************************************************************************
* Written by					:  	sai.ram.tatiraju@oracle.com
* Class Name					:  	ListDQNames
* Class Description			    : 	This class will Return an array of all the DQ rules in the Table.
* Date Created					:  	17-April-2016
* Date Modified					: 	21-June-2016
* Modified by					: 	sai.ram.tatiraju@oracle.com
*************************************************************************************************/

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;


public class ListDQNames {

	
	public static String[] Fn_DQ_Names(String dbuser, String dbpassword)
	{
		System.out.println("**************Fn_DQ_Names start**************");
		
		ArrayList <String> list = new ArrayList<String>();
		Connection conn =null;
		String sql= "select v_dq_check_id  from dq_check_master t";

		
		try {
			
			System.out.println("sql :"+ sql);
			 conn = dbConnect.getConnect( dbuser , dbpassword);
			 
			 Statement st = conn.createStatement();
			 
			 ResultSet rs = st.executeQuery(sql);
			
			 while (rs.next()) {
				 list.add(rs.getString("v_dq_check_id"));
			 }
		
		}
		catch (Exception e) {
			
			e.printStackTrace();
		}
		String[] arr= new String[list.size()];
		for(int k=0;k<arr.length;k++)
		{
			arr[k]=list.get(k);
		}
		System.out.println("**************Fn_DQ_Names executed succesfully**************");
		return arr;
	}
	
	public static void main(String[] args) {
		
		

	}
	
}
