/************************************************************************************************
* Written by					:  	sai.ram.tatiraju@oracle.com
* Class Name					:  	Primary Key
* Class Description			    : 	This class will Return the primary Key of a particular table.
* Date Created					:  	27-April-2016
* Date Modified					: 	21-June-2016
* Modified by					: 	sai.ram.tatiraju@oracle.com
*************************************************************************************************/

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;


public class PrimaryKey {
	public static void main(String[] args) {
	
	Variables.table="stg_casa";
	Variables.atomUser="ofsatmcap5";
	Variables.AtomPassword="password123";
	getPrimaryKey();
	}
	

public static void getPrimaryKey() {
	Connection conn =null;
	String sql="select cols.column_name from all_constraints cons, all_cons_columns cols where cols.table_name = '"+Variables.table.toUpperCase()+"' "
			+ "AND cons.constraint_type = 'P' AND cons.constraint_name = cols.constraint_name AND cons.owner = cols.owner AND cons.OWNER='"+Variables.atomUser.toUpperCase()+"'";
	try {
		 conn = dbConnect.getConnect(Variables.atomUser,Variables.AtomPassword);
		 System.out.println("Connection established");
		 Statement st = conn.createStatement();
		 System.out.println("Create Statement");
		 ResultSet rs = st.executeQuery(sql);
		 System.out.println("After Result Set");
		 while(rs.next())
		 {
			 Variables.primaryKey=rs.getString(1);
		 }
		 
		 System.out.println("***" + Variables.primaryKey);
		 
	} catch (Exception e) {
		e.printStackTrace();
	}
	
	
	}
	
}


