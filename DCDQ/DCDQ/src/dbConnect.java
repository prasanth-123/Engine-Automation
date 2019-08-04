
/************************************************************************************************
* Written by					:  	sai.ram.tatiraju@oracle.com
* Class Name					:  	dbConnect
* Class Description			    : 	This class will connect to the Database.
* Date Created					:  	20-April-2016
* Date Modified					: 	21-June-2016
* Modified by					: 	sai.ram.tatiraju@oracle.com
*************************************************************************************************/

import java.sql.*;

public class dbConnect {

	public static Connection getConnect(String USER, String PASS) {
		Connection conn = null;
		try {

			String DB_URL = "jdbc:oracle:thin:@10.184.158.254:1521:SILICA12C";
			// (DESCRIPTION =(ADDRESS_LIST =(ADDRESS = (PROTOCOL = TCP)(HOST =
			// 10.184.155.59)(PORT = 1521)))(CONNECT_DATA =(SERVICE_NAME =
			// GLDCPY80)))
			// (DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=10.184.152.242)(port=1521))(ADDRESS=(PROTOCOL=TCP)(HOST=10.184.152.243)(PORT=1521))(LOAD_BALANCE=yes)(FAILOVER=yes))(CONNECT_DATA=(SERVICE_NAME=ORA12CQA)))
			Class.forName("oracle.jdbc.driver.OracleDriver");

			System.out.println("Connecting to database...");

			conn = DriverManager.getConnection(DB_URL, USER, PASS);

			System.out.println("Connected");
		} catch (SQLException se) {
			se.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return conn;

	}

}