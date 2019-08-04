

 

import java.sql.* ; 
import java.math.* ;

 

public class dbConnect {
	
	static final String USER = "ofs802tmqacap";

    static final String PASS = "password123";
          
          public static Connection getConnect( String USER, String PASS)
          {
        	  Connection conn = null;
        	  try{
        		  
        		  String DB_URL = "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=10.184.152.242)(port=1521))(ADDRESS=(PROTOCOL=TCP)(HOST=10.184.152.243)(PORT=1521))(LOAD_BALANCE=yes)(FAILOVER=yes))(CONNECT_DATA=(SERVICE_NAME=ORA12CQA)))";
                  //STEP 2: Register JDBC driver

                  Class.forName("oracle.jdbc.driver.OracleDriver");

      

                  //STEP 3: Open a connection

                  System.out.println("Connecting to database...");

                  conn = DriverManager.getConnection(DB_URL, USER, PASS);
                  System.out.println("Connected");
        	  }
        	  catch(SQLException se){

                  //Handle errors for JDBC

                  se.printStackTrace();
        	  } catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return conn;
        	  
          }

      

      

}