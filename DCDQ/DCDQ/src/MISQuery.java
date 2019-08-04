import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;


public class MISQuery {
	
	// This function will change update the MIS date in case of a multiple check
		public static void mis_query() {
			
			Variables.MIS_date = "01-01-2013";
			String sql = "update  stg_non_sec_exposures t set t.fic_mis_date = to_date('01-01-2013', 'dd-MM-yyyy') where t.fic_mis_date = to_date('02-01-2013', 'dd-MM-yyyy')";
			try {
				
				Connection conn = null;
				conn = dbConnect.getConnect( Variables.atomUser , Variables.AtomPassword);
				Statement st = conn.createStatement();
				st.executeQuery(sql);
				
			 } 
				 catch (SQLException e) {
				e.printStackTrace();
			}
			
		}

}
