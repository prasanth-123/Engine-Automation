import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;


public class UpdateTable {

	
	
	public static void FnLoadTestData(String dbuser, String dbpassword) 
	
	
	{	
		
		String file="";
		StringBuffer queries=new StringBuffer();
		FileReader dcdq;
		
		try 
		{
			//C:\Users\rmamilla\Desktop\EngineDCDQ
			dcdq = new FileReader(new File("C:\\Users\\prasanth\\Desktop\\load.sql"));
			BufferedReader br=new BufferedReader(dcdq);
			while((file =br.readLine()) !=null)
			{
				queries.append(file);
			}
			br.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		String[] q = queries.toString().split(";");
		for(int i =0; i<q.length; i++)
			{
				Connection conn = dbConnect.getConnect(dbuser, dbpassword);
				Statement st;
				try {
					st = conn.createStatement();
					;st.executeUpdate(q[i]);
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}



	}
	
	
	
	public static void FnUpdateData(String dbuser, String dbpassword) 
	{
		
		
		
		
		
		
		
		
		
	}
	
	
	
	
	
	public static void main(String[] args) {
		
		
		FnLoadTestData("ofs802tmqacap", "password123");
		
	}
	
	
	
	
}
