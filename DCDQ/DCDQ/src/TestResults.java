import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;


public class TestResults {
	// This function will show the test results (warning & information), whether the case is passed or failed
		public static void test_results(Object[] arr) throws ParseException {
			
			Connection conn = dbConnect.getConnect(Variables.atomUser, Variables.AtomPassword);
			
			String query = "select " + Variables.column + " from " + Variables.table + " where " + Variables.MIScolumn + " = to_date('" + Variables.MIS_date + "', 'dd-MM-yyyy') order by " + Variables.primaryKey;
			System.out.println(query);
			ArrayList <Object> list1= new ArrayList<Object>();
			int  count=0;
			
			if(Variables.column.startsWith("N_") || Variables.column.startsWith("n_")){
			try {
				 Statement st = conn.createStatement();
				 ResultSet rs = st.executeQuery(query);
				
				while (rs.next()) {
					 
					list1.add(rs.getInt(1));
				 } 
				conn.close();
			 } 
			catch (SQLException e) {
				e.printStackTrace();
			}
			
			for(int i=0;i<list1.size();i++){
				if((int)arr[i]==(int)list1.get(i)){
					System.out.print(list1.get(i) + " ");
					count++;
				}
					
			}
			}
			
			else if(Variables.column.startsWith("V_") || Variables.column.startsWith("v_")){
				
				System.out.println("in");
				try {
					 Statement st = conn.createStatement();
					 ResultSet rs = st.executeQuery(query);
					
					while (rs.next()) {
						 
						list1.add(rs.getString(1));
					 } 
					conn.close();
				 } 
				catch (SQLException e) {
					e.printStackTrace();
				}
				
				for(int i=0;i<list1.size();i++){
					
					 if((arr[i]).equals(list1.get(i))){
						System.out.print(list1.get(i) + " ");
						count++;
						
					}
						
				}
				
				
			}
			
			else if(Variables.column.startsWith("D_") || Variables.column.startsWith("d_")){
				
				DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
				DateFormat df1 = new SimpleDateFormat("MM/dd/yyyy");
				
				try {
					 Statement st = conn.createStatement();
					 ResultSet rs = st.executeQuery(query);
					
					while (rs.next()) {
						 
						list1.add(rs.getString(1));
					 } 
					conn.close();
				 } 
				catch (SQLException e) {
					e.printStackTrace();
				}
				
				for(int i=0;i<list1.size();i++){
					if(arr[i].equals(df1.format(df.parse((String)list1.get(i))))){
						System.out.print(df1.format(df.parse((String)list1.get(i))) + " ");
						count++;
					}
						
				}
				
				
			}
			
			System.out.println("");
			if(count==list1.size())
			{
				System.out.println("*************************");
				System.out.println("*                       *");
				System.out.println("*         PASSED        *");
				System.out.println("*                       *");
				System.out.println("*************************");
			}
			
			else
			{
				System.out.println("*************************");
				System.out.println("*                       *");
				System.out.println("*         FAILED        *");
				System.out.println("*                       *");
				System.out.println("*************************");
			}
			
			
			
			
		
		
		}
		
		// This function will show the test results (error), whether the case is passed or failed
		public static void test_results_error(int count, String DQ_Name) {
		
			
			
			String query = "select v_batch_run_id  from batch_run where v_batch_id = '" +Variables.infodom+"_"+DQ_Name +"_batch' order by timestamp";
			System.out.println(query);
			System.out.println(Variables.configUser+ " " +Variables.ConfigPassword);
			int val=0;
			String x="";
			Connection  conn;
			
			try {
				 conn= dbConnect.getConnect(Variables.configUser, Variables.ConfigPassword);
				 Statement st = conn.createStatement();
				 ResultSet rs = st.executeQuery(query);
				
				while (rs.next()) {
					 
					x =rs.getString(1);
				 } 
				conn.close();
			 } 
			catch (SQLException e) {
				e.printStackTrace();
			}
			
			String query1 ="select N_rec_fail_count from dq_result_detl_master where v_batch_id = '"+x+"'";
			System.out.println(query1);
		
			try {
				 conn= dbConnect.getConnect(Variables.atomUser, Variables.AtomPassword);
				 Statement st = conn.createStatement();
				 ResultSet rs = st.executeQuery(query1);
				
				while (rs.next()) {
					 
					val =rs.getInt(1);
				 } 
				conn.close();
			 } 
			catch (SQLException e) {
				e.printStackTrace();
			}
			
			System.out.println(count+" "+ val);
			
			if(count==val)
			{
				System.out.println("*************************");
				System.out.println("*                       *");
				System.out.println("*         PASSED        *");
				System.out.println("*                       *");
				System.out.println("*************************");
			}
			
			else
			{
				System.out.println("*************************");
				System.out.println("*                       *");
				System.out.println("*         FAILED        *");
				System.out.println("*                       *");
				System.out.println("*************************");
			}
		
		}
		
		public static void FnTest_results(Object [] arr) throws ParseException
		{
			Connection conn = dbConnect.getConnect(Variables.atomUser, Variables.AtomPassword);
			
			String query = "select " + Variables.column + " from " + Variables.table + " where " + Variables.MIScolumn + " = to_date('" + Variables.MIS_date + "', 'dd-MM-yyyy') order by " + Variables.primaryKey;
			System.out.println(query);
			ArrayList <Object> list1= new ArrayList<Object>();
			int  count=0;
			
			if(Variables.column.startsWith("N_") || Variables.column.startsWith("n_")){
			try {
				 Statement st = conn.createStatement();
				 ResultSet rs = st.executeQuery(query);
				int i=0;
				while (rs.next()) {
					 System.out.println("element "+i+"is :"+rs.getInt(1));
					list1.add(rs.getInt(1));
					i++;
				 } 
				conn.close();
			 } 
			catch (SQLException e) {
				e.printStackTrace();
			}
			
			for(int i=0;i<list1.size();i++){
				System.out.println("our :"+arr[i]);
				System.out.println("list1 :"+list1.get(i));
				if((int)arr[i]==(int)list1.get(i)){
					count++;
				}
					
			}
			}
			
			
			System.out.println("");
			if(count==list1.size())
			{
				System.out.println("*************************");
				System.out.println("*                       *");
				System.out.println("*         PASSED        *");
				System.out.println("*                       *");
				System.out.println("*************************");
			}
			
			else
			{
				System.out.println("*************************");
				System.out.println("*                       *");
				System.out.println("*         FAILED        *");
				System.out.println("*                       *");
				System.out.println("*************************");
			}
			
			
		}
}
