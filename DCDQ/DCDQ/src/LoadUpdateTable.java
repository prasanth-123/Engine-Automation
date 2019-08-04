/************************************************************************************************
* Written by					:  	sai.ram.tatiraju@oracle.com
* Class Name					:  	LoadUpdateTable
* Class Description			    : 	This class will have update MIS date function, Load test data
* 									into base table function and Load functions for the base column.
* Date Created					:  	29-April-2016
* Date Modified					: 	28-June-2016
* Modified by					: 	sai.ram.tatiraju@oracle.com
*************************************************************************************************/
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;


public class LoadUpdateTable {


	public static void Fn_LoadTestData(String dbuser, String dbpassword) 
	{	
		String file="";
		StringBuffer queries=new StringBuffer();
		FileReader dcdq;
		
		System.out.println("**************Fn_LoadTestData started***************");
		
		try 
		{
			
			dcdq = new FileReader(new File("src/Insert_query_files/load.sql"));
			BufferedReader br=new BufferedReader(dcdq);
			while((file =br.readLine()) !=null)
			{
				queries.append(file);
			}
			br.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		//This can be removed once the flow is finished.
		Variables.atomUser = dbuser;
		Variables.AtomPassword = dbpassword;
		
		
		String[] q = queries.toString().split(";");
		for(int i =0; i<q.length; i++)
			{
			
			
				Connection conn = dbConnect.getConnect(dbuser, dbpassword);
				Statement st;
				try {
					st = conn.createStatement();
					st.executeUpdate(q[i]);
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		System.out.println("**************Fn_LoadTestData completed successfully**************");
	
	}
	
	
	
	
	public static void Fn_UpdateMISDate()
	{
		
		ArrayList <String> list = new ArrayList<String>();
		Connection conn =null;
		String query = "select "+ Variables.primaryKey +" from " + Variables.table ;
		
		
		
		try {
			
			 conn = dbConnect.getConnect( Variables.atomUser , Variables.AtomPassword);
			 
			 Statement st = conn.createStatement();
			 
			 ResultSet rs = st.executeQuery(query);
			
			 while (rs.next()) {
				 
				 list.add(rs.getString(Variables.primaryKey));
			 }
		
			}
		
		catch (Exception e) {
			
			e.printStackTrace();
			}	
		
		
		
					String[] arr= new String[list.size()];
					
					
					for(int k=0;k<arr.length;k++)
					{
						arr[k]=list.get(k);
						//System.out.println(arr[k]);
					}
			
		String MIS_date="01-01-2013";
		int row_gap = 5;
		int count = row_gap;
		
		for(int k=0;k<arr.length;k++)
		{
		
			if(k==row_gap)
			{
			//can take any date in current format    
			SimpleDateFormat dateFormat = new SimpleDateFormat( "dd-MM-yyyy" );   
			Calendar cal = Calendar.getInstance();    
			try {
				cal.setTime( dateFormat.parse(MIS_date));
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}    
			cal.add( Calendar.DATE, 1 );    
			String convertedDate=dateFormat.format(cal.getTime());    
			System.out.println("Date increase by one.."+convertedDate);
			MIS_date = convertedDate;
			row_gap = row_gap + count;
			}
			
			String query1 = "update " + Variables.table + " set " + Variables.MIScolumn + " = " + "to_date('" + MIS_date + "', 'dd-MM-yyyy')"  + " where " + Variables.primaryKey +" = '" + arr[k] + "'";
			 
			
			 
			
			try {
				Statement st;
				conn = dbConnect.getConnect( Variables.atomUser , Variables.AtomPassword);
				st = conn.createStatement();
				st.executeQuery(query1);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			 
		
		}
					
					
		
		
	}
	
	
	
	
	
	
	
	public static void Fn_UpdateTestData( String MIS , String type) throws ParseException
	{
		
		ArrayList <String> list = new ArrayList<String>();
		Connection conn =null;
		String query = "select " + Variables.primaryKey + " from " + Variables.table + " where " + Variables.MIScolumn + " = to_date('" + MIS + "', 'dd-MM-yyyy')" ;
		System.out.println(query);
		Variables.MIS_date = MIS;
		
		try {
			
			 conn = dbConnect.getConnect( Variables.atomUser , Variables.AtomPassword);
			 
			 Statement st = conn.createStatement();
			 
			 ResultSet rs = st.executeQuery(query);
			
			 while (rs.next()) {
				 
				 list.add(rs.getString("v_exposure_id"));
			 }
		
			}
		
		catch (Exception e) {
			
			e.printStackTrace();
		}	
		
		
		
					Variables.pk= new String[list.size()];
					
					
					for(int k=0;k<Variables.pk.length;k++)
					{
						Variables.pk[k]=list.get(k);
						System.out.println(Variables.pk[k]);
					}
				

		
		Object[]  arr1 = UpdateData.Update(type);
	
	
	
	
		for(int k=0;k<arr1.length;k++)
		{
			
			String sql ="";
			
			
			if (arr1[k]==null)
			{
				sql = "update " + Variables.table + " set " + Variables.column + " = " + arr1[k] + " where v_exposure_id = '" + Variables.pk[k] + "'";
			}
			
			
			else  if(Variables.date_flag)
			{if(((String) arr1[k]).contains("/")||((String) arr1[k]).contains("-"))
				sql = "update " + Variables.table + " set " + Variables.column + " = to_date('" + arr1[k] + "', 'yyyy-MM-dd') where v_exposure_id = '" + Variables.pk[k] + "'";
			}
			
			
			
			
			else
			sql = "update " + Variables.table + " set " + Variables.column + " = '" + arr1[k] + "' where v_exposure_id = '" + Variables.pk[k] + "'";
			
			
			System.out.println(sql);
			
			Variables.mul_col_vals.add(arr1[k]);
			Variables.mul_pk_vals.add(Variables.pk[k]);
		
			conn = dbConnect.getConnect( Variables.atomUser , Variables.AtomPassword);
			
			try {
				Statement st;
				st = conn.createStatement();
				st.executeQuery(sql);
			} 
			
			
			catch (SQLException e) {
				e.printStackTrace();
			}
			 
			 
		}
		
		
	}

	

	
	
	
	public static void main(String[] args) throws ParseException {
		
        
	}
	
	
	
	
}
