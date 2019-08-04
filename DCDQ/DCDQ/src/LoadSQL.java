/************************************************************************************************
* Written by					:  	sai.ram.tatiraju@oracle.com
* Class Name					:  	Load SQL
* Class Description			    : 	This class will Load test data into the base table.
* Date Created					:  	1-May-2016
* Date Modified					: 	23-June-2016
* Modified by					: 	sai.ram.tatiraju@oracle.com
*************************************************************************************************/

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;



@SuppressWarnings("unused")
public class LoadSQL 
{
	public static void Fn_Readfile( String dbuser, String dbpassword)
	{
		System.out.println("**************Readfile started**************");
			String file="";
			StringBuffer queries=new StringBuffer();
			FileReader dcdq;
			
			try 
			{
				
				dcdq = new FileReader(new File("src/Insert_query_files/colref_check.sql"));
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
					try 
					{
						st = conn.createStatement();
						st.executeUpdate(q[i]);
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			
		
			System.out.println("**************Reading file completed succesfully**************");
	}
	
	public static void main(String[] args) {
		Fn_Readfile( "ofsatmcap5", "password123");
		
		

	}
}
			
			
