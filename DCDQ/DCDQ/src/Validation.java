/************************************************************************************************
* Written by					:  	sai.ram.tatiraju@oracle.com
* Class Name					:  	Validation
* Class Description			    : 	This class will have the validation functions for all the
* 									specific checks in DQ.
* Date Created					:  	17-April-2016
* Date Modified					: 	28-June-2016
* Modified by					: 	sai.ram.tatiraju@oracle.com
*************************************************************************************************/
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.text.DateFormat;

import javax.swing.text.TabExpander;
import javax.swing.text.html.MinimalHTMLWriter;
 



public class Validation {

	public static enum Type 
	{
	    RANGE , DATALENGTH , COL_REF, ISNULL , LOV , BLANK , REF , DUPLICATE 
	}
	
	public static void  Fn_Validate(String dbuser, String dbpassword, String type, String DQ_Name) throws ParseException
	{
		if(Variables.severity.equalsIgnoreCase("E")){
		
			Validation_error.Fn_Validate(dbuser, dbpassword, type, DQ_Name);
		}
		
		else{
		Connection conn =null;
		int count =0;
		Variables.col_vals = new Object[Variables.pk.length];
		
		switch (Type.valueOf(type.toUpperCase())) {
		
		case RANGE:
			
		
			Variables.get_val(DQ_Name,1);
			Variables.assType();
			System.out.println("value of range is " + Variables.val);
			
			
			if((Variables.global==null)&&(Variables.local==null))
			{
			
			Object[]  arr_range = {Variables.a1, Variables.a2, Variables.a3, Variables.a4, Variables.a5};
			
			for(int i=0; i<5; i++)
			{
			if(arr_range[i]!=null){
				if(Variables.min_inclu.equalsIgnoreCase("y") && Variables.max_inclu.equalsIgnoreCase("y"))
				{
					
					if((int)arr_range[i]>Integer.parseInt(Variables.max) || (int)arr_range[i]<Integer.parseInt(Variables.min))
						{
						if(Variables.assType.equalsIgnoreCase("3"))
							arr_range[i]= Integer.parseInt((String) Variables.col_vals[i]);
							
						else	
						arr_range[i]= Integer.parseInt(Variables.val);
						
						count++;
						}
				}
				
				if(Variables.min_inclu.equalsIgnoreCase("n") && Variables.max_inclu.equalsIgnoreCase("y"))
				{
					if((int)arr_range[i] > Integer.parseInt(Variables.max) || (int)arr_range[i]<=Integer.parseInt(Variables.min))
						{
						if(Variables.assType.equalsIgnoreCase("3"))
							arr_range[i]= Integer.parseInt((String) Variables.col_vals[i]);
						else
						arr_range[i]= Integer.parseInt(Variables.val);
						
						count++;
						}
				}
				
				if(Variables.min_inclu.equalsIgnoreCase("y") && Variables.max_inclu.equalsIgnoreCase("n"))
				{
					if((int)arr_range[i] >= Integer.parseInt(Variables.max) || (int)arr_range[i]<Integer.parseInt(Variables.min))
						{
						if(Variables.assType.equalsIgnoreCase("3"))
							arr_range[i]= Integer.parseInt((String) Variables.col_vals[i]);
						else
						arr_range[i]= Integer.parseInt(Variables.val);
						
						count++;
						}
				}
				
				if(Variables.min_inclu.equalsIgnoreCase("n") && Variables.max_inclu.equalsIgnoreCase("n"))
				{
					if((int)arr_range[i] >= Integer.parseInt(Variables.max) ||(int) arr_range[i]<=Integer.parseInt(Variables.min))
						{
						if(Variables.assType.equalsIgnoreCase("3"))
							arr_range[i]= Integer.parseInt((String) Variables.col_vals[i]);
						else
						arr_range[i]= Integer.parseInt(Variables.val);
						
						count++;
						}
				}
				
				
				
			}
			Variables.mul_upd_range.add(arr_range[i]);
			
			
			}
			
			
			System.out.println(count + " rows have been Impacted");
			System.out.println(arr_range[0] + " " +arr_range[1] + " " +arr_range[2] + " " +arr_range[3] + " " +arr_range[4]);
			
			 if (Variables.noofcheck.equalsIgnoreCase("single"))
			 { 
			TestResults.test_results(arr_range);
			 }
			
			}
			
			else
			{
			
				Object[]  arr_range = { Variables.a1, Variables.a2, Variables.a3, Variables.a4, Variables.a5};
				ArrayList <String> list1 = new ArrayList<String>();
				String sql2="";
				conn = dbConnect.getConnect( dbuser , dbpassword);
				Statement st;
				ResultSet rs ;
				
				if((Variables.global!=null)&&(Variables.local!=null))
				{sql2 = "select " + Variables.primaryKey + " from " + Variables.table + " where " + Variables.MIScolumn + " = to_date('" + Variables.MIS_date + "', 'dd-MM-yyyy') and " + Variables.global + " and " + Variables.local ;
				System.out.println("qweeeryyy");
				}
				if((Variables.global!=null)&&(Variables.local==null))
				sql2 = "select " + Variables.primaryKey + " from " + Variables.table + " where " + Variables.MIScolumn + " = to_date('" + Variables.MIS_date + "', 'dd-MM-yyyy') and " + Variables.global ;	
					
				if((Variables.global==null)&&(Variables.local!=null))
				sql2 = "select " + Variables.primaryKey + " from " + Variables.table + " where " + Variables.MIScolumn + " = to_date('" + Variables.MIS_date + "', 'dd-MM-yyyy') and " + Variables.local ;	
					
				try {
					 st = conn.createStatement();
					 rs = st.executeQuery(sql2);
					
					while (rs.next()) {
						 
						list1.add(rs.getString(1));
					 } 
					
				 } 
				catch (SQLException e) {
					e.printStackTrace();
				}
				
				
				Variables.pk_filt = new String[list1.size()];
				
				for(int k=0;k<Variables.pk_filt.length;k++)
				{
					Variables.pk_filt[k]=list1.get(k);
					System.out.println(Variables.pk_filt[k]);
				}
				
				
				int[] cols = new int[list1.size()];
				for(int i =0; i<cols.length;i++)
				{
				String	query_1 ="select " + Variables.column + " from " + Variables.table + " where " + Variables.primaryKey + " = '" + Variables.pk_filt[i] + "'";				
				
				try {
					st = conn.createStatement();
					 rs = st.executeQuery(query_1);
						
						while (rs.next()) {
							 
							cols[i]=rs.getInt(1);
							System.out.println(cols[i]);
						 } 
					
				} catch (SQLException e) {
					e.printStackTrace();
				}
				
				}
				
				
				for(int i=0; i<cols.length; i++)
				{
					if(arr_range[i]!=null){
					if(Variables.min_inclu.equalsIgnoreCase("y") && Variables.max_inclu.equalsIgnoreCase("y"))
					{
				
						if((int)arr_range[Variables.get_index(i)]>Integer.parseInt(Variables.max) || (int)arr_range[Variables.get_index(i)]<Integer.parseInt(Variables.min))
							{
							if(Variables.assType.equalsIgnoreCase("3"))
								arr_range[Variables.get_index(i)] = Integer.parseInt((String)Variables.col_vals[Variables.get_index(i)]);
								
							else
								arr_range[Variables.get_index(i)]= Integer.parseInt(Variables.val);
							
							count++;
							}
					}
					
					if(Variables.min_inclu.equalsIgnoreCase("n") && Variables.max_inclu.equalsIgnoreCase("y"))
					{
						if((int)arr_range[Variables.get_index(i)] > Integer.parseInt(Variables.max) ||(int) arr_range[Variables.get_index(i)]<=Integer.parseInt(Variables.min))
							{
							if(Variables.assType.equalsIgnoreCase("3"))
								arr_range[Variables.get_index(i)]= Integer.parseInt((String)Variables.col_vals[Variables.get_index(i)]);
								
							else
								arr_range[Variables.get_index(i)]= Integer.parseInt(Variables.val);
							
							count++;
							}
					}
					
					if(Variables.min_inclu.equalsIgnoreCase("y") && Variables.max_inclu.equalsIgnoreCase("n"))
					{
						if((int)arr_range[Variables.get_index(i)] >= Integer.parseInt(Variables.max) ||(int) arr_range[Variables.get_index(i)]<Integer.parseInt(Variables.min))
							{
							if(Variables.assType.equalsIgnoreCase("3"))
								arr_range[Variables.get_index(i)]= Integer.parseInt((String)Variables.col_vals[Variables.get_index(i)]);
								
							else
								arr_range[Variables.get_index(i)]= Integer.parseInt(Variables.val);
							
							count++;
							}
					}
					
					if(Variables.min_inclu.equalsIgnoreCase("n") && Variables.max_inclu.equalsIgnoreCase("n"))
					{
						System.out.println("in1");
						if((int)arr_range[Variables.get_index(i)] >= Integer.parseInt(Variables.max) ||(int) arr_range[Variables.get_index(i)]<=Integer.parseInt(Variables.min))
							{
							System.out.println("in2");
							if(Variables.assType.equalsIgnoreCase("3"))
								{arr_range[Variables.get_index(i)]= Integer.parseInt((String)Variables.col_vals[Variables.get_index(i)]);
								System.out.println("in3");}
							else
								{arr_range[Variables.get_index(i)]= Integer.parseInt(Variables.val);
								System.out.println("in4");}
							count++;
							}
					}
					
				}
				
					
				}
				System.out.println(count + " rows have been Impacted");
				System.out.println(arr_range[0] + " " +arr_range[1] + " " +arr_range[2] + " " +arr_range[3] + " " +arr_range[4]);
				
				TestResults.test_results(arr_range);
				
				for(int i=0; i<5;i++){
					Variables.mul_upd_range.add(arr_range[i]);	
				}
				
				
			}
			break;
		
			
			
		case DATALENGTH:
			
			if((Variables.global==null)&&(Variables.local==null))
			{
				
			if(Variables.column.startsWith("V_") || Variables.column.startsWith("v_"))
			{
				
				String[]  arr_dl_s = {(String)Variables.a1, (String)Variables.a2, (String)Variables.a3, (String)Variables.a4, (String)Variables.a5};
				
				for(int i=0; i<arr_dl_s.length; i++)
				{
					
					if((arr_dl_s[i].length() < Integer.parseInt(Variables.min))||(arr_dl_s[i].length() > Integer.parseInt(Variables.max)))
					{
						System.out.print(arr_dl_s[i] + " " );
						count++;
					}
					Variables.mul_upd_data.add(arr_dl_s[i]);
				}
				
				System.out.println(" are outside the range");
				System.out.println(count + " rows are outside the range");
				
				 if (Variables.noofcheck.equalsIgnoreCase("single"))
				 {TestResults.test_results_error(count, DQ_Name);}
			}
			
			if(Variables.column.startsWith("N_") || Variables.column.startsWith("n_")) 
			{
				
				Object[] arr_dl_n =	{ Variables.a1, Variables.a2, Variables.a3, Variables.a4, Variables.a5};
				
				for(int i=0; i<arr_dl_n.length; i++)
				{
					
					int min_val = (int) Math.pow(10,(Integer.parseInt(Variables.min)-1));
					int max_val = (int) (Math.pow(10,(Integer.parseInt(Variables.max)))-1);
					
					if(min_val==1)
						min_val=0;
					
					if(((int)arr_dl_n[i] < min_val)||((int)arr_dl_n[i] > max_val))
					{
						System.out.print(arr_dl_n[i] + " ");
						count++;
					}
					
					Variables.mul_upd_data.add(arr_dl_n[i]);
				}
				System.out.println(" are outside the range");
				System.out.println(count + " rows are outside the range");
				
				 if (Variables.noofcheck.equalsIgnoreCase("single"))
				 {TestResults.test_results_error(count, DQ_Name);}
			}
			
			}
			
			else
			{
			ArrayList <String> list1 = new ArrayList<String>();
			String sql2="";
			
			
			if((Variables.global!=null)&&(Variables.local!=null))
			{sql2 = "select " + Variables.column + " from " + Variables.table + " where " + Variables.MIScolumn + " = to_date('" + Variables.MIS_date + "', 'dd-MM-yyyy') and " + Variables.global + " and " + Variables.local ;
			System.out.println(sql2);
			}
			if((Variables.global!=null)&&(Variables.local==null))
			sql2 = "select " + Variables.column + " from " + Variables.table + " where " + Variables.MIScolumn + " = to_date('" + Variables.MIS_date + "', 'dd-MM-yyyy') and " + Variables.global ;	
				
			if((Variables.global==null)&&(Variables.local!=null))
			sql2 = "select " + Variables.column + " from " + Variables.table + " where " + Variables.MIScolumn + " = to_date('" + Variables.MIS_date + "', 'dd-MM-yyyy') and " + Variables.local ;	
				
			try {
				
				conn = dbConnect.getConnect( dbuser , dbpassword);
				Statement st = conn.createStatement();
				ResultSet rs = st.executeQuery(sql2);
				
				while (rs.next()) {
					 
					list1.add(rs.getString(Variables.column));
				 } 
				
			 } 
			catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			String[] cols = new String[list1.size()];
			for(int k=0;k<cols.length;k++)
			{
				cols[k]=list1.get(k);
				
			}
			
			if(Variables.column.startsWith("V_") || Variables.column.startsWith("v_"))
			{
				
				for(int i=0; i<cols.length; i++)
				{
					
					if((cols[i].length() < Integer.parseInt(Variables.min))||(cols[i].length() > Integer.parseInt(Variables.max)))
					{
						System.out.print(cols[i] + " ");
						count++;
					}
					
				}
				
				System.out.println("  are outside the range");
				System.out.println(count + " rows are outside the range");
				 if (Variables.noofcheck.equalsIgnoreCase("single"))
				 {TestResults.test_results_error(count, DQ_Name);}
			}
			
			if(Variables.column.startsWith("N_") || Variables.column.startsWith("n_")) 
			{
				
				for(int i=0; i<cols.length; i++)
				{
					
					int min_val = (int) Math.pow(10,(Integer.parseInt(Variables.min)-1));
					int max_val = (int) (Math.pow(10,(Integer.parseInt(Variables.max)))-1);
					
					if((Integer.parseInt(cols[i]) < min_val)||(Integer.parseInt(cols[i]) > max_val))
					{
						System.out.print(cols[i] + " ");
						count++;
					}
					}
				System.out.println("  are outside the range");
				System.out.println(count + " rows are outside the range");
				 if (Variables.noofcheck.equalsIgnoreCase("single"))
				 {TestResults.test_results_error(count, DQ_Name);}
			}
			
			
			}
			
			break;
			
		case COL_REF:
			
			Variables.get_val(DQ_Name,2);
			Variables.assType();
			
			if((Variables.global==null)&&(Variables.local==null))
			{
			
			if(Variables.col_ref_name.startsWith("N_")||Variables.col_ref_name.startsWith("D_"))
            {
				
				
				
				if(Variables.col_ref_name.startsWith("N_"))
                {
					
					Object[] arr_crc_2 = {Variables.a1, Variables.a2, Variables.a3, Variables.a4, Variables.a5};
					
					
					
					String sql="select "+Variables.col_ref_name+" from "+Variables.table+" where "+Variables.MIScolumn+" = to_date('" + Variables.MIS_date + "', 'dd-MM-yyyy')";
                    System.out.println(sql);
                    System.out.println(Variables.AtomPassword + " " + Variables.atomUser);
                    conn = dbConnect.getConnect(Variables.atomUser,Variables.AtomPassword);
                    System.out.println("Connection established");
                   
                          ArrayList <String> list = new ArrayList<String>();
                          
						try {
							
							Statement st;
							st = conn.createStatement();
							ResultSet rs = st.executeQuery(sql);
	                          while(rs.next())
	                          {
	                                 list.add(rs.getString(Variables.col_ref_name));
	                          }
							} 
						
						catch (SQLException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
                         
                       String[] a= new String[list.size()];
                       
      					
                       if(Variables.col_ref_operator.equalsIgnoreCase(">"))
                       {
                       
      					for(int k=0;k<a.length;k++)
      					{
      						a[k]=list.get(k);
      						System.out.println(arr_crc_2[k] + " " + a[k]);
      						if((int)arr_crc_2[k] <= Integer.parseInt(a[k]))
      							{
      							if(Variables.assType.equalsIgnoreCase("3"))
      								arr_crc_2[k]= Integer.parseInt((String)Variables.col_vals[k]);
      							
          						else
      							arr_crc_2[k]=Integer.parseInt(Variables.val);
      							count++;
      							}
      					}
                       }
                       
                       else if(Variables.col_ref_operator.equalsIgnoreCase("<")) 
      					
                       {
                           
         					for(int k=0;k<a.length;k++)
         					{
         						a[k]=list.get(k);
         						System.out.println(arr_crc_2[k] + " " + a[k]);
         						if((int)arr_crc_2[k] >= Integer.parseInt(a[k]))
         							{
         							if(Variables.assType.equalsIgnoreCase("3"))
          								arr_crc_2[k]= Integer.parseInt((String)Variables.col_vals[k]);
          							
              						else
         							arr_crc_2[k]=Integer.parseInt(Variables.val);
         							count++;
         							}
         					}
                        }
      					
                       else if(Variables.col_ref_operator.equalsIgnoreCase(">=")) 
         					
                       {
                           
         					for(int k=0;k<a.length;k++)
         					{
         						a[k]=list.get(k);
         						System.out.println(arr_crc_2[k] + " " + a[k]);
         						if((int)arr_crc_2[k] < Integer.parseInt(a[k]))
         							{
         							if(Variables.assType.equalsIgnoreCase("3"))
          								arr_crc_2[k]= Integer.parseInt((String)Variables.col_vals[k]);
          							
              						else
         							arr_crc_2[k]=Integer.parseInt(Variables.val);
         							count++;
         							}
         					}
                        }
                       
                       
                       else if(Variables.col_ref_operator.equalsIgnoreCase("<=")) 
        					
                       {
                           
         					for(int k=0;k<a.length;k++)
         					{
         						a[k]=list.get(k);
         						System.out.println(arr_crc_2[k] + " " + a[k]);
         						if((int)arr_crc_2[k] > Integer.parseInt(a[k]))
         							{
         							if(Variables.assType.equalsIgnoreCase("3"))
          								arr_crc_2[k]= Integer.parseInt((String)Variables.col_vals[k]);
          							
              						else
         							arr_crc_2[k]=Integer.parseInt(Variables.val);
         							count++;
         							}
         					}
                        }
                       
                       
                       else if(Variables.col_ref_operator.equalsIgnoreCase("=")) 
        					
                       {
                           
         					for(int k=0;k<a.length;k++)
         					{
         						a[k]=list.get(k);
         						System.out.println(arr_crc_2[k] + " " + a[k]);
         						if((int)arr_crc_2[k] != Integer.parseInt(a[k]))
         							{
         							if(Variables.assType.equalsIgnoreCase("3"))
          								arr_crc_2[k]= Integer.parseInt((String)Variables.col_vals[k]);
          							
              						else
         							arr_crc_2[k]=Integer.parseInt(Variables.val);
         							count++;
         							}
         					}
                        }
                       
                       
                       else if(Variables.col_ref_operator.equalsIgnoreCase("<>")) 
       					
                       {
                           
         					for(int k=0;k<a.length;k++)
         					{
         						a[k]=list.get(k);
         						System.out.println(arr_crc_2[k] + " " + a[k]);
         						if((int)arr_crc_2[k] == Integer.parseInt(a[k]))
         							{
         							if(Variables.assType.equalsIgnoreCase("3"))
          								arr_crc_2[k]= Integer.parseInt((String)Variables.col_vals[k]);
          							
              						else
         							arr_crc_2[k]=Integer.parseInt(Variables.val);
         							count++;
         							}
         					}
                        }
                       
      					
      					
      					 System.out.println(count + " no. of rows have been updated");
      					 System.out.println(arr_crc_2[0]+ " " +arr_crc_2[1]+ " " +arr_crc_2[2]+ " " +arr_crc_2[3]+ " " +arr_crc_2[4]);
      					 
      					 TestResults.test_results(arr_crc_2);
      					 for(int i=0; i<5;i++){
      					 Variables.mul_upd_col_ref.add(arr_crc_2[i]);
      					 }
                }
				
				
				
				
				
                
				else if(Variables.col_ref_name.startsWith("D_"))
                {
					
					
					DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
					DateFormat df1 = new SimpleDateFormat("MM/dd/yyyy");
				
					Date[] arr_crc_3 = { df.parse((String) Variables.a1), df.parse((String) Variables.a2), df.parse((String) Variables.a3), df.parse((String) Variables.a4), df.parse((String) Variables.a5)};
					
					 Date[] a= new Date[Variables.pk.length];
					 
					 String sql= "";
					for(int i=0;i<Variables.pk.length;i++) 
	                  {
	                	  sql="select "+Variables.col_ref_name+" from "+Variables.table+" where "+Variables.MIScolumn+" = to_date('" + Variables.MIS_date + "', 'dd-MM-yyyy') and "+Variables.primaryKey+" = '"+Variables.pk[i] + "'";
	                	  
	                      conn = dbConnect.getConnect(Variables.atomUser,Variables.AtomPassword);
	                      System.out.println("Connection established");
	                      
	                            Statement st;
								try {
									st = conn.createStatement();
									ResultSet rs = st.executeQuery(sql);
		                            while(rs.next())
		                            {
		                         	   a[i]= df.parse(rs.getString(1));
		                         	   System.out.println(a[i]);
		                            }
								} catch (SQLException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
	                            
	                  }

                       if(Variables.col_ref_operator.equalsIgnoreCase(">"))
                       {
                       
      					for(int k=0;k<a.length;k++)
      					{
      						      						
      						if(arr_crc_3[k].compareTo(a[k])<1)
      							{
      							if(Variables.assType.equalsIgnoreCase("3"))
      								arr_crc_3[k]= df.parse((String)Variables.col_vals[k]);
      							
          						else
      							arr_crc_3[k]=df.parse(Variables.val);
      							
      							count++;
      							}
      					}
                       }
                      
                       else if(Variables.col_ref_operator.equalsIgnoreCase("<"))
                       {
                       
      					for(int k=0;k<a.length;k++)
      					{
      						      						
      						if(arr_crc_3[k].compareTo(a[k])>-1)
      							{
      							if(Variables.assType.equalsIgnoreCase("3"))
      								arr_crc_3[k]= df.parse((String)Variables.col_vals[k]);
      							
          						else
      							arr_crc_3[k]=df1.parse(Variables.val);
      							count++;
      							}
      					}
                       }
                       
                       else if(Variables.col_ref_operator.equalsIgnoreCase(">="))
                       {
                       
      					for(int k=0;k<a.length;k++)
      					{
      						      						
      						if(arr_crc_3[k].compareTo(a[k])<0)
      						{
      							if(Variables.assType.equalsIgnoreCase("3"))
      								arr_crc_3[k]= df.parse((String)Variables.col_vals[k]);
      							
          						else
      							arr_crc_3[k]=df1.parse(Variables.val);
      							count++;
      							}
      					}
                       }
                       
                       else if(Variables.col_ref_operator.equalsIgnoreCase("<="))
                       {
                       
      					for(int k=0;k<a.length;k++)
      					{
      						      						
      						if(arr_crc_3[k].compareTo(a[k])>0)
      						{
      							if(Variables.assType.equalsIgnoreCase("3"))
  								arr_crc_3[k]= df.parse((String)Variables.col_vals[k]);
  							
      							else
      							arr_crc_3[k]=df1.parse(Variables.val);
      							count++;
      							}
      					}
                       }
                       
                       else if(Variables.col_ref_operator.equalsIgnoreCase("<>"))
                       {
                       
      					for(int k=0;k<a.length;k++)
      					{
      						      						
      						if(arr_crc_3[k].compareTo(a[k])==0)
      						{
      							if(Variables.assType.equalsIgnoreCase("3"))
      								arr_crc_3[k]= df.parse((String)Variables.col_vals[k]);
      							
          						else
      							arr_crc_3[k]=df1.parse(Variables.val);
      							count++;
      							}
      					}
                       }
					
                       
                       else if(Variables.col_ref_operator.equalsIgnoreCase("="))
                       {
                       
      					for(int k=0;k<a.length;k++)
      					{
      						      						
      						if(arr_crc_3[k].compareTo(a[k])!=0)
      						{
      							if(Variables.assType.equalsIgnoreCase("3"))
  								arr_crc_3[k]= df.parse((String)Variables.col_vals[k]);
  							
      							else
      							arr_crc_3[k]=df1.parse(Variables.val);
      							count++;
      							}
      					}
                       }
                       
                    System.out.println( count + " rows have been updated");
                    System.out.println(df1.format(arr_crc_3[0]) + " " + df1.format(arr_crc_3[1]) + " " + df1.format(arr_crc_3[2]) + " " + df1.format(arr_crc_3[3]) + " " + df1.format(arr_crc_3[4]));
   					
   					String[] arr_crc_3_s = {df1.format(arr_crc_3[0]),df1.format(arr_crc_3[1]),df1.format(arr_crc_3[2]),df1.format(arr_crc_3[3]),df1.format(arr_crc_3[4])};
   					
   				 if (Variables.noofcheck.equalsIgnoreCase("single"))
   				 {TestResults.test_results(arr_crc_3_s);}
					
                }
				
				
				
            }
			
			
			else
            {
				
				if(Variables.col_ref_name.contains("/")||Variables.col_ref_name.contains("-"))
                {
					
					
					DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
					DateFormat df1 = new SimpleDateFormat("MM/dd/yyyy");
				
					Date[] arr_crc_4 = { df.parse((String) Variables.a1), df.parse((String) Variables.a2), df.parse((String) Variables.a3), df.parse((String) Variables.a4), df.parse((String) Variables.a5)};
					
					if(Variables.col_ref_operator.equalsIgnoreCase(">"))
                    {
                    
   					for(int k=0;k<arr_crc_4.length;k++)
   					{
   						      						
   						if(arr_crc_4[k].compareTo(df1.parse(Variables.col_ref_name))<1)
   							{
   							if(Variables.assType.equalsIgnoreCase("3"))
  								arr_crc_4[k]= df.parse((String)Variables.col_vals[k]);
  							
 							else
   							arr_crc_4[k] = df1.parse(Variables.val);
   							
   							count++;
   							}
   					}
                    }
					
					else if(Variables.col_ref_operator.equalsIgnoreCase("<"))
                    {
                    
   					for(int k=0;k<arr_crc_4.length;k++)
   					{
   						      						
   						if(arr_crc_4[k].compareTo(df1.parse(Variables.col_ref_name))>-1)
   							{
   							if(Variables.assType.equalsIgnoreCase("3"))
  								arr_crc_4[k]= df.parse((String)Variables.col_vals[k]);
  							
 							else
   							arr_crc_4[k] = df1.parse(Variables.val);
   							
   							count++;
   							}
   					}
                    }
					
					
					if(Variables.col_ref_operator.equalsIgnoreCase(">="))
                    {
                    
   					for(int k=0;k<arr_crc_4.length;k++)
   					{
   						      						
   						if(arr_crc_4[k].compareTo(df1.parse(Variables.col_ref_name))<0)
   							{
   							if(Variables.assType.equalsIgnoreCase("3"))
  								arr_crc_4[k]= df.parse((String)Variables.col_vals[k]);
  							
 							else
   							arr_crc_4[k] = df1.parse(Variables.val);
   							
   							count++;
   							}
   					}
                    }
					
					
					if(Variables.col_ref_operator.equalsIgnoreCase("<="))
                    {
                    
   					for(int k=0;k<arr_crc_4.length;k++)
   					{
   						      						
   						if(arr_crc_4[k].compareTo(df1.parse(Variables.col_ref_name))>0)
   							{
   							if(Variables.assType.equalsIgnoreCase("3"))
  								arr_crc_4[k]= df.parse((String)Variables.col_vals[k]);
  							
 							else
   							arr_crc_4[k] = df1.parse(Variables.val);
   							
   							count++;
   							}
   					}
                    }
					
					
					
					if(Variables.col_ref_operator.equalsIgnoreCase("<>"))
                    {
                    
   					for(int k=0;k<arr_crc_4.length;k++)
   					{
   						      						
   						if(arr_crc_4[k].compareTo(df1.parse(Variables.col_ref_name))==0)
   							{
   							if(Variables.assType.equalsIgnoreCase("3"))
  								arr_crc_4[k]= df.parse((String)Variables.col_vals[k]);
  							
 							else
   							arr_crc_4[k] = df1.parse(Variables.val);
   							
   							count++;
   							}
   					}
                    }
					
					
					if(Variables.col_ref_operator.equalsIgnoreCase("="))
                    {
                    
   					for(int k=0;k<arr_crc_4.length;k++)
   					{
   						      						
   						if(arr_crc_4[k].compareTo(df1.parse(Variables.col_ref_name))!=0)
   							{
   							if(Variables.assType.equalsIgnoreCase("3"))
  								arr_crc_4[k]= df.parse((String)Variables.col_vals[k]);
  							
 							else
   							arr_crc_4[k] = df1.parse(Variables.val);
   							
   							count++;
   							}
   					}
                    }
					
					
					
					System.out.println( count + " rows have been updated");
   					System.out.println(df1.format(arr_crc_4[0]) + " " + df1.format(arr_crc_4[1]) + " " + df1.format(arr_crc_4[2]) + " " + df1.format(arr_crc_4[3]) + " " + df1.format(arr_crc_4[4]));
   					
   					String[] arr_crc_4_s = {df1.format(arr_crc_4[0]),df1.format(arr_crc_4[1]),df1.format(arr_crc_4[2]),df1.format(arr_crc_4[3]),df1.format(arr_crc_4[4])};
   					
   				 if (Variables.noofcheck.equalsIgnoreCase("single"))
   				 {TestResults.test_results(arr_crc_4_s);}
					
					
                }
				
				else
                {
					Object[] arr_crc_1 = { Variables.a1, Variables.a2, Variables.a3, Variables.a4,Variables.a5};
					
					if(Variables.col_ref_operator.equalsIgnoreCase("<"))
						
					{
						for(int i =0; i<5;i++)
					{
						
						if((int)arr_crc_1[i] >= Integer.parseInt(Variables.col_ref_name))
							{
							
							if(Variables.assType.equalsIgnoreCase("3"))
  								arr_crc_1[i]= Integer.parseInt((String)Variables.col_vals[i]);
  							
 							else
							arr_crc_1[i]= Integer.parseInt(Variables.val);
							count++;
							}
					}
					
					}
                
					else if(Variables.col_ref_operator.equalsIgnoreCase(">"))
						
					{
						for(int i =0; i<5;i++)
					{
						
						if((int)arr_crc_1[i] <= Integer.parseInt(Variables.col_ref_name))
							{
							if(Variables.assType.equalsIgnoreCase("3"))
  								arr_crc_1[i]= Integer.parseInt((String)Variables.col_vals[i]);
  							
 							else
							arr_crc_1[i]= Integer.parseInt(Variables.val);
							count++;
							}
					}
					
					}
                
                
					else if(Variables.col_ref_operator.equalsIgnoreCase("<="))
						
					{
						for(int i =0; i<5;i++)
					{
						
						if((int)arr_crc_1[i] > Integer.parseInt(Variables.col_ref_name))
							{
							if(Variables.assType.equalsIgnoreCase("3"))
  								arr_crc_1[i]= Integer.parseInt((String)Variables.col_vals[i]);
  							
 							else
							arr_crc_1[i]= Integer.parseInt(Variables.val);
							count++;
							}
					}
					
					}
					
					else if(Variables.col_ref_operator.equalsIgnoreCase(">="))
						
					{
						for(int i =0; i<5;i++)
					{
						
						if((int)arr_crc_1[i] < Integer.parseInt(Variables.col_ref_name))
							{
							if(Variables.assType.equalsIgnoreCase("3"))
  								arr_crc_1[i]= Integer.parseInt((String)Variables.col_vals[i]);
  							
 							else
							arr_crc_1[i]= Integer.parseInt(Variables.val);
							count++;
							
							}
					}
					
					}
					
					
					else if(Variables.col_ref_operator.equalsIgnoreCase("<>"))
						
					{
						for(int i =0; i<5;i++)
					{
						
						if((int)arr_crc_1[i] == Integer.parseInt(Variables.col_ref_name))
							{
							if(Variables.assType.equalsIgnoreCase("3"))
  								arr_crc_1[i]= Integer.parseInt((String)Variables.col_vals[i]);
  							
 							else
							arr_crc_1[i]= Integer.parseInt(Variables.val);
							count++;
							}
					}
					
					}
					
					else if(Variables.col_ref_operator.equalsIgnoreCase("="))
						
					{
						for(int i =0; i<5;i++)
					{
						
						if((int)arr_crc_1[i] != Integer.parseInt(Variables.col_ref_name))
							{
							if(Variables.assType.equalsIgnoreCase("3"))
  								arr_crc_1[i]= Integer.parseInt((String)Variables.col_vals[i]);
  							
 							else
							arr_crc_1[i]= Integer.parseInt(Variables.val);
							count++;
							}
					}
					
					}
                
                
					System.out.println( count + " rows have been updated");
					System.out.println(arr_crc_1[0] + " " + arr_crc_1[1] + " " + arr_crc_1[2] + " " + arr_crc_1[3] + " " + arr_crc_1[4]);
					if (Variables.noofcheck.equalsIgnoreCase("single"))
					 { 
					 TestResults.test_results(arr_crc_1);
					 }
					for(int i=0; i<5;i++){
     					 Variables.mul_upd_col_ref.add(arr_crc_1[i]);
     					 }
                
                }
				
            }
			
			}
			
			//***************************************
			
			else
			{
				

				
				if(Variables.col_ref_name.startsWith("N_")||Variables.col_ref_name.startsWith("D_"))
	            {
					
					
					
					if(Variables.col_ref_name.startsWith("N_"))
	                {
						
						Object[] arr_crc_2 = { Variables.a1, Variables.a2, Variables.a3, Variables.a4,Variables.a5};
						
						//#######
						
						ArrayList <String> list1 = new ArrayList<String>();
	       				String sql2="";
	       				conn = dbConnect.getConnect( dbuser , dbpassword);
	       				Statement st;
	       				ResultSet rs ;
	       				
	       				if((Variables.global!=null)&&(Variables.local!=null))
	       				{sql2 = "select " + Variables.primaryKey + " from " + Variables.table + " where " + Variables.MIScolumn + " = to_date('" + Variables.MIS_date + "', 'dd-MM-yyyy') and " + Variables.global + " and " + Variables.local ;
	       				System.out.println("qweeeryyy");
	       				}
	       				if((Variables.global!=null)&&(Variables.local==null))
	       				sql2 = "select " + Variables.primaryKey + " from " + Variables.table + " where " + Variables.MIScolumn + " = to_date('" + Variables.MIS_date + "', 'dd-MM-yyyy') and " + Variables.global ;	
	       					
	       				if((Variables.global==null)&&(Variables.local!=null))
	       				sql2 = "select " + Variables.primaryKey + " from " + Variables.table + " where " + Variables.MIScolumn + " = to_date('" + Variables.MIS_date + "', 'dd-MM-yyyy') and " + Variables.local ;	
	       					
	       				try {
	       					 st = conn.createStatement();
	       					 rs = st.executeQuery(sql2);
	       					
	       					while (rs.next()) {
	       						 
	       						list1.add(rs.getString(1));
	       					 } 
	       					
	       				 } 
	       				catch (SQLException e) {
	       					e.printStackTrace();
	       				}
	       				
	       				
	       				Variables.pk_filt = new String[list1.size()];
	       				
	       				for(int k=0;k<Variables.pk_filt.length;k++)
	       				{
	       					Variables.pk_filt[k]=list1.get(k);
	       					System.out.println(Variables.pk_filt[k]);
	       				}
	       				
	       				
	       				int[] cols = new int[list1.size()];
	       				for(int i =0; i<cols.length;i++)
	       				{
	       				String	query_1 ="select " + Variables.column + " from " + Variables.table + " where " + Variables.primaryKey + " = '" + Variables.pk_filt[i] + "'";				
	       				
	       				try {
	       					st = conn.createStatement();
	       					 rs = st.executeQuery(query_1);
	       						
	       						while (rs.next()) {
	       							 
	       							cols[i]=rs.getInt(1);
	       							System.out.println(cols[i]);
	       						 } 
	       					
	       				} catch (SQLException e) {
	       					e.printStackTrace();
	       				}
	       				
	       				}
						
						//#####
						
						String sql="select "+Variables.col_ref_name+" from "+Variables.table+" where "+Variables.MIScolumn+" = to_date('" + Variables.MIS_date + "', 'dd-MM-yyyy')";
	                    System.out.println(sql);
	                    System.out.println(Variables.AtomPassword + " " + Variables.atomUser);
	                    conn = dbConnect.getConnect(Variables.atomUser,Variables.AtomPassword);
	                    System.out.println("Connection established");
	                   
	                          ArrayList <String> list = new ArrayList<String>();
	                          
							try {
								
								
								st = conn.createStatement();
								rs = st.executeQuery(sql);
		                          while(rs.next())
		                          {
		                                 list.add(rs.getString(Variables.col_ref_name));
		                          }
								} 
							
							catch (SQLException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
	                         
	                       String[] a= new String[list.size()];
	                    
	                       
	                       for(int i=0;i<a.length;i++)
	                       {
	                    	   a[i]=list.get(i);
	                       }
	                       
	                       
	                       
	      					
	                       if(Variables.col_ref_operator.equalsIgnoreCase(">"))
	                       {
	                       
	      					for(int k=0;k<cols.length;k++)
	      					{
	      						
	      						if((int)arr_crc_2[Variables.get_index(k)] <= Integer.parseInt(a[Variables.get_index(k)]))
	      							{
	      							if(Variables.assType.equalsIgnoreCase("3"))
	      								arr_crc_2[Variables.get_index(k)]= Integer.parseInt((String)Variables.col_vals[Variables.get_index(k)]);
	      							else
	      							arr_crc_2[Variables.get_index(k)]=Integer.parseInt(Variables.val);
	      							count++;
	      							}
	      					}
	                       }
	                       
	                       else if(Variables.col_ref_operator.equalsIgnoreCase("<")) 
	      					
	                       {
	                           
	         					for(int k=0;k<cols.length;k++)
	         					{
	         						if((int)arr_crc_2[Variables.get_index(k)] >= Integer.parseInt(a[Variables.get_index(k)]))
	         							{
	         							if(Variables.assType.equalsIgnoreCase("3"))
		      								arr_crc_2[Variables.get_index(k)]= Integer.parseInt((String)Variables.col_vals[Variables.get_index(k)]);
		      							
	         							else
	         							arr_crc_2[Variables.get_index(k)]=Integer.parseInt(Variables.val);
	         							count++;
	         							}
	         					}
	                        }
	      					
	                       else if(Variables.col_ref_operator.equalsIgnoreCase(">=")) 
	         					
	                       {
	                           
	         					for(int k=0;k<cols.length;k++)
	         					{
	         						if((int)arr_crc_2[Variables.get_index(k)] < Integer.parseInt(a[Variables.get_index(k)]))
	         							{
	         							if(Variables.assType.equalsIgnoreCase("3"))
		      								arr_crc_2[Variables.get_index(k)]= Integer.parseInt((String)Variables.col_vals[Variables.get_index(k)]);
		      							
	         							else
	         							arr_crc_2[Variables.get_index(k)]=Integer.parseInt(Variables.val);
	         							count++;
	         							}
	         					}
	                        }
	                       
	                       
	                       else if(Variables.col_ref_operator.equalsIgnoreCase("<=")) 
	        					
	                       {
	                           
	         					for(int k=0;k<cols.length;k++)
	         					{
	         						
	         						if((int)arr_crc_2[Variables.get_index(k)] > Integer.parseInt(a[Variables.get_index(k)]))
	         							{
	         							if(Variables.assType.equalsIgnoreCase("3"))
		      								arr_crc_2[Variables.get_index(k)]= Integer.parseInt((String)Variables.col_vals[Variables.get_index(k)]);
		      							
	         							else
	         							arr_crc_2[Variables.get_index(k)]=Integer.parseInt(Variables.val);
	         							count++;
	         							}
	         					}
	                        }
	                       
	                       
	                       else if(Variables.col_ref_operator.equalsIgnoreCase("=")) 
	        					
	                       {
	                           
	         					for(int k=0;k<cols.length;k++)
	         					{
	         						if((int)arr_crc_2[Variables.get_index(k)] != Integer.parseInt(a[Variables.get_index(k)]))
	         							{
	         							if(Variables.assType.equalsIgnoreCase("3"))
		      								arr_crc_2[Variables.get_index(k)]= Integer.parseInt((String)Variables.col_vals[Variables.get_index(k)]);
		      							
	         							else
	         							arr_crc_2[Variables.get_index(k)]=Integer.parseInt(Variables.val);
	         							count++;
	         							}
	         					}
	                        }
	                       
	                       
	                       else if(Variables.col_ref_operator.equalsIgnoreCase("<>")) 
	       					
	                       {
	                           
	         					for(int k=0;k<cols.length;k++)
	         					{
	         						if((int)arr_crc_2[Variables.get_index(k)] == Integer.parseInt(a[Variables.get_index(k)]))
	         							{
	         							if(Variables.assType.equalsIgnoreCase("3"))
		      								arr_crc_2[Variables.get_index(k)]= Integer.parseInt((String)Variables.col_vals[Variables.get_index(k)]);
		      							
	         							else
	         							arr_crc_2[Variables.get_index(k)]=Integer.parseInt(Variables.val);
	         							count++;
	         							}
	         					}
	                        }
	                       
	      					
	      					
	      					 System.out.println(count + " no. of rows have been updated");
	      					 System.out.println(arr_crc_2[0]+ " " +arr_crc_2[1]+ " " +arr_crc_2[2]+ " " +arr_crc_2[3]+ " " +arr_crc_2[4]);
	      					 if (Variables.noofcheck.equalsIgnoreCase("single"))
	      					 { TestResults.test_results(arr_crc_2);}
	                }
					
					
					
					
					
	                
					else if(Variables.col_ref_name.startsWith("D_"))
	                {
						
						
						DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
						DateFormat df1 = new SimpleDateFormat("MM/dd/yyyy");
					
						Date[] arr_crc_3 = { df.parse((String) Variables.a1), df.parse((String) Variables.a2), df.parse((String) Variables.a3), df.parse((String) Variables.a4), df.parse((String) Variables.a5)};
						
						Date[] a= new Date[Variables.pk.length];
						 
						//#####
						

						ArrayList <String> list1 = new ArrayList<String>();
	       				String sql2="";
	       				conn = dbConnect.getConnect( dbuser , dbpassword);
	       				Statement st;
	       				ResultSet rs ;
	       				
	       				if((Variables.global!=null)&&(Variables.local!=null))
	       				{sql2 = "select " + Variables.primaryKey + " from " + Variables.table + " where " + Variables.MIScolumn + " = to_date('" + Variables.MIS_date + "', 'dd-MM-yyyy') and " + Variables.global + " and " + Variables.local ;
	       				
	       				}
	       				if((Variables.global!=null)&&(Variables.local==null))
	       				sql2 = "select " + Variables.primaryKey + " from " + Variables.table + " where " + Variables.MIScolumn + " = to_date('" + Variables.MIS_date + "', 'dd-MM-yyyy') and " + Variables.global ;	
	       					
	       				if((Variables.global==null)&&(Variables.local!=null))
	       				sql2 = "select " + Variables.primaryKey + " from " + Variables.table + " where " + Variables.MIScolumn + " = to_date('" + Variables.MIS_date + "', 'dd-MM-yyyy') and " + Variables.local ;	
	       					
	       				try {
	       					 st = conn.createStatement();
	       					 rs = st.executeQuery(sql2);
	       					
	       					while (rs.next()) {
	       						 
	       						list1.add(rs.getString(1));
	       					 } 
	       					
	       				 } 
	       				catch (SQLException e) {
	       					e.printStackTrace();
	       				}
	       				
	       				
	       				Variables.pk_filt = new String[list1.size()];
	       				
	       				for(int k=0;k<Variables.pk_filt.length;k++)
	       				{
	       					Variables.pk_filt[k]=list1.get(k);
	       					System.out.println(Variables.pk_filt[k]);
	       				}
	       				
	       				
	       				String[] cols = new String[list1.size()];
	       				for(int i =0; i<cols.length;i++)
	       				{
	       				String	query_1 ="select " + Variables.column + " from " + Variables.table + " where " + Variables.primaryKey + " = '" + Variables.pk_filt[i] + "'";				
	       				System.out.println(query_1);
	       				try {
	       					st = conn.createStatement();
	       					 rs = st.executeQuery(query_1);
	       						
	       						while (rs.next()) {
	       							 
	       							cols[i]=rs.getString(1);
	       							System.out.println(cols[i]);
	       						 } 
	       					
	       				} catch (SQLException e) {
	       					e.printStackTrace();
	       				}
	       				
	       				}
						
						
						//######
						
						String sql= "";
						for(int i=0;i<Variables.pk.length;i++) 
		                  {
		                	  sql="select "+Variables.col_ref_name+" from "+Variables.table+" where "+Variables.MIScolumn+" = to_date('" + Variables.MIS_date + "', 'dd-MM-yyyy') and "+Variables.primaryKey+" = '"+Variables.pk[i] + "'";
		                	  System.out.println(sql);
		                      
		                      //System.out.println(Variables.AtomPassword + " " + Variables.atomUser);
		                      conn = dbConnect.getConnect(Variables.atomUser,Variables.AtomPassword);
		                      System.out.println("Connection established");
		                      
		                          
									try {
										st = conn.createStatement();
										 rs = st.executeQuery(sql);
			                            while(rs.next())
			                            {
			                         	   a[i]= df.parse(rs.getString(1));
			                         	   System.out.println(a[i]);
			                            }
									} catch (SQLException e) {
										// TODO Auto-generated catch block
										e.printStackTrace();
									}
		                            
		                  }

	                       if(Variables.col_ref_operator.equalsIgnoreCase(">"))
	                       {
	                       
	      					for(int k=0;k<cols.length;k++)
	      					{
	      						      						
	      						if(arr_crc_3[Variables.get_index(k)].compareTo(a[Variables.get_index(k)])<1)
	      							{
	      							
	      							if(Variables.assType.equalsIgnoreCase("3"))
	      								arr_crc_3[Variables.get_index(k)]= df.parse((String)Variables.col_vals[Variables.get_index(k)]);
	      							
         							else
	      							arr_crc_3[Variables.get_index(k)]=df1.parse(Variables.val);
	      							
	      							count++;
	      							}
	      					}
	                       }
	                      
	                       else if(Variables.col_ref_operator.equalsIgnoreCase("<"))
	                       {
	                       
	      					for(int k=0;k<cols.length;k++)
	      					{
	      						      						
	      						if(arr_crc_3[Variables.get_index(k)].compareTo(a[Variables.get_index(k)])>-1)
	      							{
	      							if(Variables.assType.equalsIgnoreCase("3"))
	      								arr_crc_3[Variables.get_index(k)]= df.parse((String)Variables.col_vals[Variables.get_index(k)]);
	      							
         							else
	      							arr_crc_3[Variables.get_index(k)]=df1.parse(Variables.val);
	      							count++;
	      							}
	      					}
	                       }
	                       
	                       else if(Variables.col_ref_operator.equalsIgnoreCase(">="))
	                       {
	                       
	      					for(int k=0;k<cols.length;k++)
	      					{
	      						      						
	      						if(arr_crc_3[Variables.get_index(k)].compareTo(a[Variables.get_index(k)])<0)
	      						{
	      							if(Variables.assType.equalsIgnoreCase("3"))
	      								arr_crc_3[Variables.get_index(k)]= df.parse((String)Variables.col_vals[Variables.get_index(k)]);
	      							
         							else
	      							arr_crc_3[Variables.get_index(k)]=df1.parse(Variables.val);
	      							count++;
	      							}
	      					}
	                       }
	                       
	                       else if(Variables.col_ref_operator.equalsIgnoreCase("<="))
	                       {
	                       
	      					for(int k=0;k<cols.length;k++)
	      					{
	      						      						
	      						if(arr_crc_3[Variables.get_index(k)].compareTo(a[Variables.get_index(k)])>0)
	      						{
	      							if(Variables.assType.equalsIgnoreCase("3"))
	      								arr_crc_3[Variables.get_index(k)]= df.parse((String)Variables.col_vals[Variables.get_index(k)]);
	      							
         							else
	      							arr_crc_3[Variables.get_index(k)]=df1.parse(Variables.val);
	      							count++;
	      							}
	      					}
	                       }
	                       
	                       else if(Variables.col_ref_operator.equalsIgnoreCase("<>"))
	                       {
	                       
	      					for(int k=0;k<cols.length;k++)
	      					{
	      						      						
	      						if(arr_crc_3[Variables.get_index(k)].compareTo(a[Variables.get_index(k)])==0)
	      						{
	      							if(Variables.assType.equalsIgnoreCase("3"))
	      								arr_crc_3[Variables.get_index(k)]= df.parse((String)Variables.col_vals[Variables.get_index(k)]);
	      							
         							else
	      							arr_crc_3[Variables.get_index(k)]=df1.parse(Variables.val);
	      							count++;
	      							}
	      					}
	                       }
						
	                       
	                       else if(Variables.col_ref_operator.equalsIgnoreCase("="))
	                       {
	                       
	      					for(int k=0;k<cols.length;k++)
	      					{
	      						      						
	      						if(arr_crc_3[Variables.get_index(k)].compareTo(a[Variables.get_index(k)])!=0)
	      						{
	      							if(Variables.assType.equalsIgnoreCase("3"))
	      								arr_crc_3[Variables.get_index(k)]= df.parse((String)Variables.col_vals[Variables.get_index(k)]);
	      							
         							else
	      							arr_crc_3[Variables.get_index(k)]=df1.parse(Variables.val);
	      							count++;
	      							}
	      					}
	                       }
	                       
	                    System.out.println( count + " rows have been updated");
	                    System.out.println(df1.format(arr_crc_3[0]) + " " + df1.format(arr_crc_3[1]) + " " + df1.format(arr_crc_3[2])  + " " + df1.format(arr_crc_3[3]) + " " + df1.format(arr_crc_3[4]));
						
	   					String[] arr_crc_3_s = {df1.format(arr_crc_3[0]),df1.format(arr_crc_3[1]),df1.format(arr_crc_3[2]),df1.format(arr_crc_3[3]),df1.format(arr_crc_3[4])};
	   					
	   				 if (Variables.noofcheck.equalsIgnoreCase("single"))
	   				 {TestResults.test_results(arr_crc_3_s);}
						
						
	                }
					
					
					
	            }
				
				
				else
	            {
					
					if(Variables.col_ref_name.contains("/")||Variables.col_ref_name.contains("-"))
	                {
						
						
						DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
						DateFormat df1 = new SimpleDateFormat("MM/dd/yyyy");
					
						Date[] arr_crc_4 = { df.parse((String) Variables.a1), df.parse((String) Variables.a2), df.parse((String) Variables.a3), df.parse((String) Variables.a4), df.parse((String) Variables.a5)};
					
						
						
						//#####


						ArrayList <String> list1 = new ArrayList<String>();
	       				String sql2="";
	       				conn = dbConnect.getConnect( dbuser , dbpassword);
	       				Statement st;
	       				ResultSet rs ;
	       				
	       				if((Variables.global!=null)&&(Variables.local!=null))
	       				{sql2 = "select " + Variables.primaryKey + " from " + Variables.table + " where " + Variables.MIScolumn + " = to_date('" + Variables.MIS_date + "', 'dd-MM-yyyy') and " + Variables.global + " and " + Variables.local ;
	       				System.out.println("qweeeryyy");
	       				}
	       				if((Variables.global!=null)&&(Variables.local==null))
	       				sql2 = "select " + Variables.primaryKey + " from " + Variables.table + " where " + Variables.MIScolumn + " = to_date('" + Variables.MIS_date + "', 'dd-MM-yyyy') and " + Variables.global ;	
	       					
	       				if((Variables.global==null)&&(Variables.local!=null))
	       				sql2 = "select " + Variables.primaryKey + " from " + Variables.table + " where " + Variables.MIScolumn + " = to_date('" + Variables.MIS_date + "', 'dd-MM-yyyy') and " + Variables.local ;	
	       					
	       				try {
	       					 st = conn.createStatement();
	       					 rs = st.executeQuery(sql2);
	       					
	       					while (rs.next()) {
	       						 
	       						list1.add(rs.getString(1));
	       					 } 
	       					
	       				 } 
	       				catch (SQLException e) {
	       					e.printStackTrace();
	       				}
	       				
	       				
	       				Variables.pk_filt = new String[list1.size()];
	       				
	       				for(int k=0;k<Variables.pk_filt.length;k++)
	       				{
	       					Variables.pk_filt[k]=list1.get(k);
	       					System.out.println(Variables.pk_filt[k]);
	       				}
	       				
	       				
	       				String[] cols = new String[list1.size()];
	       				for(int i =0; i<cols.length;i++)
	       				{
	       				String	query_1 ="select " + Variables.column + " from " + Variables.table + " where " + Variables.primaryKey + " = '" + Variables.pk_filt[i] + "'";				
	       				
	       				try {
	       					st = conn.createStatement();
	       					 rs = st.executeQuery(query_1);
	       						
	       						while (rs.next()) {
	       							 
	       							cols[i]=rs.getString(1);
	       							System.out.println(cols[i]);
	       						 } 
	       					
	       				} catch (SQLException e) {
	       					e.printStackTrace();
	       				}
	       				
	       				}
						
						
						//#####
						
						
						
						
						if(Variables.col_ref_operator.equalsIgnoreCase(">"))
	                    {
	                    
	   					for(int k=0;k<cols.length;k++)
	   					{
	   						System.out.println("intooo");
	   						System.out.println(Variables.col_vals[Variables.get_index(k)]);
	   						if(arr_crc_4[Variables.get_index(k)].compareTo(df1.parse(Variables.col_ref_name))<1)
	   							{
	   							if(Variables.assType.equalsIgnoreCase("3"))
      								arr_crc_4[Variables.get_index(k)]= df.parse((String)Variables.col_vals[Variables.get_index(k)]);
      							
     							else
	   							arr_crc_4[Variables.get_index(k)] = df1.parse(Variables.val);
	   							
	   							count++;
	   							}
	   					}
	                    }
						
						else if(Variables.col_ref_operator.equalsIgnoreCase("<"))
	                    {
	                    
	   					for(int k=0;k<cols.length;k++)
	   					{
	   						      						
	   						if(arr_crc_4[Variables.get_index(k)].compareTo(df1.parse(Variables.col_ref_name))>-1)
	   							{
	   							if(Variables.assType.equalsIgnoreCase("3"))
      								arr_crc_4[Variables.get_index(k)]= df.parse((String)Variables.col_vals[Variables.get_index(k)]);
      							
     							else
	   							arr_crc_4[Variables.get_index(k)] = df1.parse(Variables.val);
	   							
	   							count++;
	   							}
	   					}
	                    }
						
						
						if(Variables.col_ref_operator.equalsIgnoreCase(">="))
	                    {
	                    
	   					for(int k=0;k<cols.length;k++)
	   					{
	   						      						
	   						if(arr_crc_4[Variables.get_index(k)].compareTo(df1.parse(Variables.col_ref_name))<0)
	   							{
	   							if(Variables.assType.equalsIgnoreCase("3"))
      								arr_crc_4[Variables.get_index(k)]= df.parse((String)Variables.col_vals[Variables.get_index(k)]);
      							
     							else
	   							arr_crc_4[Variables.get_index(k)] = df1.parse(Variables.val);
	   							
	   							count++;
	   							}
	   					}
	                    }
						
						
						if(Variables.col_ref_operator.equalsIgnoreCase("<="))
	                    {
	                    
	   					for(int k=0;k<cols.length;k++)
	   					{
	   						      						
	   						if(arr_crc_4[Variables.get_index(k)].compareTo(df1.parse(Variables.col_ref_name))>0)
	   							{
	   							if(Variables.assType.equalsIgnoreCase("3"))
      								arr_crc_4[Variables.get_index(k)]= df.parse((String)Variables.col_vals[Variables.get_index(k)]);
      							
     							else
	   							arr_crc_4[Variables.get_index(k)] = df1.parse(Variables.val);
	   							
	   							count++;
	   							}
	   					}
	                    }
						
						
						
						if(Variables.col_ref_operator.equalsIgnoreCase("<>"))
	                    {
	                    
	   					for(int k=0;k<cols.length;k++)
	   					{
	   						      						
	   						if(arr_crc_4[Variables.get_index(k)].compareTo(df1.parse(Variables.col_ref_name))==0)
	   							{
	   							if(Variables.assType.equalsIgnoreCase("3"))
      								arr_crc_4[Variables.get_index(k)]= df.parse((String)Variables.col_vals[Variables.get_index(k)]);
      							
     							else
	   							arr_crc_4[Variables.get_index(k)] = df1.parse(Variables.val);
	   							
	   							count++;
	   							}
	   					}
	                    }
						
						
						if(Variables.col_ref_operator.equalsIgnoreCase("="))
	                    {
	                    
	   					for(int k=0;k<cols.length;k++)
	   					{
	   						      						
	   						if(arr_crc_4[Variables.get_index(k)].compareTo(df1.parse(Variables.col_ref_name))!=0)
	   							{
	   							if(Variables.assType.equalsIgnoreCase("3"))
      								arr_crc_4[Variables.get_index(k)]= df.parse((String)Variables.col_vals[Variables.get_index(k)]);
      							
     							else
	   							arr_crc_4[Variables.get_index(k)] = df1.parse(Variables.val);
	   							
	   							count++;
	   							}
	   					}
	                    }
						
						
						System.out.println( count + " rows have been updated");
	   					System.out.println(df1.format(arr_crc_4[0]) + " " + df1.format(arr_crc_4[1]) + " " + df1.format(arr_crc_4[2])  + " " + df1.format(arr_crc_4[3]) + " " + df1.format(arr_crc_4[4]));
						
	   					String[] arr_crc_4_s = {df1.format(arr_crc_4[0]),df1.format(arr_crc_4[1]),df1.format(arr_crc_4[2]),df1.format(arr_crc_4[3]),df1.format(arr_crc_4[4])};
	   					
	   				 if (Variables.noofcheck.equalsIgnoreCase("single"))
	   				 {TestResults.test_results(arr_crc_4_s);}
						
	                }
					
					else
	                {
						Object[] arr_crc_1 = {Variables.a1, Variables.a2, Variables.a3, Variables.a4,Variables.a5};
						
						//###########
						
						ArrayList <String> list1 = new ArrayList<String>();
	       				String sql2="";
	       				conn = dbConnect.getConnect( dbuser , dbpassword);
	       				Statement st;
	       				ResultSet rs ;
	       				
	       				if((Variables.global!=null)&&(Variables.local!=null))
	       				{sql2 = "select " + Variables.primaryKey + " from " + Variables.table + " where " + Variables.MIScolumn + " = to_date('" + Variables.MIS_date + "', 'dd-MM-yyyy') and " + Variables.global + " and " + Variables.local ;
	       				System.out.println("qweeeryyy");
	       				}
	       				if((Variables.global!=null)&&(Variables.local==null))
	       				sql2 = "select " + Variables.primaryKey + " from " + Variables.table + " where " + Variables.MIScolumn + " = to_date('" + Variables.MIS_date + "', 'dd-MM-yyyy') and " + Variables.global ;	
	       					
	       				if((Variables.global==null)&&(Variables.local!=null))
	       				sql2 = "select " + Variables.primaryKey + " from " + Variables.table + " where " + Variables.MIScolumn + " = to_date('" + Variables.MIS_date + "', 'dd-MM-yyyy') and " + Variables.local ;	
	       					
	       				try {
	       					 st = conn.createStatement();
	       					 rs = st.executeQuery(sql2);
	       					
	       					while (rs.next()) {
	       						 
	       						list1.add(rs.getString(1));
	       					 } 
	       					
	       				 } 
	       				catch (SQLException e) {
	       					e.printStackTrace();
	       				}
	       				
	       				
	       				Variables.pk_filt = new String[list1.size()];
	       				
	       				for(int k=0;k<Variables.pk_filt.length;k++)
	       				{
	       					Variables.pk_filt[k]=list1.get(k);
	       					System.out.println(Variables.pk_filt[k]);
	       				}
	       				
	       				
	       				int[] cols = new int[list1.size()];
	       				for(int i =0; i<cols.length;i++)
	       				{
	       				String	query_1 ="select " + Variables.column + " from " + Variables.table + " where " + Variables.primaryKey + " = '" + Variables.pk_filt[i] + "'";				
	       				
	       				try {
	       					st = conn.createStatement();
	       					 rs = st.executeQuery(query_1);
	       						
	       						while (rs.next()) {
	       							 
	       							cols[i]=rs.getInt(1);
	       							System.out.println(cols[i]);
	       						 } 
	       					
	       				} catch (SQLException e) {
	       					e.printStackTrace();
	       				}
	       				
	       				}
						
						
						
						//###########
						
						
						
						
						if(Variables.col_ref_operator.equalsIgnoreCase("<"))
							
						{
							for(int i =0; i<cols.length;i++)
						{
							
							if((int)arr_crc_1[Variables.get_index(i)] >= Integer.parseInt(Variables.col_ref_name))
								{
								if(Variables.assType.equalsIgnoreCase("3"))
      								arr_crc_1[Variables.get_index(i)]= Integer.parseInt((String)Variables.col_vals[Variables.get_index(i)]);
      							
     							else
								arr_crc_1[Variables.get_index(i)]= Integer.parseInt(Variables.val);
								count++;
								}
						}
						
						}
	                
						else if(Variables.col_ref_operator.equalsIgnoreCase(">"))
							
						{
							for(int i =0; i<cols.length;i++)
						{
							System.out.println((int)arr_crc_1[Variables.get_index(i)] + " " + Integer.parseInt(Variables.col_ref_name));
							if((int)arr_crc_1[Variables.get_index(i)] <= Integer.parseInt(Variables.col_ref_name))
								{
								if(Variables.assType.equalsIgnoreCase("3"))
      								arr_crc_1[Variables.get_index(i)]= Integer.parseInt((String)Variables.col_vals[Variables.get_index(i)]);
      							
     							else
								arr_crc_1[Variables.get_index(i)]= Integer.parseInt(Variables.val);
								count++;
								}
						}
						
						}
	                
	                
						else if(Variables.col_ref_operator.equalsIgnoreCase("<="))
							
						{
							for(int i =0; i<cols.length;i++)
						{
							
							if((int)arr_crc_1[Variables.get_index(i)] > Integer.parseInt(Variables.col_ref_name))
								{
								if(Variables.assType.equalsIgnoreCase("3"))
      								arr_crc_1[Variables.get_index(i)]= Integer.parseInt((String)Variables.col_vals[Variables.get_index(i)]);
      							
     							else
								arr_crc_1[Variables.get_index(i)]= Integer.parseInt(Variables.val);
								count++;
								}
						}
						
						}
						
						else if(Variables.col_ref_operator.equalsIgnoreCase(">="))
							
						{
							for(int i =0; i<cols.length;i++)
						{
							
							if((int)arr_crc_1[Variables.get_index(i)] < Integer.parseInt(Variables.col_ref_name))
								{
								
								if(Variables.assType.equalsIgnoreCase("3"))
      								arr_crc_1[Variables.get_index(i)]= Integer.parseInt((String)Variables.col_vals[Variables.get_index(i)]);
      							
     							else
								arr_crc_1[Variables.get_index(i)]= Integer.parseInt(Variables.val);
								count++;
								}
						}
						
						}
						
						
						else if(Variables.col_ref_operator.equalsIgnoreCase("<>"))
							
						{
							for(int i =0; i<cols.length;i++)
						{
							
							if((int)arr_crc_1[Variables.get_index(i)] == Integer.parseInt(Variables.col_ref_name))
								{
								if(Variables.assType.equalsIgnoreCase("3"))
      								arr_crc_1[Variables.get_index(i)]= Integer.parseInt((String)Variables.col_vals[Variables.get_index(i)]);
      							
     							else
								arr_crc_1[Variables.get_index(i)]= Integer.parseInt(Variables.val);
								count++;
								}
						}
						
						}
						
						else if(Variables.col_ref_operator.equalsIgnoreCase("="))
							
						{
							for(int i =0; i<cols.length;i++)
								
						{
							
							if((int)arr_crc_1[Variables.get_index(i)] != Integer.parseInt(Variables.col_ref_name))
								{
								if(Variables.assType.equalsIgnoreCase("3"))
      								arr_crc_1[Variables.get_index(i)]= Integer.parseInt((String)Variables.col_vals[Variables.get_index(i)]);
      							
     							else
								arr_crc_1[Variables.get_index(i)]= Integer.parseInt(Variables.val);
								count++;
								}
						}
						
						}
	                
	                
						System.out.println( count + " rows have been updated");
						System.out.println(arr_crc_1[0] + " " + arr_crc_1[1] + " " + arr_crc_1[2] + " " + arr_crc_1[3] + " " + arr_crc_1[4]);
						 if (Variables.noofcheck.equalsIgnoreCase("single"))
						 {TestResults.test_results(arr_crc_1);}
	                }
					
	            }
				
				
				
			}
			
			break;
			
		case ISNULL:
	
			Variables.get_val(DQ_Name,3);
			Variables.assType();

			DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
			DateFormat df1 = new SimpleDateFormat("MM/dd/yyyy");
			
			
			if((Variables.global==null)&&(Variables.local==null))
			{
				Object[]  arr_null = {Variables.a1, Variables.a2, Variables.a3, Variables.a4, Variables.a5};
			
			
				if(Variables.column.startsWith("N_")||Variables.column.startsWith("n_")){
				
				for(int i=0; i<5; i++)
			{
				
				if(arr_null[i]==null)
					{
					if(Variables.assType.equalsIgnoreCase("3"))
						arr_null[i]= Integer.parseInt((String)Variables.col_vals[i]);
					
					else
					arr_null[i]= Integer.parseInt((String)Variables.val);
					
					count++;
					}
				 Variables.mul_upd_null.add(arr_null[i]);
				
			}
				}
			
				else if(Variables.column.startsWith("V_")||Variables.column.startsWith("v_")){

					
					for(int i=0; i<5; i++)
				{
					
					if(arr_null[i]==null)
						{
						if(Variables.assType.equalsIgnoreCase("3"))
							arr_null[i]= Variables.col_vals[i];
						
						else
						arr_null[i]= Variables.val;
						
						count++;
						}
					 Variables.mul_upd_null.add(arr_null[i]);
					
				}
					
				}
				
				
				else if(Variables.column.startsWith("D_")||Variables.column.startsWith("d_")){
					
					arr_null[3] = df1.format(df.parse((String)Variables.a4));
					arr_null[4] = df1.format(df.parse((String)Variables.a5));
				
					
					for(int i=0; i<5; i++)
				{
					
					if(arr_null[i]==null)
						{
						if(Variables.assType.equalsIgnoreCase("3"))
							arr_null[i]= df1.format(df.parse((String)Variables.col_vals[i]));
						
						else
						arr_null[i]= Variables.val;
						
						count++;
						}
					 Variables.mul_upd_null.add(arr_null[i]);
					
				}
					
					
					
				}
				
			System.out.println(count + " rows have been updated");
			System.out.println(arr_null[0] + " " +arr_null[1] + " " +arr_null[2] + " " +arr_null[3] + " " +arr_null[4]);
			if (Variables.noofcheck.equalsIgnoreCase("single"))
			{
			TestResults.test_results(arr_null);
			}
			}
			
			else
			{
				Object[]  arr_null = {Variables.a1, Variables.a2, Variables.a3, Variables.a4, Variables.a5};
				ArrayList <String> list1 = new ArrayList<String>();
				String sql2="";
				conn = dbConnect.getConnect( dbuser , dbpassword);
				Statement st;
				ResultSet rs ;
				
				
			
				if((Variables.global!=null)&&(Variables.local!=null))
				sql2 = "select " + Variables.primaryKey + " from " + Variables.table + " where " + Variables.MIScolumn + " = to_date('" + Variables.MIS_date + "', 'dd-MM-yyyy') and " + Variables.global + " and " + Variables.local ;
				
				if((Variables.global!=null)&&(Variables.local==null))
				sql2 = "select " + Variables.primaryKey + " from " + Variables.table + " where " + Variables.MIScolumn + " = to_date('" + Variables.MIS_date + "', 'dd-MM-yyyy') and " + Variables.global ;	
					
				if((Variables.global==null)&&(Variables.local!=null))
				sql2 = "select " + Variables.primaryKey + " from " + Variables.table + " where " + Variables.MIScolumn + " = to_date('" + Variables.MIS_date + "', 'dd-MM-yyyy') and " + Variables.local ;	
					
				try {
					 st = conn.createStatement();
					 rs = st.executeQuery(sql2);
					
					while (rs.next()) {
						 
						list1.add(rs.getString(1));
					 } 
					
				 } 
				catch (SQLException e) {
					e.printStackTrace();
				}
				
				
				Variables.pk_filt = new String[list1.size()];
				
				for(int k=0;k<Variables.pk_filt.length;k++)
				{
					Variables.pk_filt[k]=list1.get(k);
					System.out.println(Variables.pk_filt[k]);
				}
				
				
				int[] cols = new int[list1.size()];
				for(int i =0; i<cols.length;i++)
				{
				String	query_1 ="select " + Variables.column + " from " + Variables.table + " where " + Variables.primaryKey + " = '" + Variables.pk_filt[i] + "'";				
				
				try {
					st = conn.createStatement();
					 rs = st.executeQuery(query_1);
						
						while (rs.next()) {
							 
							cols[i]=rs.getInt(1);
							
						 } 
					
				} catch (SQLException e) {
					e.printStackTrace();
				}
				
				}
				
				if(Variables.column.startsWith("N_")||Variables.column.startsWith("n_")){
				
				for(int i=0; i<cols.length; i++)
				{
					
					
					if(arr_null[Variables.get_index(i)]==null)
						{
						if(Variables.assType.equalsIgnoreCase("3"))
						arr_null[Variables.get_index(i)]= Integer.parseInt((String)Variables.col_vals[Variables.get_index(i)]);
						
					else
						arr_null[Variables.get_index(i)]= Integer.parseInt((String)Variables.val);
						
						count++;
						}
					
					
				}
				}
				
				

				else if(Variables.column.startsWith("V_")||Variables.column.startsWith("v_")){

					
					for(int i=0; i<cols.length; i++)
					{
						
						
						if(arr_null[Variables.get_index(i)]==null)
							{
							if(Variables.assType.equalsIgnoreCase("3"))
							arr_null[Variables.get_index(i)]= Variables.col_vals[Variables.get_index(i)];
							
						else
							arr_null[Variables.get_index(i)]= Variables.val;
							
							count++;
							}
						
						
					}
					
				}
				
				
				else if(Variables.column.startsWith("D_")||Variables.column.startsWith("d_")){
					
					arr_null[3] = df1.format(df.parse((String)Variables.a4));
					arr_null[4] = df1.format(df.parse((String)Variables.a5));
				
					
					for(int i=0; i<cols.length; i++)
					{
						
						
						if(arr_null[Variables.get_index(i)]==null)
							{
							if(Variables.assType.equalsIgnoreCase("3"))
							arr_null[Variables.get_index(i)]= df1.format(df.parse((String)Variables.col_vals[Variables.get_index(i)]));
							
						else
							arr_null[Variables.get_index(i)]= Variables.val;
							
							count++;
							}
						
						
					}
					
					
					
				}
				
				
				System.out.println(count + " rows have been updated");
				System.out.println(arr_null[0] + " " +arr_null[1] + " " +arr_null[2] + " " +arr_null[3] + " " +arr_null[4]);
				TestResults.test_results(arr_null);
			}
			
			
			
			
			break;
		
			
		case LOV:
			
			Variables.get_val(DQ_Name,5);
			Variables.assType();

			
			if((Variables.global==null)&&(Variables.local==null))
			{
			
			 Object[]  arr_lov = {Variables.a1, Variables.a2, Variables.a3, Variables.a4, Variables.a5};
			  
			 String[] list=Variables.lov_value.split(",");
              String[] values=new String[3];
              for(int k=0;(k<list.length && k<3);k++)
              {
                    values[k]=list[k];
              }
			
			 if(Variables.column.startsWith("V_"))
			
			 {
				 
				 for(int i=0; i<arr_lov.length; i++)
				 {
					
					if(!(arr_lov[i].equals(values[0])||arr_lov[i].equals(values[1])||arr_lov[i].equals(values[2]))) 
				
						{
						if(Variables.assType.equalsIgnoreCase("3"))
							arr_lov[i]= Variables.col_vals[i];
						else
						arr_lov[i]= Variables.val;
						count++;
						}
					 Variables.mul_upd_lov.add(arr_lov[i]);
				 }
				 

					System.out.println(count + " rows have been updated");
					System.out.println(arr_lov[0] + " " +arr_lov[1] + " " +arr_lov[2] + " " +arr_lov[3] + " " +arr_lov[4]);
					if (Variables.noofcheck.equalsIgnoreCase("single"))
					 { 
					TestResults.test_results(arr_lov);
					 }
			
			 } 
			 
			 if(Variables.column.startsWith("N_")){
				 

				 
				 for(int i=0; i<arr_lov.length; i++)
				 {
					
					 if(list.length==1){
							if(!((int)arr_lov[i]==Integer.parseInt(values[0]))) 
						
								{
								if(Variables.assType.equalsIgnoreCase("3"))
									arr_lov[i]= Integer.parseInt((String)Variables.col_vals[i]);
								else
								arr_lov[i]= Integer.parseInt(Variables.val);
								count++;
								}
							 }
					 
					if(list.length==2){
					if(!((int)arr_lov[i]==Integer.parseInt(values[0])||(int)arr_lov[i]==Integer.parseInt(values[1]))) 
				
						{
						if(Variables.assType.equalsIgnoreCase("3"))
							arr_lov[i]= Integer.parseInt((String)Variables.col_vals[i]);
						else
						arr_lov[i]= Integer.parseInt(Variables.val);
						count++;
						}
					 }
					
					if(list.length==3){
						if(!((int)arr_lov[i]==Integer.parseInt(values[0])||(int)arr_lov[i]==Integer.parseInt(values[1])||(int)arr_lov[i]==Integer.parseInt(values[2]))) 
					
							{
							if(Variables.assType.equalsIgnoreCase("3"))
								arr_lov[i]= Integer.parseInt((String)Variables.col_vals[i]);
							else
							arr_lov[i]= Integer.parseInt(Variables.val);
							count++;
							}
						 }
					
					 Variables.mul_upd_lov.add(arr_lov[i]);
				 }
				 
				
				 	System.out.println(count + " rows have been updated");
					System.out.println(arr_lov[0] + " " +arr_lov[1] + " " +arr_lov[2] + " " +arr_lov[3] + " " +arr_lov[4]);
					if (Variables.noofcheck.equalsIgnoreCase("single"))
					 { 
					TestResults.test_results(arr_lov);
					 }
			 
			 }
			 
			 if(Variables.column.startsWith("D_"))
			 {
				 
				 
				 
				 if(list.length==1)
				 {
					 
					 Date startDate;
					 DateFormat formatter = new SimpleDateFormat("MM/dd/yyyy"); 
	                 SimpleDateFormat dmy= new SimpleDateFormat("yyyy-MM-dd");
	                 Calendar cal = Calendar.getInstance();
	                 
	                 startDate = (Date) formatter.parse(values[0]);
	                 cal.setTime(startDate);
	                 values[0]=dmy.format(cal.getTime());
	                 
	                 
	                 for(int i=0; i<arr_lov.length; i++)
					 {
	                	 
						
						if(!(arr_lov[i].equals(values[0]))) 
					
							{
							if(Variables.assType.equalsIgnoreCase("3"))
								arr_lov[i]= Variables.col_vals[i];
							
							else
							arr_lov[i]= Variables.val;
							count++;
							}
						
					 } 
				 }
				
				 else if(list.length==2)
				 {
					 
					 Date startDate;
					 DateFormat formatter = new SimpleDateFormat("MM/dd/yyyy"); 
	                 SimpleDateFormat dmy= new SimpleDateFormat("yyyy-MM-dd");
	                 Calendar cal = Calendar.getInstance();
	                 
	                 startDate = (Date) formatter.parse(values[0]);
	                 cal.setTime(startDate);
	                 values[0]=dmy.format(cal.getTime());
	                 
	                 
	                 startDate = (Date) formatter.parse(values[1]);
	                 cal.setTime(startDate);
	                 values[1]=dmy.format(cal.getTime());
					 
					 for(int i=0; i<arr_lov.length; i++)
					 {
						
						if(!(arr_lov[i].equals(values[0])||arr_lov[i].equals(values[1]))) 
					
							{
							if(Variables.assType.equalsIgnoreCase("3"))
								arr_lov[i]= Variables.col_vals[i];
							
							else
							arr_lov[i]= Variables.val;
							count++;
							}
						
					 } 
					 
					 
				 }
				 
				 
				
				 
				 else 
				 {
					 
				 Date startDate;
				 DateFormat formatter = new SimpleDateFormat("MM/dd/yyyy"); 
                 SimpleDateFormat dmy= new SimpleDateFormat("yyyy-MM-dd");
                 Calendar cal = Calendar.getInstance();
                 
                 startDate = (Date) formatter.parse(values[0]);
                 cal.setTime(startDate);
                 values[0]=dmy.format(cal.getTime());
                 
                 
                 startDate = (Date) formatter.parse(values[1]);
                 cal.setTime(startDate);
                 values[1]=dmy.format(cal.getTime());
                 
                 startDate = (Date) formatter.parse(values[2]);
                 cal.setTime(startDate);
                 values[2]=dmy.format(cal.getTime());
                 
                 
				 for(int i=0; i<arr_lov.length; i++)
				 {
					 
					if(!(arr_lov[i].equals(values[0])||arr_lov[i].equals(values[1])||arr_lov[i].equals(values[2]))) 
				
						{
						if(Variables.assType.equalsIgnoreCase("3"))
							arr_lov[i]= Variables.col_vals[i];
						
						else
						arr_lov[i]= Variables.val;
						count++;
						}
					
				 } 
				 
				 }
				 

					System.out.println(count + " rows have been updated");
					System.out.println(arr_lov[0] + " " +arr_lov[1] + " " +arr_lov[2] + " " +arr_lov[3] + " " +arr_lov[4]);
					if (Variables.noofcheck.equalsIgnoreCase("single"))
					 { 
					TestResults.test_results_error(count, DQ_Name);
					 }
			 }
			
			 
			
			}
			
			
			else
			
			{
				
				Object[]  arr_lov = {Variables.a1, Variables.a2, Variables.a3, Variables.a4, Variables.a5};
				ArrayList <String> list1 = new ArrayList<String>();
				String sql2="";
				conn = dbConnect.getConnect( dbuser , dbpassword);
				Statement st;
				ResultSet rs ;
				
				
			
				if((Variables.global!=null)&&(Variables.local!=null))
				sql2 = "select " + Variables.primaryKey + " from " + Variables.table + " where " + Variables.MIScolumn + " = to_date('" + Variables.MIS_date + "', 'dd-MM-yyyy') and " + Variables.global + " and " + Variables.local ;
				
				if((Variables.global!=null)&&(Variables.local==null))
				sql2 = "select " + Variables.primaryKey + " from " + Variables.table + " where " + Variables.MIScolumn + " = to_date('" + Variables.MIS_date + "', 'dd-MM-yyyy') and " + Variables.global ;	
					
				if((Variables.global==null)&&(Variables.local!=null))
				sql2 = "select " + Variables.primaryKey + " from " + Variables.table + " where " + Variables.MIScolumn + " = to_date('" + Variables.MIS_date + "', 'dd-MM-yyyy') and " + Variables.local ;	
					
				try {
					 st = conn.createStatement();
					 rs = st.executeQuery(sql2);
					
					while (rs.next()) {
						 
						list1.add(rs.getString(1));
					 } 
					
				 } 
				catch (SQLException e) {
					e.printStackTrace();
				}
				
				
				Variables.pk_filt = new String[list1.size()];
				
				for(int k=0;k<Variables.pk_filt.length;k++)
				{
					Variables.pk_filt[k]=list1.get(k);
					System.out.println(Variables.pk_filt[k]);
				}
				
				
				int[] cols = new int[list1.size()];
				for(int i =0; i<cols.length;i++)
				{
				String	query_1 ="select " + Variables.column + " from " + Variables.table + " where " + Variables.primaryKey + " = '" + Variables.pk_filt[i] + "'";				
				
				try {
					st = conn.createStatement();
					 rs = st.executeQuery(query_1);
						
						while (rs.next()) {
							 
							cols[i]=rs.getInt(1);
							
						 } 
					
				} catch (SQLException e) {
					e.printStackTrace();
				}
				
				}
				 
				 String[] list=Variables.lov_value.split(",");
	              String[] values=new String[3];
	              for(int k=0;(k<list.length && k<3);k++)
	              {
	                    values[k]=list[k];
	                    //System.out.println(values[k]);
	              }
				
				 if(Variables.column.startsWith("V_")|| Variables.column.startsWith("N_"))
				
				 {
					 System.out.println("#%$#%#%%"+values[2]);
					 
					 for(int i=0; i<cols.length; i++)
					 {
						
						if(!(arr_lov[Variables.get_index(i)].equals(values[0])||arr_lov[Variables.get_index(i)].equals(values[1])||arr_lov[Variables.get_index(i)].equals(values[2]))) 
					
							{
							if(Variables.assType.equalsIgnoreCase("3"))
								arr_lov[Variables.get_index(i)]= Variables.col_vals[Variables.get_index(i)];	
							else
							arr_lov[Variables.get_index(i)]= Variables.val;
							count++;
							}
						
					 }

						System.out.println(count + " rows have been updated");
						System.out.println(arr_lov[0] + " " +arr_lov[1] + " " +arr_lov[2] + " " +arr_lov[3] + " " +arr_lov[4]);
						if (Variables.noofcheck.equalsIgnoreCase("single"))
						 { 
						TestResults.test_results(arr_lov);
						 }
				 } 
				 
				 
				 if(Variables.column.startsWith("D_"))
				 {
					 
					 if(list.length==1)
					 {
						 
						 Date startDate;
						 DateFormat formatter = new SimpleDateFormat("MM/dd/yyyy"); 
		                 SimpleDateFormat dmy= new SimpleDateFormat("yyyy-MM-dd");
		                 Calendar cal = Calendar.getInstance();
		                 
		                 startDate = (Date) formatter.parse(values[0]);
		                 cal.setTime(startDate);
		                 values[0]=dmy.format(cal.getTime());
		                 
		                 for(int i=0; i<cols.length; i++)
						 {
							
							if(!(arr_lov[Variables.get_index(i)].equals(values[0]))) 
						
								{
								if(Variables.assType.equalsIgnoreCase("3"))
									arr_lov[Variables.get_index(i)]= Variables.col_vals[Variables.get_index(i)];
								else
								arr_lov[Variables.get_index(i)]= Variables.val;
								count++;
								}
							
						 } 
						
						 
					 }
					
					 else if(list.length==2)
					 {
						 
						 Date startDate;
						 DateFormat formatter = new SimpleDateFormat("MM/dd/yyyy"); 
		                 SimpleDateFormat dmy= new SimpleDateFormat("yyyy-MM-dd");
		                 Calendar cal = Calendar.getInstance();
		                 
		                 startDate = (Date) formatter.parse(values[0]);
		                 cal.setTime(startDate);
		                 values[0]=dmy.format(cal.getTime());
		                 
		                 
		                 startDate = (Date) formatter.parse(values[1]);
		                 cal.setTime(startDate);
		                 values[1]=dmy.format(cal.getTime());
						 
						 for(int i=0; i<cols.length; i++)
						 {
							
							if(!(arr_lov[Variables.get_index(i)].equals(values[0])||arr_lov[Variables.get_index(i)].equals(values[1]))) 
						
								{
								if(Variables.assType.equalsIgnoreCase("3"))
									arr_lov[Variables.get_index(i)]= Variables.col_vals[Variables.get_index(i)];
								else
								arr_lov[Variables.get_index(i)]= Variables.val;
								count++;
								}
							
						 } 
						 

					 }
					 
					 
					
					 
					 else 
					 {
						 
					 Date startDate;
					 DateFormat formatter = new SimpleDateFormat("MM/dd/yyyy"); 
	                 SimpleDateFormat dmy= new SimpleDateFormat("yyyy-MM-dd");
	                 Calendar cal = Calendar.getInstance();
	                 
	                 startDate = (Date) formatter.parse(values[0]);
	                 cal.setTime(startDate);
	                 values[0]=dmy.format(cal.getTime());
	                 
	                 
	                 startDate = (Date) formatter.parse(values[1]);
	                 cal.setTime(startDate);
	                 values[1]=dmy.format(cal.getTime());
	                 
	                 startDate = (Date) formatter.parse(values[2]);
	                 cal.setTime(startDate);
	                 values[2]=dmy.format(cal.getTime());
	                 
	                 
					 for(int i=0; i<cols.length; i++)
					 {
						
						if(!(arr_lov[Variables.get_index(i)].equals(values[0])||arr_lov[Variables.get_index(i)].equals(values[1])||arr_lov[Variables.get_index(i)].equals(values[2]))) 
					
							{
							if(Variables.assType.equalsIgnoreCase("3"))
								arr_lov[Variables.get_index(i)]= Variables.col_vals[Variables.get_index(i)];
							else
							arr_lov[Variables.get_index(i)]= Variables.val;
							count++;
							}
						
						
					 }
					 
	                
									 
					 }
					 

						System.out.println(count + " rows have been updated");
						System.out.println(arr_lov[0] + " " +arr_lov[1] + " " +arr_lov[2] + " " +arr_lov[3] + " " +arr_lov[4]);
						if (Variables.noofcheck.equalsIgnoreCase("single"))
						 { 
						TestResults.test_results_error(count, DQ_Name);
						 }
				 }
				
				 
			}
			
			
			break;
			
		case BLANK:
		
			Variables.get_val(DQ_Name,4);
			Variables.assType();
			
			if((Variables.global==null)&&(Variables.local==null))
			{
			Object[]  arr_blank = {Variables.a1, Variables.a2, Variables.a3, Variables.a4, Variables.a5};
			
			if(Variables.assType.equalsIgnoreCase("3"))
			{
				arr_blank[2]= Variables.col_vals[0];
				arr_blank[3]= Variables.col_vals[1];
				count =2;
			}
			else
			{
			arr_blank[2]= Variables.val;
			arr_blank[3]= Variables.val;
			count =2;
			}
			System.out.println("2 rows have been updated");
			System.out.println(arr_blank[0] + " " +arr_blank[1] + " " +arr_blank[2] + " " +arr_blank[3] + " " +arr_blank[4]);
			TestResults.test_results_error(count, DQ_Name);
			}
			
			
			else
			{
				
				Object[]  arr_blank = {Variables.a1, Variables.a2, Variables.a3, Variables.a4, Variables.a5};
				ArrayList <String> list1 = new ArrayList<String>();
				String sql2="";
				conn = dbConnect.getConnect( dbuser , dbpassword);
				Statement st;
				ResultSet rs ;
				
				
			
				if((Variables.global!=null)&&(Variables.local!=null))
				sql2 = "select " + Variables.primaryKey + " from " + Variables.table + " where " + Variables.MIScolumn + " = to_date('" + Variables.MIS_date + "', 'dd-MM-yyyy') and " + Variables.global + " and " + Variables.local ;
				
				if((Variables.global!=null)&&(Variables.local==null))
				sql2 = "select " + Variables.primaryKey + " from " + Variables.table + " where " + Variables.MIScolumn + " = to_date('" + Variables.MIS_date + "', 'dd-MM-yyyy') and " + Variables.global ;	
					
				if((Variables.global==null)&&(Variables.local!=null))
				sql2 = "select " + Variables.primaryKey + " from " + Variables.table + " where " + Variables.MIScolumn + " = to_date('" + Variables.MIS_date + "', 'dd-MM-yyyy') and " + Variables.local ;	
					
				try {
					 st = conn.createStatement();
					 rs = st.executeQuery(sql2);
					
					while (rs.next()) {
						 
						list1.add(rs.getString(1));
					 } 
					
				 } 
				catch (SQLException e) {
					e.printStackTrace();
				}
				
				
				Variables.pk_filt = new String[list1.size()];
				
				for(int k=0;k<Variables.pk_filt.length;k++)
				{
					Variables.pk_filt[k]=list1.get(k);
					System.out.println(Variables.pk_filt[k]);
				}
				
				
				int[] cols = new int[list1.size()];
				for(int i =0; i<cols.length;i++)
				{
				String	query_1 ="select " + Variables.column + " from " + Variables.table + " where " + Variables.primaryKey + " = '" + Variables.pk_filt[i] + "'";				
				
				try {
					st = conn.createStatement();
					 rs = st.executeQuery(query_1);
						
						while (rs.next()) {
							 
							cols[i]=rs.getInt(1);
							
						 } 
					
				} catch (SQLException e) {
					e.printStackTrace();
				}
				
				}
			
				 for(int i=0; i<cols.length; i++)
				 {
					System.out.println(Variables.get_index(i));
					if(Variables.get_index(i)==2||Variables.get_index(i)==3) 
				
						{
						if(Variables.assType.equalsIgnoreCase("3"))
							arr_blank[Variables.get_index(i)]= Variables.col_vals[Variables.get_index(i)];
						else
							arr_blank[Variables.get_index(i)]= Variables.val;
						count++;
						}
					
				 } 
				
				
			System.out.println(count + "no. of rows have been updated");
			System.out.println(arr_blank[0] + " " +arr_blank[1] + " " +arr_blank[2] + " " +arr_blank[3] + " " +arr_blank[4]);
			TestResults.test_results_error(count, DQ_Name);		}
			
			
			
			break;
			
		case REF:
			
			
			
			if((Variables.global==null)&&(Variables.local==null))
			{
				
				ArrayList <Object> list = new ArrayList<Object>();
				ArrayList <Object> list1 = new ArrayList<Object>();
				Object[] arr_ref = {Variables.a1,Variables.a2,Variables.a3,Variables.a4,Variables.a5};
				
				list.add(Variables.a1);list.add(Variables.a2);list.add(Variables.a3);list.add(Variables.a4);list.add(Variables.a5);	
				
				
			String sql="Select " + Variables.ref_column +" from " + Variables.ref_table;
					
			System.out.println(sql);
					
			Statement st;
					try {
						conn = dbConnect.getConnect( dbuser , dbpassword);
						st = conn.createStatement();
						ResultSet rs = st.executeQuery(sql);
		                while(rs.next())
		                {
		                	list1.add(rs.getString(1));
		                }
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
					
					if(Variables.column.startsWith("N_")|| Variables.column.startsWith("n_")||(Variables.column.startsWith("V_")|| Variables.column.startsWith("v_")))
					{
					for(int i=0; i<list.size();i++)
					{
						
						for(int j=0; j<list1.size();j++){
							
							if((int)arr_ref[i]==Integer.parseInt((String)list1.get(j))){
								count++;
								break;
							}
								
							
						}
						
					}
					}
					
					else if(Variables.column.startsWith("D_")|| Variables.column.startsWith("d_")){
						
						SimpleDateFormat dmy= new SimpleDateFormat("yyyy-MM-dd");
						
						for(int i=0; i<list.size();i++)
						{
							
							
							
							for(int j=0; j<list1.size();j++){
								
								if((dmy.parse((String)arr_ref[i])).compareTo(dmy.parse((String)list1.get(j)))==0){
									count++;
									break;
								}
									
								
							}
							
						}
					}
				
					
					System.out.println(5-count + " no. of rows are not duplicates");
					TestResults.test_results_error(5-count, DQ_Name);
					
					
					
			}
			
			else
			{

				
				ArrayList <Object> list = new ArrayList<Object>();
				ArrayList <Object> list1 = new ArrayList<Object>();
				String sql2 ="";
				if((Variables.global!=null)&&(Variables.local!=null))
					sql2 = "Select " + Variables.column +" from "+Variables.table+" where "+Variables.MIScolumn+" = to_date('" + Variables.MIS_date + "', 'dd-MM-yyyy')"+" and "+ Variables.global + " and " + Variables.local;
					
					if((Variables.global!=null)&&(Variables.local==null))
					sql2 = "Select " + Variables.column +" from "+Variables.table+" where "+Variables.MIScolumn+" = to_date('" + Variables.MIS_date + "', 'dd-MM-yyyy')"+" and "+ Variables.global;	
						
					if((Variables.global==null)&&(Variables.local!=null))
					sql2 = "Select " + Variables.column +" from "+Variables.table+" where "+Variables.MIScolumn+" = to_date('" + Variables.MIS_date + "', 'dd-MM-yyyy')"+" and " + Variables.local;	
						
					Statement st1;
					try {
						conn = dbConnect.getConnect( dbuser , dbpassword);
						st1 = conn.createStatement();
						ResultSet rs = st1.executeQuery(sql2);
		                while(rs.next())
		                {
		                	list.add(rs.getString(1));
		                }
					} catch (SQLException e) {
						e.printStackTrace();
					}
					
					Object[] arr_ref = new Object[list.size()];
					
					for(int i=0;i<arr_ref.length;i++){
						arr_ref[i] = list.get(i);
					}
				
			String sql="Select " + Variables.ref_column +" from " + Variables.ref_table;
					
			System.out.println(sql);
					
			Statement st;
					try {
						conn = dbConnect.getConnect( dbuser , dbpassword);
						st = conn.createStatement();
						ResultSet rs = st.executeQuery(sql);
		                while(rs.next())
		                {
		                	list1.add(rs.getString(1));
		                }
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
					
					if(Variables.column.startsWith("N_")|| Variables.column.startsWith("n_")||(Variables.column.startsWith("V_")|| Variables.column.startsWith("v_")))
					{
					for(int i=0; i<list.size();i++)
					{
						
						for(int j=0; j<list1.size();j++){
							
							if(Integer.parseInt((String)arr_ref[i])==Integer.parseInt((String)list1.get(j))){
								count++;
								break;
							}
								
							
						}
						
					}
					}
					
					else if(Variables.column.startsWith("D_")|| Variables.column.startsWith("d_")){
						
						SimpleDateFormat dmy= new SimpleDateFormat("yyyy-MM-dd");
						
						for(int i=0; i<list.size();i++)
						{
							
							
							
							for(int j=0; j<list1.size();j++){
								
								if((dmy.parse((String)arr_ref[i])).compareTo(dmy.parse((String)list1.get(j)))==0){
									count++;
									break;
								}
									
								
							}
							
						}
					}
				
					
					System.out.println(arr_ref.length-count + " no. of rows are not duplicates");
					TestResults.test_results_error(arr_ref.length-count, DQ_Name);
					
					
					
			
			}
			
			break;

			
		case DUPLICATE:
			
			if((Variables.global==null)&&(Variables.local==null))
			{		
			
			ArrayList <Object> list = new ArrayList<Object>();
			ArrayList <Object> list1 = new ArrayList<Object>();
			String col[]=Variables.dup_col.split(",");
			Object[] arr_dup = {Variables.a1,Variables.a2,Variables.a3,Variables.a4,Variables.a5};
			
			list.add(Variables.a1);list.add(Variables.a2);list.add(Variables.a3);list.add(Variables.a4);list.add(Variables.a5);
			
			for(int i=0;i<col.length;i++)
			{
				String sql1="Select " + col[i] +" from "+Variables.table+" where "+Variables.MIScolumn+" = to_date('" + Variables.MIS_date + "', 'dd-MM-yyyy') order by " + Variables.primaryKey ;
				System.out.println(sql1);
				Statement st1;
				try {
					conn = dbConnect.getConnect( dbuser , dbpassword);
					st1 = conn.createStatement();
					ResultSet rs = st1.executeQuery(sql1);
	                while(rs.next())
	                {
	             	   list1.add(rs.getString(1));
	                }
				} catch (SQLException e) {
					e.printStackTrace();
				}
				
				
			}
			
			
			if(Variables.column.startsWith("N_")|| Variables.column.startsWith("n_")||(Variables.column.startsWith("V_")|| Variables.column.startsWith("v_")))
			{
			for(int i=0; i<list.size();i++)
			{
				
				for(int j=0; j<list1.size();j++){
					
					if((int)arr_dup[i]==Integer.parseInt((String)list1.get(j))){
						count++;
						break;
					}
						
					
				}
				
			}
			}
			
			else if(Variables.column.startsWith("D_")|| Variables.column.startsWith("d_")){
				
				SimpleDateFormat dmy= new SimpleDateFormat("yyyy-MM-dd");
				
				for(int i=0; i<list.size();i++)
				{
					
					
					
					for(int j=0; j<list1.size();j++){
						
						if((dmy.parse((String)arr_dup[i])).compareTo(dmy.parse((String)list1.get(j)))==0){
							count++;
							break;
						}
							
						
					}
					
				}
			}
		
			
			System.out.println(count + " no. of rows are duplicates");
			TestResults.test_results_error(count, DQ_Name);
			}
			
			
			else
			{

				
				ArrayList <Object> list = new ArrayList<Object>();
				ArrayList <Object> list1 = new ArrayList<Object>();
				String sql2="";
				String col[]=Variables.dup_col.split(",");
				
				if((Variables.global!=null)&&(Variables.local!=null))
				sql2 = "Select " + Variables.column +" from "+Variables.table+" where "+Variables.MIScolumn+" = to_date('" + Variables.MIS_date + "', 'dd-MM-yyyy')"+" and "+ Variables.global + " and " + Variables.local;
				
				if((Variables.global!=null)&&(Variables.local==null))
				sql2 = "Select " + Variables.column +" from "+Variables.table+" where "+Variables.MIScolumn+" = to_date('" + Variables.MIS_date + "', 'dd-MM-yyyy')"+" and "+ Variables.global;	
					
				if((Variables.global==null)&&(Variables.local!=null))
				sql2 = "Select " + Variables.column +" from "+Variables.table+" where "+Variables.MIScolumn+" = to_date('" + Variables.MIS_date + "', 'dd-MM-yyyy')"+" and " + Variables.local;	
					
				Statement st1;
				try {
					conn = dbConnect.getConnect( dbuser , dbpassword);
					st1 = conn.createStatement();
					ResultSet rs = st1.executeQuery(sql2);
	                while(rs.next())
	                {
	                	list.add(rs.getString(1));
	                }
				} catch (SQLException e) {
					e.printStackTrace();
				}
				
				Object[] arr_dup = new Object[list.size()];
				
				for(int i=0;i<arr_dup.length;i++){
					arr_dup[i] = list.get(i);
				}
				for(int i=0;i<col.length;i++)
				{
					String sql1="Select " + col[i] +" from "+Variables.table+" where "+Variables.MIScolumn+" = to_date('" + Variables.MIS_date + "', 'dd-MM-yyyy') order by " + Variables.primaryKey ;
					System.out.println(sql1);
					
					try {
						conn = dbConnect.getConnect( dbuser , dbpassword);
						st1 = conn.createStatement();
						ResultSet rs = st1.executeQuery(sql1);
		                while(rs.next())
		                {
		             	   list1.add(rs.getString(1));
		                }
					} catch (SQLException e) {
						e.printStackTrace();
					}
					
					
				}
				
				
				if(Variables.column.startsWith("N_")|| Variables.column.startsWith("n_")||(Variables.column.startsWith("V_")|| Variables.column.startsWith("v_")))
				{
				for(int i=0; i<list.size();i++)
				{
					
					for(int j=0; j<list1.size();j++){
						
						if(Integer.parseInt((String)arr_dup[i])==Integer.parseInt((String)list1.get(j))){
							count++;
							break;
						}
							
						
					}
					
				}
				}
				
				else if(Variables.column.startsWith("D_")|| Variables.column.startsWith("d_")){
					
					SimpleDateFormat dmy= new SimpleDateFormat("yyyy-MM-dd");
					
					for(int i=0; i<list.size();i++)
					{
						
						
						
						for(int j=0; j<list1.size();j++){
							
							if((dmy.parse((String)arr_dup[i])).compareTo(dmy.parse((String)list1.get(j)))==0){
								count++;
								break;
							}
								
							
						}
						
					}
				}
			
				
				System.out.println(count + " no. of rows are duplicates");
				TestResults.test_results_error(count, DQ_Name);
				
				
				
				
			}
			break;

			
			
		}
		
		
		
		
		
	}
}
public static void main(String[] args) throws ParseException {
		
	    LoadUpdateTable.Fn_LoadTestData("ofsatmcap5" , "password123") ;
		Variables.primaryKey = "V_EXPOSURE_ID";
		ObtainValues.Fn_getValues("rangecheck_1", "RANGE");
		LoadUpdateTable.Fn_UpdateMISDate();
		LoadUpdateTable.Fn_UpdateTestData("02-01-2013", "RANGE");
		System.out.println(Variables.MIS_date);
		Unixserver.Fn_connectoUnixserver("rangecheck_1_batch", Variables.MIS_date);
		Fn_Validate("ofsatmcap5", "password123", "RANGE", "rangecheck_1");
	
	}
	
	
}
