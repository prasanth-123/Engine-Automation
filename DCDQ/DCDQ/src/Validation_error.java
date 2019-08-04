/************************************************************************************************
* Written by					:  	sai.ram.tatiraju@oracle.com
* Class Name					:  	Validation_error
* Class Description			    : 	This class will have the validation functions for all the
* 									specific checks (only error severity) in DQ.
* Date Created					:  	23-April-2016
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
import javax.swing.text.html.MinimalHTMLWriter;
	 




public class Validation_error {
	



		public static enum Type 
		{
		    RANGE , DATALENGTH , COL_REF, NULL , LOV , BLANK , REF , DUPLICATE 
		}
		
		public static void  Fn_Validate(String dbuser, String dbpassword, String type, String DQ_Name) throws ParseException
		{
			
			Connection conn =null;
			int count =0;
			Variables.col_vals = new Object[Variables.pk.length];
			
			switch (Type.valueOf(type.toUpperCase())) {
			
			case RANGE:
				
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
							count++;
							}
					}
					
					if(Variables.min_inclu.equalsIgnoreCase("n") && Variables.max_inclu.equalsIgnoreCase("y"))
					{
						if((int)arr_range[i] > Integer.parseInt(Variables.max) || (int)arr_range[i]<=Integer.parseInt(Variables.min))
							{
							count++;
							}
					}
					
					if(Variables.min_inclu.equalsIgnoreCase("y") && Variables.max_inclu.equalsIgnoreCase("n"))
					{
						if((int)arr_range[i] >= Integer.parseInt(Variables.max) || (int)arr_range[i]<Integer.parseInt(Variables.min))
							{
							count++;
							}
					}
					
					if(Variables.min_inclu.equalsIgnoreCase("n") && Variables.max_inclu.equalsIgnoreCase("n"))
					{
						if((int)arr_range[i] >= Integer.parseInt(Variables.max) ||(int) arr_range[i]<=Integer.parseInt(Variables.min))
							{
							count++;
							}
					}
					
					
					
				}
				}
				
				
				System.out.println(count + " rows have been Impacted");
				TestResults.test_results_error(count,DQ_Name);
				
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
					}
					
				
					
					for(int i=0; i<Variables.pk_filt.length; i++)
					{
						if(arr_range[i]!=null){
						if(Variables.min_inclu.equalsIgnoreCase("y") && Variables.max_inclu.equalsIgnoreCase("y"))
						{
					
							if((int)arr_range[Variables.get_index(i)]>Integer.parseInt(Variables.max) || (int)arr_range[Variables.get_index(i)]<Integer.parseInt(Variables.min))
								{
								count++;
								}
						}
						
						if(Variables.min_inclu.equalsIgnoreCase("n") && Variables.max_inclu.equalsIgnoreCase("y"))
						{
							if((int)arr_range[Variables.get_index(i)] > Integer.parseInt(Variables.max) ||(int) arr_range[Variables.get_index(i)]<=Integer.parseInt(Variables.min))
								{
								count++;
								}
						}
						
						if(Variables.min_inclu.equalsIgnoreCase("y") && Variables.max_inclu.equalsIgnoreCase("n"))
						{
							if((int)arr_range[Variables.get_index(i)] >= Integer.parseInt(Variables.max) ||(int) arr_range[Variables.get_index(i)]<Integer.parseInt(Variables.min))
								{
								count++;
								}
						}
						
						if(Variables.min_inclu.equalsIgnoreCase("n") && Variables.max_inclu.equalsIgnoreCase("n"))
						{
							if((int)arr_range[Variables.get_index(i)] >= Integer.parseInt(Variables.max) ||(int) arr_range[Variables.get_index(i)]<=Integer.parseInt(Variables.min))
								{
								count++;
								}
						}
						
					}
					
						
					}
					System.out.println("Total " + Variables.pk_filt.length+" rows and " +count + " rows have been Impacted");
					
					TestResults.test_results_error(count,DQ_Name);
					
					
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
					TestResults.test_results_error(count, DQ_Name);
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
					TestResults.test_results_error(count, DQ_Name);
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
					TestResults.test_results_error(count, DQ_Name);
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
					TestResults.test_results_error(count, DQ_Name);
				}
				
				
				}
				
				break;
				
				
			case COL_REF:

				
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
	         							count++;
	         							}
	         					}
	                        }
	                       
	      					
	      					
	      					 System.out.println(count + " no. of rows have been updated");
	      					 
	      					 TestResults.test_results_error(count, DQ_Name);
	      					 
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
		                	  //System.out.println(sql);
		                      
		                      //System.out.println(Variables.AtomPassword + " " + Variables.atomUser);
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
	      							count++;
	      							}
	      					}
	                       }
	                       
	                    System.out.println( count + " rows have been updated");
	                    
	   					TestResults.test_results_error(count, DQ_Name);
	   					
						
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
	   							count++;
	   							}
	   					}
	                    }
						
						
						
						System.out.println( count + " rows have been updated");
	   					
	   					TestResults.test_results_error(count, DQ_Name);
						
						
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
								count++;
								}
						}
						
						}
	                
	                
						System.out.println( count + " rows have been updated");
						TestResults.test_results_error(count, DQ_Name);
	                
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
		         							count++;
		         							}
		         					}
		                        }
		                       
		      					
		      					
		      					 System.out.println(count + " no. of rows have been updated");
		      					
		      					 TestResults.test_results_error(count, DQ_Name);
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
		      							count++;
		      							}
		      					}
		                       }
		                       
		                    System.out.println( count + " rows have been updated");
		                    
		   					TestResults.test_results_error(count, DQ_Name);
							
							
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
		   							count++;
		   							}
		   					}
		                    }
							
							
							System.out.println( count + " rows have been updated");
		   					
		   					TestResults.test_results_error(count, DQ_Name);
							
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
									count++;
									}
							}
							
							}
		                
		                
							System.out.println( count + " rows have been updated");
							TestResults.test_results_error(count, DQ_Name);
		                }
						
		            }
					
					
					
				}
				
				break;
				
			case NULL:
				

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
							count++;
							}
						 Variables.mul_upd_null.add(arr_null[i]);
						
					}
						
						
						
					}
					
				System.out.println(count + " rows have been updated");
				TestResults.test_results_error(count, DQ_Name);
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
							count++;
							}
						
						
					}
					}
					
					

					else if(Variables.column.startsWith("V_")||Variables.column.startsWith("v_")){

						
						for(int i=0; i<cols.length; i++)
						{
							
							
							if(arr_null[Variables.get_index(i)]==null)
								{
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
								count++;
								}
							
							
						}
						
						
						
					}
					
					
					System.out.println(count + " rows have been updated");
					TestResults.test_results_error(count, DQ_Name);
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
	                    //System.out.println(values[k]);
	              }
				
				 if(Variables.column.startsWith("V_"))
				
				 {
					 
					 for(int i=0; i<arr_lov.length; i++)
					 {
						
						if(!(arr_lov[i].equals(values[0])||arr_lov[i].equals(values[1])||arr_lov[i].equals(values[2]))) 
					
							{
							count++;
							}
						 Variables.mul_upd_lov.add(arr_lov[i]);
					 }
					 
					
				
				 } 
				 
				 if(Variables.column.startsWith("N_")){
					 

					 
					 for(int i=0; i<arr_lov.length; i++)
					 {
						
						 if(list.length==1){
								if(!((int)arr_lov[i]==Integer.parseInt(values[0]))) 
							
									{
									count++;
									}
								 }
						 
						if(list.length==2){
						if(!((int)arr_lov[i]==Integer.parseInt(values[0])||(int)arr_lov[i]==Integer.parseInt(values[1]))) 
					
							{
							count++;
							}
						 }
						
						if(list.length==3){
							if(!((int)arr_lov[i]==Integer.parseInt(values[0])||(int)arr_lov[i]==Integer.parseInt(values[1])||(int)arr_lov[i]==Integer.parseInt(values[2]))) 
						
								{
								count++;
								}
							 }
						
						 Variables.mul_upd_lov.add(arr_lov[i]);
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
							count++;
							}
						
					 } 
					 
					 }
					 
				 }
				
				 
				System.out.println(count + " rows have been updated");
				TestResults.test_results_error(count, DQ_Name);
				
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
								count++;
								}
							
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
								count++;
								}
							
							
						 }
						 
		                
										 
						 }
						 
					 }
					
					 
					System.out.println(count + " rows have been updated");
					System.out.println(arr_lov[0] + " " +arr_lov[1] + " " +arr_lov[2] + " " +arr_lov[3] + " " +arr_lov[4]);
					TestResults.test_results_error(count, DQ_Name);
				}
				
				
				break;
				
			case BLANK:
				
				if((Variables.global==null)&&(Variables.local==null))
				{
				count =2;
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
							count++;
							}
						
					 } 
					
					
					}
				
				System.out.println(count + "no. of rows have been updated");
				TestResults.test_results_error(count, DQ_Name);	
				
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
				TestResults .test_results_error(count, DQ_Name);
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
