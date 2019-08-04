/************************************************************************************************
* Written by					:  	sai.ram.tatiraju@oracle.com
* Class Name					:  	UpdateData
* Class Description			    : 	This class will have the Updation functions for the 
* 									base columns in the dq rule.
* Date Created					:  	27-April-2016
* Date Modified					: 	28-June-2016
* Modified by					: 	sai.ram.tatiraju@oracle.com
*************************************************************************************************/
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;



public class UpdateData {
       public static enum Type 
       {
           RANGE , DATALENGTH , COL_REF, ISNULL , LOV , BLANK , REF , DUPLICATE 
       }

       public static void main(String[] args) {
              // TODO Auto-generated method stub
               
       }
       
       
       @SuppressWarnings("incomplete-switch")
       public static Object[] Update(String type) throws ParseException
       {
              Object[] final_values= {};
              switch (Type.valueOf(type.toUpperCase())) 
              {
              
                     case DATALENGTH:
                           final_values = updateDataLength();
                           if(Variables.global!=null)
                           UpdateFilterData.UpdateColumn(type.toUpperCase(), "global");  
                           if(Variables.local!=null)
                           UpdateFilterData.UpdateColumn(type.toUpperCase(), "local");
                     break;
                     case RANGE:
                           final_values = updateRange();
                           if(Variables.global!=null)
                               UpdateFilterData.UpdateColumn(type.toUpperCase(), "global");  
                               if(Variables.local!=null)
                               UpdateFilterData.UpdateColumn(type.toUpperCase(), "local");
                     break;
                     case ISNULL:
                           final_values = updateNull();
                           if(Variables.global!=null)
                               UpdateFilterData.UpdateColumn(type.toUpperCase(), "global");  
                               if(Variables.local!=null)
                               UpdateFilterData.UpdateColumn(type.toUpperCase(), "local");
                     break;
                     case BLANK:
                           final_values = updateBlank();
                           if(Variables.global!=null)
                               UpdateFilterData.UpdateColumn(type.toUpperCase(), "global");  
                               if(Variables.local!=null)
                               UpdateFilterData.UpdateColumn(type.toUpperCase(), "local");
                     break;
                     case REF:
                           final_values = updateREF();
                           if(Variables.global!=null)
                               UpdateFilterData.UpdateColumn(type.toUpperCase(), "global");  
                               if(Variables.local!=null)
                               UpdateFilterData.UpdateColumn(type.toUpperCase(), "local");
                     break;
                     case DUPLICATE:
                           final_values = updateDuplicate();
                           if(Variables.global!=null)
                               UpdateFilterData.UpdateColumn(type.toUpperCase(), "global");  
                               if(Variables.local!=null)
                               UpdateFilterData.UpdateColumn(type.toUpperCase(), "local");
                     break;
                     case COL_REF:
                           final_values = updateCRC();
                           if(Variables.global!=null)
                               UpdateFilterData.UpdateColumn(type.toUpperCase(), "global");  
                               if(Variables.local!=null)
                               UpdateFilterData.UpdateColumn(type.toUpperCase(), "local");
                     break;
                     case LOV:
                           final_values = updateLOV();
                           if(Variables.global!=null)
                               UpdateFilterData.UpdateColumn(type.toUpperCase(), "global");  
                               if(Variables.local!=null)
                               UpdateFilterData.UpdateColumn(type.toUpperCase(), "local");
                     break;
              }
              return final_values;
       
       }

       public static Object[] updateRange()
       {
              Object a1=0,a2=0,a3=0,a4=0,a5=0;
                     //Assigning the values according to the minimum and maximum conditions
              Variables.a1=Integer.parseInt(Variables.min)-1;
                     if(Variables.min_inclu.equalsIgnoreCase("y"))
                     {
                    	 Variables.a2=Integer.parseInt(Variables.min);
                     }
                     else
                     {
                    	 Variables.a2=Integer.parseInt(Variables.min)-2;
                     }
                     
                     Variables.a3=Integer.parseInt(Variables.min)+1;
                     Variables.a4=Integer.parseInt(Variables.max)+1;
                     if(Variables.max_inclu.equalsIgnoreCase("y"))
                     {
                    	 Variables.a5=Integer.parseInt(Variables.max);
                     }
                     else
                     {
                    	 Variables.a5=Integer.parseInt(Variables.max)+2;
                     }
                     
                     if(Variables.global!=null)
                         GetColumns.extractcolumns_global();
                         if(Variables.local!=null)
                       	  GetColumns.extractcolumns_local();
                     
              Object[] updatedvalues={Variables.a1,Variables.a2,Variables.a3,Variables.a4,Variables.a5};
              return updatedvalues;
              
       }
       public static Object[] updateDataLength()
       {
              if(Variables.column.startsWith("V_") || Variables.column.startsWith("v_"))
              {
                     Variables.a1="a";Variables.a2="b";Variables.a3="c";Variables.a4="d";Variables.a5="e";
                     char k=65;
                     for(int i=0;i<(Integer.parseInt(Variables.min)-2);i++)
                     {
                           Variables.a1=(String) Variables.a1+ k;
                           System.out.println(Variables.a1);
                           if(k==90)
                                  k=65;
                           k++;
                           Variables.a2=(String)Variables.a2+k;
                     }
                     for(int i=0;i<(Integer.parseInt(Variables.max));i++)
                     {
                           Variables.a3=(String)Variables.a3+k;
                           if(k==90)
                                  k=65;
                           k++;
                           Variables.a4=(String)Variables.a4+k;
                     }
                     for(int i=0;i<(Integer.parseInt(Variables.max)-2);i++)
                     {
                           Variables.a5=(String)Variables.a5+k;
                           if(k==90)
                                  k=65;
                           k++;
                     }
              }
              else if(Variables.column.startsWith("N_") || Variables.column.startsWith("n_")) 
              {
                     System.out.println("in here");
                     Double k = Math.pow(10,(Integer.parseInt(Variables.min)-2));
                     Variables.a1=(int) Math.floor(k);
                     k = 2*Math.pow(10,(Integer.parseInt(Variables.min)-2));
                     Variables.a2=(int) Math.floor(k);
                     k =3*Math.pow(10,(Integer.parseInt(Variables.min)));
                     Variables.a3=(int) Math.floor(k);
                     k = 4*Math.pow(10,(Integer.parseInt(Variables.max)+1));
                     Variables.a4=(int) Math.floor(k);
                     k = 5*Math.pow(10,(Integer.parseInt(Variables.max)));
                     Variables.a5=(int) Math.floor(k);
                     
              }
                     //Assigning the values according to the minimum and maximum conditions
                     
               
              if(Variables.global!=null)
              GetColumns.extractcolumns_global();
              if(Variables.local!=null)
            	  GetColumns.extractcolumns_local();
                
                     Object[] updatedvalues={Variables.a1,Variables.a2,Variables.a3,Variables.a4,Variables.a5};
              return updatedvalues;
              
       }
       public static Object[] updateNull()
       {
             
    	   if(Variables.column.startsWith("N_") || Variables.column.startsWith("n_"))
           {
    	   	  Variables.a1=null;
              Variables.a2=null;
              Variables.a3=null;
              
                     //Assigning the values according to the minimum and maximum conditions
              Variables.a4=350;
              Variables.a5=450;
           }         
    	   
    	   else if(Variables.column.startsWith("V_") || Variables.column.startsWith("v_")){
    		   
    		   
    		   Variables.a1=null;
               Variables.a2=null;
               Variables.a3=null;
               
                      //Assigning the values according to the minimum and maximum conditions
               Variables.a4="abcd";
               Variables.a5="xyzw";
    		   
    		   
    	   }
    	   
    	   
    	   else if(Variables.column.startsWith("D_") || Variables.column.startsWith("d_")){
    		   
    		   Variables.date_flag = true;
    		   Variables.a1=null;
               Variables.a2=null;
               Variables.a3=null;
               
                      //Assigning the values according to the minimum and maximum conditions
               Variables.a4="2011-01-01";
               Variables.a5="2011-01-01";
    		   
    		   
    		   
    	   }
    	   
              if(Variables.global!=null)
                  GetColumns.extractcolumns_global();
                  if(Variables.local!=null)
                	  GetColumns.extractcolumns_local();
              
              Object[] updatedvalues={Variables.a1,Variables.a2 , Variables.a3 ,Variables.a4,Variables.a5};
              return updatedvalues;
              
       }
       
       
       
       
       public static Object[] updateBlank()
       {
              //Object a1=0,a2=0,a3=0,a4=0,a5=0;
    	   
           Variables.a1=null;
           Variables.a2=null;
           Variables.a3="       ";
           Variables.a4="    ";
           Variables.a5=null;
                     //Assigning the values according to the minimum and maximum conditions
           Variables.a5=450;
                     
           
           if(Variables.global!=null)
               GetColumns.extractcolumns_global();
               if(Variables.local!=null)
             	  GetColumns.extractcolumns_local();
              Object[] updatedvalues={(Integer) null,(Integer) null,"       ","    ",Variables.a5};
           
              return updatedvalues;
              
       }
       
       
       public static Object[] updateREF()
       {
    	   
    	   if(Variables.column.startsWith("D_")|| Variables.column.startsWith("d_")){
   			Variables.date_flag =true;
   		   Variables.a1="2010-02-01";Variables.a2="2012-02-01";Variables.a3="2014-02-01";Variables.a4="2014-02-01";Variables.a5="2012-02-01";         
              //Assigning the values according to the minimum and maximum conditions
             
     			String sql="";
     			Connection conn =null;
     			Statement st;
     			conn = dbConnect.getConnect(Variables.atomUser,Variables.AtomPassword);
     			System.out.println("Connection established");
     			
   		   
        			
        			
          				sql="UPDATE "+Variables.ref_table+ " set "+Variables.ref_column+" = to_date('01-02-2012', 'dd-MM-yyyy')"; 
          				System.out.println(sql);
          				try {
          					st = conn.createStatement();
          					st.executeUpdate(sql);
          				} catch (SQLException e) {
          					e.printStackTrace();
          				}
          			      
          				if(Variables.global!=null)
          	                GetColumns.extractcolumns_global();
          	                if(Variables.local!=null)
          	              	  GetColumns.extractcolumns_local();
          				
          				Object[] updatedvalues={Variables.a1,Variables.a2,Variables.a3,Variables.a4,Variables.a5};
          	   			return updatedvalues;		
   		   
   	   }
    	   
       else 
    	   {
    	   System.out.println(Variables.ref_table);
    	   Variables.a1=3;Variables.a2=4;Variables.a3=5;Variables.a4=6;Variables.a5=7;		
   			
   		
   		
   			if(Variables.global!=null)
                GetColumns.extractcolumns_global();
                if(Variables.local!=null)
              	  GetColumns.extractcolumns_local();
   			
   			Object[] updatedvalues={Variables.a1,Variables.a2,Variables.a3,Variables.a4,Variables.a5};
   			return updatedvalues;
    	   }
    	   }
       
       
       
       public static Object[] updateDuplicate()
       {
       
    	   if(Variables.column.startsWith("D_")|| Variables.column.startsWith("d_")){
    			Variables.date_flag =true;
    		   Variables.a1="2010-02-01";Variables.a2="2012-02-01";Variables.a3="2014-02-01";Variables.a4="2014-02-01";Variables.a5="2012-02-01";         
               //Assigning the values according to the minimum and maximum conditions
               Object a[]={"01-02-2012","01-02-2013","01-02-2014","01-02-2010","01-02-2012"};
      			String sql="";
      			Connection conn =null;
      			Statement st;
      			conn = dbConnect.getConnect(Variables.atomUser,Variables.AtomPassword);
      			System.out.println("Connection established");
      			
      			String[] arr = Variables.dup_col.split(",");
    		   
      			for(int i=0; i<arr.length;i++){
         			
         			for(int k=0;k<5;k++)
           			{
           				sql="UPDATE "+Variables.table+ " set "+arr[i]+" = to_date('"+a[k]+"', 'dd-MM-yyyy') where "
           							+Variables.MIScolumn+" = to_date('" + Variables.MIS_date + "', 'dd-MM-yyyy')"+" and "+Variables.primaryKey+" = '"+Variables.pk[k] + "'";
           				System.out.println(sql);
           				try {
           					st = conn.createStatement();
           					st.executeUpdate(sql);
           				} catch (SQLException e) {
           					// TODO Auto-generated catch block
           					e.printStackTrace();
           				}
           			}      
         			}
    		   
    	   }
    	   
    	   
    	   
    	   
    	   
    	   else  if((Variables.column.startsWith("N_")|| Variables.column.startsWith("n_"))||(Variables.column.startsWith("V_")|| Variables.column.startsWith("v_"))) 
    	   	{Variables.a1=299;Variables.a2=289;Variables.a3=299;Variables.a4=299;Variables.a5=299;         
              //Assigning the values according to the minimum and maximum conditions
              Object a[]={299,299,299,300,300};
     			String sql="";
     			Connection conn =null;
     			Statement st;
     			conn = dbConnect.getConnect(Variables.atomUser,Variables.AtomPassword);
     			System.out.println("Connection established");
     			
     			String[] arr = Variables.dup_col.split(",");
     			
     			for(int i=0; i<arr.length;i++){
     			
     			for(int k=0;k<5;k++)
       			{
       				sql="UPDATE "+Variables.table+ " set "+arr[i]+" = "+a[k]+" where "
       							+Variables.MIScolumn+" = to_date('" + Variables.MIS_date + "', 'dd-MM-yyyy')"+" and "+Variables.primaryKey+" = '"+Variables.pk[k] + "'";
       				System.out.println(sql);
       				try {
       					st = conn.createStatement();
       					st.executeUpdate(sql);
       				} catch (SQLException e) {
       					// TODO Auto-generated catch block
       					e.printStackTrace();
       				}
       			}      
     			}}
     			if(Variables.global!=null)
                    GetColumns.extractcolumns_global();
                    if(Variables.local!=null)
                  	  GetColumns.extractcolumns_local();
              Object[] updatedvalues={Variables.a1,Variables.a2,Variables.a3,Variables.a4,Variables.a5};
              return updatedvalues;
              
       }
       
       
       public static Object[] updateCRC()
       {
              
              
              Object a1=3,a2=4,a3=5,a4=6,a5=7;   
              String sql="";
        	  String[] a= new String[Variables.pk.length];
              //Assigning the values according to the minimum and maximum conditions
              if(Variables.col_ref_name.startsWith("N_")||Variables.col_ref_name.startsWith("D_"))
              {
            	  
                  for(int i=0;i<Variables.pk.length;i++) 
                  {
                	  sql="select "+Variables.col_ref_name+" from "+Variables.table+" where "+Variables.MIScolumn+" = to_date('" + Variables.MIS_date + "', 'dd-MM-yyyy') and "+Variables.primaryKey+" = '"+Variables.pk[i] + "'";
                	  System.out.println(sql);
                      Connection conn =null;
                     
                      conn = dbConnect.getConnect(Variables.atomUser,Variables.AtomPassword);
                      System.out.println("Connection established");
                      
                            Statement st;
							try {
								st = conn.createStatement();
								ResultSet rs = st.executeQuery(sql);
	                            while(rs.next())
	                            {
	                         	   a[i]= rs.getString(1);
	                            }
							} catch (SQLException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
                            
                  }
                           try 
                           {
       					
                           if(Variables.col_ref_name.startsWith("N_"))
                           {
                        	   Variables.a1=Integer.parseInt(a[0])-1;
                        	   Variables.a2=Integer.parseInt(a[1])+1;
                        	   Variables.a3=Integer.parseInt(a[2])+1;
                        	   Variables.a4=Integer.parseInt(a[3]);
                        	   Variables.a5=Integer.parseInt(a[4]);
                        	   System.out.println(Variables.a1 + " " +Variables.a2 + " " +Variables.a3 + " " +Variables.a4 + " " +Variables.a5 + " " );
                           }
                           else if(Variables.col_ref_name.startsWith("D_"))
                           {
                        	      Variables.date_flag = true;
                        	      System.out.println(a[0] + " " +a[1] + " " +a[2]+ " " +a[3] + " " +a[4] + " " );
                                  Date startDate;
                                  DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd"); 
                                  Calendar cal = Calendar.getInstance();
                                  
                                  startDate = (Date) formatter.parse(a[0]);
                                  cal.setTime(startDate);
                                  System.out.println(cal.getTime());
                                  Date d2 = new Date();

                	              d2.setTime(startDate.getTime() - 1 * 24 * 60 * 60 * 1000);
                	              cal.setTime(d2);
                	              System.out.println(cal.getTime());
                                  Variables.a1=formatter.format(cal.getTime());
                                  
                                  startDate = (Date) formatter.parse(a[1]);
                                  cal.setTime(startDate);

                	              d2.setTime(startDate.getTime() + 1 * 24 * 60 * 60 * 1000);
                	              cal.setTime(d2);
                	              System.out.println(cal.getTime());
                                  Variables.a2=formatter.format(cal.getTime());
                                  
                                  startDate = (Date) formatter.parse(a[2]);
                                  cal.setTime(startDate);
                                  Variables.a3=formatter.format(cal.getTime());
                                  
                                  startDate = (Date) formatter.parse(a[3]);
                                  cal.setTime(startDate);
                                  Variables.a4=formatter.format(cal.getTime());
                                  
                                  startDate = (Date) formatter.parse(a[4]);
                                  cal.setTime(startDate);
                                  Variables.a5=formatter.format(cal.getTime());
                                  
                                  System.out.println(Variables.a1 + " " +Variables.a2 + " " +Variables.a3 + " " +Variables.a4 + " " +Variables.a5 + " " );
                           }
                           
                           
                     }  catch (ParseException e) {
                           // TODO Auto-generated catch block
                           e.printStackTrace();
                     }
              }
              else
              {
                     if(Variables.col_ref_name.contains("/")||Variables.col_ref_name.contains("-"))
                     {
                           
                    	   Variables.date_flag = true;
                    	   Date startDate;
                           DateFormat formatter = new SimpleDateFormat("dd/MM/yyyy");
                           DateFormat formatter1 = new SimpleDateFormat("yyyy-MM-dd");
                           try 
                           {
                        	   	  
                                  startDate = (Date) formatter.parse(Variables.col_ref_name);
                                  Calendar cal = Calendar.getInstance();
                                  cal.setTime(startDate);
                                  Variables.a5=formatter1.format(cal.getTime());
                                  cal.add(Calendar.DATE, -1);
                                  Variables.a1=formatter1.format(cal.getTime());
                                  cal.add(Calendar.DATE, -1);
                                  Variables.a2=formatter1.format(cal.getTime());
                                  cal.add(Calendar.DATE, +3);
                                  Variables.a3=formatter1.format(cal.getTime());
                                  cal.add(Calendar.DATE, +1);
                                  Variables.a4=formatter1.format(cal.getTime());
                                  
                           } catch (ParseException e) {
                                  // TODO Auto-generated catch block
                                  e.printStackTrace();
                           }
                     }
                     else
                     {
                    	 System.out.println("********************sql");
                    	 Variables.a1=Integer.parseInt(Variables.col_ref_name)-1;
                    	 Variables.a2=Integer.parseInt(Variables.col_ref_name)-2;
                    	 Variables.a3=Integer.parseInt(Variables.col_ref_name);
                    	 Variables.a4=Integer.parseInt(Variables.col_ref_name)+1;
                    	 Variables.a5=Integer.parseInt(Variables.col_ref_name)+2;
                     }
                     
              }
              
              if(Variables.global!=null)
                  GetColumns.extractcolumns_global();
                  if(Variables.local!=null)
                	  GetColumns.extractcolumns_local();
              Object[] updatedvalues={Variables.a1,Variables.a2,Variables.a3,Variables.a4,Variables.a5};
              return updatedvalues;
              
       }
       public static Object[] updateLOV() throws ParseException
       {
              Object a1=3,a2=4,a3=5,a4=6,a5=7;  
              String[] list=Variables.lov_value.split(",");
              String[] values=new String[3];
              //Assigning the values according to the minimum and maximum conditions
              if(Variables.column.startsWith("V_"))
              {
                     for(int k=0;(k<list.length && k<3);k++)
                     {
                           values[k]=list[k];
                     }
                     if(list.length==1)
                     {
                           Variables.a1=values[0];
                           Variables.a2=values[0];
                           Variables.a3=values[0]+"0";
                           Variables.a4=values[0]+"0";
                           Variables.a5=values[0]+"0";
                     }
                     else if(list.length==2)
                     {
                           Variables.a1=values[0];
                           Variables.a2=values[1];
                           Variables.a3=values[0]+"0";
                           Variables.a4=values[1]+"0";
                           Variables.a5=values[0]+"0";
                           
                     }
                     else 
                     {
                           Variables.a1=values[0];
                           Variables.a2=values[1];
                           Variables.a3=values[2];
                           Variables.a4=values[0]+"0";
                           Variables.a5=values[2]+"0";
                                  System.out.println(Variables.lov_value.toString()+"  "+Variables.a4.toString()+"   "+Variables.a5.toString());
                                  if(Variables.lov_value.toString().contains(Variables.a4.toString()))
                                  {
                                         Variables.a4=Variables.a4+"0";
                                  }
                                  if(Variables.lov_value.toString().contains(Variables.a5.toString()) || Variables.a4.toString().contains(Variables.a5.toString()))
                                  {
                                         Variables.a5=Variables.a5+"0";
                                  }
                     }
                     
              }
              
              
              else if(Variables.column.startsWith("N_")){
            	  

                  for(int k=0;(k<list.length && k<3);k++)
                  {
                        values[k]= list[k];
                  }
                  if(list.length==1)
                  {
                        Variables.a1=Integer.parseInt(values[0]);
                        Variables.a2=Integer.parseInt(values[0]);
                        Variables.a3=Integer.parseInt(values[0]+"0");
                        Variables.a4=Integer.parseInt(values[0]+"0");
                        Variables.a5=Integer.parseInt(values[0]+"0");
                  }
                  else if(list.length==2)
                  {
                        Variables.a1=Integer.parseInt(values[0]);
                        Variables.a2=Integer.parseInt(values[1]);
                        Variables.a3=Integer.parseInt(values[0]+"0");
                        Variables.a4=Integer.parseInt(values[1]+"0");
                        Variables.a5=Integer.parseInt(values[0]+"0");
                        
                  }
                  else 
                  {
                        Variables.a1=Integer.parseInt(values[0]);
                        Variables.a2=Integer.parseInt(values[1]);
                        Variables.a3=Integer.parseInt(values[2]);
                        Variables.a4=Integer.parseInt(values[0]+"0");
                        Variables.a5=Integer.parseInt(values[2]+"0");
                               System.out.println(Variables.lov_value.toString()+"  "+Variables.a4.toString()+"   "+Variables.a5.toString());
                               if(Variables.lov_value.toString().contains(Variables.a4.toString()))
                               {
                                      Variables.a4=Integer.parseInt(Variables.a4+"0");
                               }
                               if(Variables.lov_value.toString().contains(Variables.a5.toString()) || Variables.a4.toString().contains(Variables.a5.toString()))
                               {
                                      Variables.a5=Integer.parseInt(Variables.a5+"0");
                               }
                  }
                  
           
            	  
              }
              
              
              else if(Variables.column.startsWith("D_"))
              
              {
            	  Variables.date_flag=true;
            	  
            	  for(int k=0;(k<list.length && k<3);k++)
                  {
                        values[k]=list[k];
                  }
            	  
                  Date startDate;
                  DateFormat formatter = new SimpleDateFormat("MM/dd/yyyy"); 
                 
                  SimpleDateFormat dmy= new SimpleDateFormat("yyyy-MM-dd"); 
                  Calendar cal = Calendar.getInstance();
                  
                
            	  
            	  if(list.length==1)
                  {
            		  
            		  startDate = (Date) formatter.parse(values[0]);
                      cal.setTime(startDate);
                      Variables.a5=dmy.format(cal.getTime());
            		  
                      Date d2 = new Date();
                      d2.setTime(startDate.getTime() + 1 * 24 * 60 * 60 * 1000);
    	              cal.setTime(d2);
                      
                      Variables.a3=dmy.format(cal.getTime());
            		  
                      Variables.a2=Variables.a5;
                    
                      Variables.a4=Variables.a3;
                      Variables.a1=Variables.a3;
            		  
                  }
            	  
            	  else if(list.length==2)
                  {
            		  startDate = (Date) formatter.parse(values[0]);
                      cal.setTime(startDate);
                      Variables.a5=dmy.format(cal.getTime());
            		  
                      Date d2 = new Date();
                      d2.setTime(startDate.getTime() + 1 * 24 * 60 * 60 * 1000);
    	              cal.setTime(d2);
                      
                      Variables.a3=dmy.format(cal.getTime());
                      
                      
                      
                      startDate = (Date) formatter.parse(values[1]);
                      cal.setTime(startDate);
                      Variables.a2=dmy.format(cal.getTime());
            	
                      d2.setTime(startDate.getTime() + 1 * 24 * 60 * 60 * 1000);
    	              cal.setTime(d2);
                      
                      Variables.a4=dmy.format(cal.getTime());
                    
                      Variables.a1=Variables.a3;
                  }
            	  
            	  else
                  {	 
            		  startDate = (Date) formatter.parse(values[0]);
                      cal.setTime(startDate);
                      
                      Variables.a5=dmy.format(cal.getTime());
                    
                     
                      Date d2 = new Date();
                      d2.setTime(startDate.getTime() + 1 * 24 * 60 * 60 * 1000);
    	              cal.setTime(d2);
                      
                      Variables.a4=dmy.format(cal.getTime());
                      
                      if(Variables.lov_value.toString().contains(Variables.a4.toString()))
                      {
                    	  d2.setTime(startDate.getTime() + 7 * 24 * 60 * 60 * 1000);
        	              cal.setTime(d2);
        	              Variables.a4=dmy.format(cal.getTime());
                      }
                    
                      startDate = (Date) formatter.parse(values[1]);
                      cal.setTime(startDate);
                      
                      Variables.a2=dmy.format(cal.getTime());
                      
                      
                      
                      startDate = (Date) formatter.parse(values[2]);
                      cal.setTime(startDate);
                      
                      Variables.a3=dmy.format(cal.getTime());
                    
                      
                      d2.setTime(startDate.getTime() + 1 * 24 * 60 * 60 * 1000);
    	              cal.setTime(d2);
                      
                      Variables.a1 = dmy.format(cal.getTime());
                      
                      
                      if(Variables.lov_value.toString().contains(Variables.a5.toString()) || Variables.a4.toString().contains(Variables.a5.toString()))
                      {
                    	  d2.setTime(startDate.getTime() + 7 * 24 * 60 * 60 * 1000);
        	              cal.setTime(d2);
        	              Variables.a1=dmy.format(cal.getTime());
                      }
                      
                      System.out.println(Variables.a1 + " " + Variables.a2 + " "+ Variables.a3 + " " + Variables.a4 + " " + Variables.a5);
                  
                  
                  }
            	  
              }
              
              if(Variables.global!=null)
                  GetColumns.extractcolumns_global();
                  if(Variables.local!=null)
                	  GetColumns.extractcolumns_local();
              
              Object[] updatedvalues={Variables.a1,Variables.a2,Variables.a3,Variables.a4,Variables.a5};
              return updatedvalues;
              
       }
}

