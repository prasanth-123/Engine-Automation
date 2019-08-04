/************************************************************************************************
* Written by					:  	sai.ram.tatiraju@oracle.com
* Class Name					:  	UpdateFilterData
* Class Description			    : 	This class will have the Updation functions for the filter
* 									conditions.
* Date Created					:  	27-April-2016
* Date Modified					: 	28-June-2016
* Modified by					: 	sai.ram.tatiraju@oracle.com
*************************************************************************************************/
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;




public class UpdateFilterData {
	public static enum Type 
    {
        RANGE , DATALENGTH , COL_REF, NULL , LOV , BLANK , REF , DUPLICATE 
    }
	public static void UpdateColumn(String type,String filtertype)
    {
           Object[] final_values={};
           switch (Type.valueOf(type.toUpperCase())) 
           {
           
                  case DATALENGTH:
                        updateDataLength(filtertype);
                  break;
                  case RANGE:
                        updateRange(filtertype);
                  break;
                  case NULL:
                        updateNull(filtertype);
                  break;
                  case BLANK:
                        updateBlank(filtertype);
                  break;
                  case REF:
                        updateREF(filtertype);
                  break;
                  case DUPLICATE:
                        updateDuplicate(filtertype);
                  break;
                  case COL_REF:
                        updateCRC(filtertype);
                  break;
                  case LOV:
                        updateLOV(filtertype);
                  break;
           }
    
    }

	private static void updateLOV(String filter) {
		// TODO Auto-generated method stub
		Connection conn =null;
		Statement st;
		String rows="";
		String sql="";
		String[] condition={};
		String[] columns={};
		Object[] l={};
		conn = dbConnect.getConnect(Variables.atomUser,Variables.AtomPassword);
		System.out.println("Connection established");
		if(filter.equalsIgnoreCase("global"))
		{
			 columns=Variables.gc;
			 condition=Variables.global_condition;
			 l=new Object[5];
		}
		else if(filter.equalsIgnoreCase("local"))
		{
			 columns=Variables.lc;
			 condition=Variables.local_condition;
			l=new Object[5];
		}
    //Assigning the values according to the global filter
    for(int i=0;i<columns.length;i++) 
    {
    		if(condition[i].contains("null"))
    		{
    			if(columns[i].startsWith("D_"))
            	{
    				l[0]=null;
    				l[1]=null;
    				l[2]="22-01-1991";
    				l[3]="22-01-1991";
    				l[4]="22-01-1991";
            	}
    			else
    			{
    				l[0]=null;
    				l[1]=null;
    				l[2]="11";
    				l[3]="11";
    				l[4]="11";
    			}
    		}
    		else if(condition[i].contains("notequal"))
    		{
    			String value=condition[i].replace("notequal", "");
    			
    			if(columns[i].startsWith("D_"))
            	{
    				Date startDate;
                    DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd"); 
                    Calendar cal = Calendar.getInstance();
                    
                    try {
							startDate = (Date) formatter.parse(value);
							cal.setTime(startDate);
                            System.out.println(cal.getTime());
                            Date d2 = new Date();

                        d2.setTime(startDate.getTime()- 1 * 24 * 60 * 60 * 1000);
          	            cal.setTime(d2);
                        System.out.println(cal.getTime());
          	            l[0]=formatter.format(cal.getTime());
          	              
          	            d2.setTime(startDate.getTime()- 1 * 24 * 60 * 60 * 1000);
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[1]=formatter.format(cal.getTime());
          	            
          	            d2.setTime(startDate.getTime());
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[2]=formatter.format(cal.getTime());
          	            
          	            d2.setTime(startDate.getTime());
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[3]=formatter.format(cal.getTime());
      	              
          	            d2.setTime(startDate.getTime());
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[4]=formatter.format(cal.getTime());
          	            
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
            	}
    			else
    			{
    				l[0]=Integer.parseInt(value)+1;
    				l[1]=Integer.parseInt(value)+1;
    				l[2]=Integer.parseInt(value)+1;
    				l[3]=value;
    				l[4]=value;
    			}
    		}
    		else if(condition[i].contains("not"))
    		{
    			if(columns[i].startsWith("D_"))
            	{
    				l[0]="22-01-1991";
    				l[1]=null;
    				l[2]=null;
    				l[3]=null;
    				l[4]=null;
            	}
    			else
    			{
    				l[0]="11";
    				l[1]=null;
    				l[2]=null;
    				l[3]=null;
    				l[4]=null;
    			}
    		}
    		else if(condition[i].contains("greaterequal"))
    		{

    			String value=condition[i].replace("greaterequal", "");
    			
    			if(columns[i].startsWith("D_"))
            	{
    				Date startDate;
                    DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd"); 
                    Calendar cal = Calendar.getInstance();
                    
                    try {
						startDate = (Date) formatter.parse(value);
						cal.setTime(startDate);
                        System.out.println(cal.getTime());
                        Date d2 = new Date();

                    d2.setTime(startDate.getTime());
      	            cal.setTime(d2);
                    System.out.println(cal.getTime());
      	            l[0]=formatter.format(cal.getTime());
      	              
      	            d2.setTime(startDate.getTime()+ 1 * 24 * 60 * 60 * 1000);
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[1]=formatter.format(cal.getTime());
      	            
      	            d2.setTime(startDate.getTime()+ 1 * 24 * 60 * 60 * 1000);
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[2]=formatter.format(cal.getTime());
      	            
      	            d2.setTime(startDate.getTime() - 1 * 24 * 60 * 60 * 1000);
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[3]=formatter.format(cal.getTime());
  	              
      	            d2.setTime(startDate.getTime() - 1 * 24 * 60 * 60 * 1000);
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[4]=formatter.format(cal.getTime());
      	            
				} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
            	}
    			else
    			{
    				l[0]=value;
    				l[1]=Integer.parseInt(value)+1;
    				l[2]=Integer.parseInt(value)+1;
    				l[3]=Integer.parseInt(value)-1;
    				l[4]=Integer.parseInt(value)-1;
    			}
    		}
    		else if(condition[i].contains("lesserequal"))
    		{

    			String value=condition[i].replace("lesserequal", "");
    			
    			if(columns[i].startsWith("D_"))
            	{
    				Date startDate;
                    DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd"); 
                    Calendar cal = Calendar.getInstance();
                    
                    try {
						startDate = (Date) formatter.parse(value);
						cal.setTime(startDate);
                        System.out.println(cal.getTime());
                        Date d2 = new Date();

                    d2.setTime(startDate.getTime());
      	            cal.setTime(d2);
                    System.out.println(cal.getTime());
      	            l[0]=formatter.format(cal.getTime());
      	              
      	            d2.setTime(startDate.getTime()- 1 * 24 * 60 * 60 * 1000);
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[1]=formatter.format(cal.getTime());
      	            
      	            d2.setTime(startDate.getTime()- 1 * 24 * 60 * 60 * 1000);
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[2]=formatter.format(cal.getTime());
      	            
      	            d2.setTime(startDate.getTime() + 1 * 24 * 60 * 60 * 1000);
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[3]=formatter.format(cal.getTime());
  	              
      	            d2.setTime(startDate.getTime()+ 1 * 24 * 60 * 60 * 1000 );
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[4]=formatter.format(cal.getTime());
      	            
				} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
            	}
    			else
    			{
    				l[0]=value;
    				l[1]=Integer.parseInt(value)-1;
    				l[2]=Integer.parseInt(value)-1;
    				l[3]=Integer.parseInt(value)+1;
    				l[4]=Integer.parseInt(value)+1;
    			}
    		}
    		else if(condition[i].contains("greater"))
    		{
    			String value=condition[i].replace("greater", "");
    			
    			if(columns[i].startsWith("D_"))
            	{
    				Date startDate;
                    DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd"); 
                    Calendar cal = Calendar.getInstance();
                    
                    try {
						startDate = (Date) formatter.parse(value);
						cal.setTime(startDate);
                        System.out.println(cal.getTime());
                        Date d2 = new Date();

                    d2.setTime(startDate.getTime() + 1 * 24 * 60 * 60 * 1000);
      	            cal.setTime(d2);
                    System.out.println(cal.getTime());
      	            l[0]=formatter.format(cal.getTime());
      	              
      	            d2.setTime(startDate.getTime()+ 1 * 24 * 60 * 60 * 1000);
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[1]=formatter.format(cal.getTime());
      	            
      	            d2.setTime(startDate.getTime()+ 1 * 24 * 60 * 60 * 1000);
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[2]=formatter.format(cal.getTime());
      	            
      	            d2.setTime(startDate.getTime());
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[3]=formatter.format(cal.getTime());
  	              
      	            d2.setTime(startDate.getTime());
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[4]=formatter.format(cal.getTime());
      	            
				} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
            	}
    			else
    			{
    				l[0]=Integer.parseInt(value)+1;
    				l[1]=Integer.parseInt(value)+1;
    				l[2]=Integer.parseInt(value)+1;
    				l[3]=Integer.parseInt(value)-2;
    				l[4]=Integer.parseInt(value)-3;;
    			}
    		}
    		else if(condition[i].contains("lesser"))
    		{
    			String value=condition[i].replace("lesser", "");
    			
    			if(columns[i].startsWith("D_"))
            	{
    				Date startDate;
                    DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd"); 
                    Calendar cal = Calendar.getInstance();
                    
                    try {
							startDate = (Date) formatter.parse(value);
							cal.setTime(startDate);
                            System.out.println(cal.getTime());
                            Date d2 = new Date();

                        d2.setTime(startDate.getTime() - 1 * 24 * 60 * 60 * 1000);
          	            cal.setTime(d2);
                        System.out.println(cal.getTime());
          	            l[0]=formatter.format(cal.getTime());
          	              
          	            d2.setTime(startDate.getTime()- 1 * 24 * 60 * 60 * 1000);
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[1]=formatter.format(cal.getTime());
          	            
          	            d2.setTime(startDate.getTime()- 1 * 24 * 60 * 60 * 1000);
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[2]=formatter.format(cal.getTime());
          	            
          	            d2.setTime(startDate.getTime());
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[3]=formatter.format(cal.getTime());
      	              
          	            d2.setTime(startDate.getTime());
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[4]=formatter.format(cal.getTime());
          	            
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
            	}
    			else
    			{
    				l[0]=Integer.parseInt(value)-1;
    				l[1]=Integer.parseInt(value)-1;
    				l[2]=Integer.parseInt(value)-2;
    				l[3]=Integer.parseInt(value)+1;
    				l[4]=Integer.parseInt(value)+3;;
    			}
    		}
    		else if(condition[i].contains("equal"))
    		{
    			String value=condition[i].replace("equal", "");
    			
    			if(columns[i].startsWith("D_"))
            	{
    				Date startDate;
                    DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd"); 
                    Calendar cal = Calendar.getInstance();
                    
                    try {
							startDate = (Date) formatter.parse(value);
							cal.setTime(startDate);
                            System.out.println(cal.getTime());
                            Date d2 = new Date();

                        d2.setTime(startDate.getTime() );
          	            cal.setTime(d2);
                        System.out.println(cal.getTime());
          	            l[0]=formatter.format(cal.getTime());
          	              
          	            d2.setTime(startDate.getTime());
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[1]=formatter.format(cal.getTime());
          	            
          	            d2.setTime(startDate.getTime());
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[2]=formatter.format(cal.getTime());
          	            
          	            d2.setTime(startDate.getTime()- 2 * 24 * 60 * 60 * 1000);
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[3]=formatter.format(cal.getTime());
      	              
          	            d2.setTime(startDate.getTime()- 3 * 24 * 60 * 60 * 1000);
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[4]=formatter.format(cal.getTime());
          	            
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
            	}
    			else
    			{
    				l[0]=value;
    				l[1]=value;
    				l[2]=value;
    				l[3]=Integer.parseInt(value)+1;
    				l[4]=Integer.parseInt(value)+1;
    			}
    		}
    		
    		
    		for(int j=0;j<5;j++)
  	      {	
    		
    		
    	if(columns[i].startsWith("D_"))
    	{
    		sql="UPDATE "+Variables.table+ " set "+columns[i]+" = to_date('" + l[j] + "', 'dd-MM-yyyy') where "
						+Variables.MIScolumn+" = to_date('" + Variables.MIS_date + "', 'dd-MM-yyyy')"+" and "+Variables.primaryKey+" = '"+Variables.pk[j]+ "'";
    	}
    	else 
    	{
    		sql="UPDATE "+Variables.table+ " set "+columns[i]+" = '"+l[j]+"' where "
						+Variables.MIScolumn+" = to_date('" + Variables.MIS_date + "', 'dd-MM-yyyy')"+" and "+Variables.primaryKey+" = '"+Variables.pk[j] + "'";
    	}
    		
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

	private static void updateCRC(String filter) {
		// TODO Auto-generated method stub
		Connection conn =null;
		Statement st;
		String rows="";
		String sql="";
		String[] condition={};
		String[] columns={};
		Object[] l={};
		conn = dbConnect.getConnect(Variables.atomUser,Variables.AtomPassword);
		System.out.println("Connection established");
		if(filter.equalsIgnoreCase("global"))
		{
			 columns=Variables.gc;
			 condition=Variables.global_condition;
			 l=new Object[5];
		}
		else if(filter.equalsIgnoreCase("local"))
		{
			 columns=Variables.lc;
			 condition=Variables.local_condition;
			l=new Object[5];
		}
    //Assigning the values according to the global filter
    for(int i=0;i<columns.length;i++) 
    {
    		if(condition[i].contains("null"))
    		{
    			if(columns[i].startsWith("D_"))
            	{
    				l[0]=null;
    				l[1]=null;
    				l[2]=null;
    				l[3]="22-01-1991";
    				l[4]="22-01-1991";
            	}
    			else
    			{
    				l[0]=null;
    				l[1]=null;
    				l[2]=null;
    				l[3]="11";
    				l[4]="11";
    			}
    		}
    		else if(condition[i].contains("notequal"))
    		{
    			String value=condition[i].replace("notequal", "");
    			
    			if(columns[i].startsWith("D_"))
            	{
    				Date startDate;
                    DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd"); 
                    Calendar cal = Calendar.getInstance();
                    
                    try {
							startDate = (Date) formatter.parse(value);
							cal.setTime(startDate);
                            System.out.println(cal.getTime());
                            Date d2 = new Date();

                        d2.setTime(startDate.getTime()- 3 * 24 * 60 * 60 * 1000);
          	            cal.setTime(d2);
                        System.out.println(cal.getTime());
          	            l[0]=formatter.format(cal.getTime());
          	              
          	            d2.setTime(startDate.getTime()- 2 * 24 * 60 * 60 * 1000);
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[1]=formatter.format(cal.getTime());
          	            
          	            d2.setTime(startDate.getTime()- 1 * 24 * 60 * 60 * 1000);
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[2]=formatter.format(cal.getTime());
          	            
          	            d2.setTime(startDate.getTime());
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[3]=formatter.format(cal.getTime());
      	              
          	            d2.setTime(startDate.getTime());
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[4]=formatter.format(cal.getTime());
          	            
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
            	}
    			else
    			{
    				l[0]=Integer.parseInt(value)+1;
    				l[1]=Integer.parseInt(value)+2;
    				l[2]=Integer.parseInt(value)+3;
    				l[3]=value;
    				l[4]=value;
    			}
    		}
    		else if(condition[i].contains("not"))
    		{
    			if(columns[i].startsWith("D_"))
            	{
    				l[0]="22-01-1991";
    				l[1]="22-01-1991";
    				l[2]="22-01-1991";
    				l[3]=null;
    				l[4]=null;
            	}
    			else
    			{
    				l[0]="11";
    				l[1]="11";
    				l[2]="11";
    				l[3]=null;
    				l[4]=null;
    			}
    		}
    		else if(condition[i].contains("greaterequal"))
    		{

    			String value=condition[i].replace("greaterequal", "");
    			
    			if(columns[i].startsWith("D_"))
            	{
    				Date startDate;
                    DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd"); 
                    Calendar cal = Calendar.getInstance();
                    
                    try {
						startDate = (Date) formatter.parse(value);
						cal.setTime(startDate);
                        System.out.println(cal.getTime());
                        Date d2 = new Date();

                    d2.setTime(startDate.getTime());
      	            cal.setTime(d2);
                    System.out.println(cal.getTime());
      	            l[0]=formatter.format(cal.getTime());
      	              
      	            d2.setTime(startDate.getTime()  + 1 * 24 * 60 * 60 * 1000);
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[1]=formatter.format(cal.getTime());
      	            
      	            d2.setTime(startDate.getTime() + 1 * 24 * 60 * 60 * 1000);
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[2]=formatter.format(cal.getTime());
      	            
      	            d2.setTime(startDate.getTime() - 1 * 24 * 60 * 60 * 1000);
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[3]=formatter.format(cal.getTime());
  	              
      	            d2.setTime(startDate.getTime() - 1 * 24 * 60 * 60 * 1000);
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[4]=formatter.format(cal.getTime());
      	            
				} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
            	}
    			else
    			{
    				l[0]=value;
    				l[1]=value;
    				l[2]=Integer.parseInt(value)+1;
    				l[3]=Integer.parseInt(value)-1;
    				l[4]=Integer.parseInt(value)-1;
    			}
    		}
    		else if(condition[i].contains("lesserequal"))
    		{

    			String value=condition[i].replace("lesserequal", "");
    			
    			if(columns[i].startsWith("D_"))
            	{
    				Date startDate;
                    DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd"); 
                    Calendar cal = Calendar.getInstance();
                    
                    try {
						startDate = (Date) formatter.parse(value);
						cal.setTime(startDate);
                        System.out.println(cal.getTime());
                        Date d2 = new Date();

                    d2.setTime(startDate.getTime());
      	            cal.setTime(d2);
                    System.out.println(cal.getTime());
      	            l[0]=formatter.format(cal.getTime());
      	              
      	            d2.setTime(startDate.getTime() - 1 * 24 * 60 * 60 * 1000);
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime() );
      	            l[1]=formatter.format(cal.getTime());
      	            
      	            d2.setTime(startDate.getTime()- 1 * 24 * 60 * 60 * 1000);
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[2]=formatter.format(cal.getTime());
      	            
      	            d2.setTime(startDate.getTime() + 1 * 24 * 60 * 60 * 1000);
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[3]=formatter.format(cal.getTime());
  	              
      	            d2.setTime(startDate.getTime()+ 1 * 24 * 60 * 60 * 1000 );
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[4]=formatter.format(cal.getTime());
      	            
				} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
            	}
    			else
    			{
    				l[0]=value;
    				l[1]=Integer.parseInt(value)-1;
    				l[2]=Integer.parseInt(value)-1;
    				l[3]=Integer.parseInt(value)+1;
    				l[4]=Integer.parseInt(value)+1;
    			}
    		}
    		else if(condition[i].contains("greater"))
    		{
    			String value=condition[i].replace("greater", "");
    			
    			if(columns[i].startsWith("D_"))
            	{
    				Date startDate;
                    DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd"); 
                    Calendar cal = Calendar.getInstance();
                    
                    try {
						startDate = (Date) formatter.parse(value);
						cal.setTime(startDate);
                        System.out.println(cal.getTime());
                        Date d2 = new Date();

                    d2.setTime(startDate.getTime() + 1 * 24 * 60 * 60 * 1000);
      	            cal.setTime(d2);
                    System.out.println(cal.getTime());
      	            l[0]=formatter.format(cal.getTime());
      	              
      	            d2.setTime(startDate.getTime()+ 2 * 24 * 60 * 60 * 1000);
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[1]=formatter.format(cal.getTime());
      	            
      	            d2.setTime(startDate.getTime()+ 3 * 24 * 60 * 60 * 1000);
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[2]=formatter.format(cal.getTime());
      	            
      	            d2.setTime(startDate.getTime());
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[3]=formatter.format(cal.getTime());
  	              
      	            d2.setTime(startDate.getTime());
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[4]=formatter.format(cal.getTime());
      	            
				} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
            	}
    			else
    			{
    				l[0]=Integer.parseInt(value)+1;
    				l[1]=Integer.parseInt(value)+2;
    				l[2]=Integer.parseInt(value)+3;
    				l[3]=Integer.parseInt(value)-2;
    				l[4]=Integer.parseInt(value)-3;;
    			}
    		}
    		else if(condition[i].contains("lesser"))
    		{
    			String value=condition[i].replace("lesser", "");
    			
    			if(columns[i].startsWith("D_"))
            	{
    				Date startDate;
                    DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd"); 
                    Calendar cal = Calendar.getInstance();
                    
                    try {
							startDate = (Date) formatter.parse(value);
							cal.setTime(startDate);
                            System.out.println(cal.getTime());
                            Date d2 = new Date();

                        d2.setTime(startDate.getTime() - 1 * 24 * 60 * 60 * 1000);
          	            cal.setTime(d2);
                        System.out.println(cal.getTime());
          	            l[0]=formatter.format(cal.getTime());
          	              
          	            d2.setTime(startDate.getTime() - 2 * 24 * 60 * 60 * 1000);
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[1]=formatter.format(cal.getTime());
          	            
          	            d2.setTime(startDate.getTime()- 3 * 24 * 60 * 60 * 1000);
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[2]=formatter.format(cal.getTime());
          	            
          	            d2.setTime(startDate.getTime());
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[3]=formatter.format(cal.getTime());
      	              
          	            d2.setTime(startDate.getTime());
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[4]=formatter.format(cal.getTime());
          	            
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
            	}
    			else
    			{
    				l[0]=Integer.parseInt(value)-1;
    				l[1]=Integer.parseInt(value)-2;
    				l[2]=Integer.parseInt(value)-3;
    				l[3]=Integer.parseInt(value)+1;
    				l[4]=Integer.parseInt(value)+3;;
    			}
    		}
    		else if(condition[i].contains("equal"))
    		{
    			String value=condition[i].replace("equal", "");
    			
    			if(columns[i].startsWith("D_"))
            	{
    				Date startDate;
                    DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd"); 
                    Calendar cal = Calendar.getInstance();
                    
                    try {
							startDate = (Date) formatter.parse(value);
							cal.setTime(startDate);
                            System.out.println(cal.getTime());
                            Date d2 = new Date();

                        d2.setTime(startDate.getTime() );
          	            cal.setTime(d2);
                        System.out.println(cal.getTime());
          	            l[0]=formatter.format(cal.getTime());
          	              
          	            d2.setTime(startDate.getTime());
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[1]=formatter.format(cal.getTime());
          	            
          	            d2.setTime(startDate.getTime());
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[2]=formatter.format(cal.getTime());
          	            
          	            d2.setTime(startDate.getTime()- 2 * 24 * 60 * 60 * 1000);
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[3]=formatter.format(cal.getTime());
      	              
          	            d2.setTime(startDate.getTime()- 3 * 24 * 60 * 60 * 1000);
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[4]=formatter.format(cal.getTime());
          	            
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
            	}
    			else
    			{
    				l[0]=value;
    				l[1]=value;
    				l[2]=value;
    				l[3]=Integer.parseInt(value)+1;
    				l[4]=Integer.parseInt(value)+1;
    			}
    		}
    
    	for(int j=0;j<5;j++)
    	      {	
    		
    	if(columns[i].startsWith("D_"))
    	{
    		sql="UPDATE "+Variables.table+ " set "+columns[i]+" = to_date('" + l[j] + "', 'dd-MM-yyyy') where "
						+Variables.MIScolumn+" = to_date('" + Variables.MIS_date + "', 'dd-MM-yyyy')"+" and "+Variables.primaryKey+" = '"+Variables.pk[j]+"'";
    	}
    	else 
    	{
    		sql="UPDATE "+Variables.table+ " set "+columns[i]+" = '"+l[j]+"' where "
						+Variables.MIScolumn+" = to_date('" + Variables.MIS_date + "', 'dd-MM-yyyy')"+" and "+Variables.primaryKey+" = '"+Variables.pk[j]+"'";
    	}
    		
    	try {
			st = conn.createStatement();
			System.out.println(sql);
			st.executeUpdate(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	      }
    }
}

	private static void updateDuplicate(String filter) {
		// TODO Auto-generated method stub
		Connection conn =null;
		Statement st;
		String rows="";
		String sql="";
		String[] condition={};
		String[] columns={};
		Object[] l={};
		conn = dbConnect.getConnect(Variables.atomUser,Variables.AtomPassword);
		System.out.println("Connection established");
		if(filter.equalsIgnoreCase("global"))
		{
			 columns=Variables.gc;
			 condition=Variables.global_condition;
			 l=new Object[5];
		}
		else if(filter.equalsIgnoreCase("local"))
		{
			 columns=Variables.lc;
			 condition=Variables.local_condition;
			l=new Object[5];
		}
    //Assigning the values according to the global filter
    for(int i=0;i<columns.length;i++) 
    {
    		if(condition[i].contains("null"))
    		{
    			if(columns[i].startsWith("D_"))
            	{
    				l[0]=null;
    				l[1]=null;
    				l[2]="22-01-1991";
    				l[3]="22-01-1991";
    				l[4]="22-01-1991";
            	}
    			else
    			{
    				l[0]=null;
    				l[1]=null;
    				l[2]="11";
    				l[3]="11";
    				l[4]="11";
    			}
    		}
    		else if(condition[i].contains("notequal"))
    		{
    			String value=condition[i].replace("notequal", "");
    			
    			if(columns[i].startsWith("D_"))
            	{
    				Date startDate;
                    DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd"); 
                    Calendar cal = Calendar.getInstance();
                    
                    try {
							startDate = (Date) formatter.parse(value);
							cal.setTime(startDate);
                            System.out.println(cal.getTime());
                            Date d2 = new Date();

                        d2.setTime(startDate.getTime()- 1 * 24 * 60 * 60 * 1000);
          	            cal.setTime(d2);
                        System.out.println(cal.getTime());
          	            l[0]=formatter.format(cal.getTime());
          	              
          	            d2.setTime(startDate.getTime()- 2 * 24 * 60 * 60 * 1000);
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[1]=formatter.format(cal.getTime());
          	            
          	            d2.setTime(startDate.getTime()- 2 * 24 * 60 * 60 * 1000);
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[2]=formatter.format(cal.getTime());
          	            
          	            d2.setTime(startDate.getTime());
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[3]=formatter.format(cal.getTime());
      	              
          	            d2.setTime(startDate.getTime());
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[4]=formatter.format(cal.getTime());
          	            
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
            	}
    			else
    			{
    				l[0]=Integer.parseInt(value)+1;
    				l[1]=Integer.parseInt(value)+1;
    				l[2]=Integer.parseInt(value)+1;
    				l[3]=value;
    				l[4]=value;
    			}
    		}
    		else if(condition[i].contains("not"))
    		{
    			if(columns[i].startsWith("D_"))
            	{
    				l[0]="22-01-1991";
    				l[1]="22-01-1991";
    				l[2]=null;
    				l[3]=null;
    				l[4]=null;
            	}
    			else
    			{
    				l[0]="11";
    				l[1]="11";
    				l[2]=null;
    				l[3]=null;
    				l[4]=null;
    			}
    		}
    		else if(condition[i].contains("greaterequal"))
    		{

    			String value=condition[i].replace("greaterequal", "");
    			
    			if(columns[i].startsWith("D_"))
            	{
    				Date startDate;
                    DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd"); 
                    Calendar cal = Calendar.getInstance();
                    
                    try {
						startDate = (Date) formatter.parse(value);
						cal.setTime(startDate);
                        System.out.println(cal.getTime());
                        Date d2 = new Date();

                    d2.setTime(startDate.getTime());
      	            cal.setTime(d2);
                    System.out.println(cal.getTime());
      	            l[0]=formatter.format(cal.getTime());
      	              
      	            d2.setTime(startDate.getTime() );
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[1]=formatter.format(cal.getTime());
      	            
      	            d2.setTime(startDate.getTime()+ 1 * 24 * 60 * 60 * 1000);
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[2]=formatter.format(cal.getTime());
      	            
      	            d2.setTime(startDate.getTime() - 1 * 24 * 60 * 60 * 1000);
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[3]=formatter.format(cal.getTime());
  	              
      	            d2.setTime(startDate.getTime() - 1 * 24 * 60 * 60 * 1000);
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[4]=formatter.format(cal.getTime());
      	            
				} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
            	}
    			else
    			{
    				l[0]=value;
    				l[1]=value;
    				l[2]=Integer.parseInt(value)+1;
    				l[3]=Integer.parseInt(value)-1;
    				l[4]=Integer.parseInt(value)-1;
    			}
    		}
    		else if(condition[i].contains("lesserequal"))
    		{

    			String value=condition[i].replace("lesserequal", "");
    			
    			if(columns[i].startsWith("D_"))
            	{
    				Date startDate;
                    DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd"); 
                    Calendar cal = Calendar.getInstance();
                    
                    try {
						startDate = (Date) formatter.parse(value);
						cal.setTime(startDate);
                        System.out.println(cal.getTime());
                        Date d2 = new Date();

                    d2.setTime(startDate.getTime());
      	            cal.setTime(d2);
                    System.out.println(cal.getTime());
      	            l[0]=formatter.format(cal.getTime());
      	              
      	            d2.setTime(startDate.getTime() );
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[1]=formatter.format(cal.getTime());
      	            
      	            d2.setTime(startDate.getTime()- 1 * 24 * 60 * 60 * 1000);
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[2]=formatter.format(cal.getTime());
      	            
      	            d2.setTime(startDate.getTime() + 1 * 24 * 60 * 60 * 1000);
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[3]=formatter.format(cal.getTime());
  	              
      	            d2.setTime(startDate.getTime()+ 1 * 24 * 60 * 60 * 1000 );
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[4]=formatter.format(cal.getTime());
      	            
				} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
            	}
    			else
    			{
    				l[0]=value;
    				l[1]=value;
    				l[2]=Integer.parseInt(value)-1;
    				l[3]=Integer.parseInt(value)+1;
    				l[4]=Integer.parseInt(value)+1;
    			}
    		}
    		else if(condition[i].contains("greater"))
    		{
    			String value=condition[i].replace("greater", "");
    			
    			if(columns[i].startsWith("D_"))
            	{
    				Date startDate;
                    DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd"); 
                    Calendar cal = Calendar.getInstance();
                    
                    try {
						startDate = (Date) formatter.parse(value);
						cal.setTime(startDate);
                        System.out.println(cal.getTime());
                        Date d2 = new Date();

                    d2.setTime(startDate.getTime() + 1 * 24 * 60 * 60 * 1000);
      	            cal.setTime(d2);
                    System.out.println(cal.getTime());
      	            l[0]=formatter.format(cal.getTime());
      	              
      	            d2.setTime(startDate.getTime()+ 2 * 24 * 60 * 60 * 1000);
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[1]=formatter.format(cal.getTime());
      	            
      	            d2.setTime(startDate.getTime()+ 3 * 24 * 60 * 60 * 1000);
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[2]=formatter.format(cal.getTime());
      	            
      	            d2.setTime(startDate.getTime());
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[3]=formatter.format(cal.getTime());
  	              
      	            d2.setTime(startDate.getTime());
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[4]=formatter.format(cal.getTime());
      	            
				} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
            	}
    			else
    			{
    				l[0]=Integer.parseInt(value)+1;
    				l[1]=Integer.parseInt(value)+2;
    				l[2]=Integer.parseInt(value)+3;
    				l[3]=Integer.parseInt(value)-2;
    				l[4]=Integer.parseInt(value)-3;;
    			}
    		}
    		else if(condition[i].contains("lesser"))
    		{
    			String value=condition[i].replace("lesser", "");
    			
    			if(columns[i].startsWith("D_"))
            	{
    				Date startDate;
                    DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd"); 
                    Calendar cal = Calendar.getInstance();
                    
                    try {
							startDate = (Date) formatter.parse(value);
							cal.setTime(startDate);
                            System.out.println(cal.getTime());
                            Date d2 = new Date();

                        d2.setTime(startDate.getTime() - 1 * 24 * 60 * 60 * 1000);
          	            cal.setTime(d2);
                        System.out.println(cal.getTime());
          	            l[0]=formatter.format(cal.getTime());
          	              
          	            d2.setTime(startDate.getTime() - 2 * 24 * 60 * 60 * 1000);
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[1]=formatter.format(cal.getTime());
          	            
          	            d2.setTime(startDate.getTime()- 3 * 24 * 60 * 60 * 1000);
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[2]=formatter.format(cal.getTime());
          	            
          	            d2.setTime(startDate.getTime());
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[3]=formatter.format(cal.getTime());
      	              
          	            d2.setTime(startDate.getTime());
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[4]=formatter.format(cal.getTime());
          	            
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
            	}
    			else
    			{
    				l[0]=Integer.parseInt(value)-1;
    				l[1]=Integer.parseInt(value)-2;
    				l[2]=Integer.parseInt(value)-3;
    				l[3]=Integer.parseInt(value)+1;
    				l[4]=Integer.parseInt(value)+3;;
    			}
    		}
    		else if(condition[i].contains("equal"))
    		{
    			String value=condition[i].replace("equal", "");
    			
    			if(columns[i].startsWith("D_"))
            	{
    				Date startDate;
                    DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd"); 
                    Calendar cal = Calendar.getInstance();
                    
                    try {
							startDate = (Date) formatter.parse(value);
							cal.setTime(startDate);
                            System.out.println(cal.getTime());
                            Date d2 = new Date();

                        d2.setTime(startDate.getTime() );
          	            cal.setTime(d2);
                        System.out.println(cal.getTime());
          	            l[0]=formatter.format(cal.getTime());
          	              
          	            d2.setTime(startDate.getTime());
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[1]=formatter.format(cal.getTime());
          	            
          	            d2.setTime(startDate.getTime());
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[2]=formatter.format(cal.getTime());
          	            
          	            d2.setTime(startDate.getTime()- 2 * 24 * 60 * 60 * 1000);
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[3]=formatter.format(cal.getTime());
      	              
          	            d2.setTime(startDate.getTime()- 3 * 24 * 60 * 60 * 1000);
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[4]=formatter.format(cal.getTime());
          	            
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
            	}
    			else
    			{
    				l[0]=value;
    				l[1]=value;
    				l[2]=value;
    				l[3]=Integer.parseInt(value)+1;
    				l[4]=Integer.parseInt(value)+1;
    			}
    		}
    		
    		
    		 for(int j=0;j<5;j++)
    	      {	
    	if(columns[i].startsWith("D_"))
    	{
    		sql="UPDATE "+Variables.table+ " set "+columns[i]+" = to_date('" + l[j] + "', 'dd-MM-yyyy') where "
						+Variables.MIScolumn+" = to_date('" + Variables.MIS_date + "', 'dd-MM-yyyy')"+" and "+Variables.primaryKey+" = '"+Variables.pk[j]+ "'";
    		System.out.println(sql);
    	}
    	else 
    	{
    		sql="UPDATE "+Variables.table+ " set "+columns[i]+" = '"+l[j]+"' where "
						+Variables.MIScolumn+" = to_date('" + Variables.MIS_date + "', 'dd-MM-yyyy')"+" and "+Variables.primaryKey+" = '"+Variables.pk[j] + "'";
    		System.out.println(sql);
    	}
    		
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

	private static void updateREF(String filter) {
		// TODO Auto-generated method stub
		Connection conn =null;
		Statement st;
		String rows="";
		String sql="";
		String[] condition={};
		String[] columns={};
		Object[] l={};
		conn = dbConnect.getConnect(Variables.atomUser,Variables.AtomPassword);
		System.out.println("Connection established");
		if(filter.equalsIgnoreCase("global"))
		{
			 columns=Variables.gc;
			 condition=Variables.global_condition;
			 l=new Object[5];
		}
		else if(filter.equalsIgnoreCase("local"))
		{
			 columns=Variables.lc;
			 condition=Variables.local_condition;
			l=new Object[5];
		}
    //Assigning the values according to the global filter

    for(int i=0;i<columns.length;i++) 
    {
    		if(condition[i].contains("null"))
    		{
    			if(columns[i].startsWith("D_"))
            	{
    				l[0]=null;
    				l[1]=null;
    				l[2]="22-01-1991";
    				l[3]="22-01-1991";
    				l[4]="22-01-1991";
            	}
    			else
    			{
    				l[0]=null;
    				l[1]=null;
    				l[2]="11";
    				l[3]="11";
    				l[4]="11";
    			}
    		}
    		else if(condition[i].contains("notequal"))
    		{
    			String value=condition[i].replace("notequal", "");
    			
    			if(columns[i].startsWith("D_"))
            	{
    				Date startDate;
                    DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd"); 
                    Calendar cal = Calendar.getInstance();
                    
                    try {
							startDate = (Date) formatter.parse(value);
							cal.setTime(startDate);
                            System.out.println(cal.getTime());
                            Date d2 = new Date();

                        d2.setTime(startDate.getTime()- 1 * 24 * 60 * 60 * 1000);
          	            cal.setTime(d2);
                        System.out.println(cal.getTime());
          	            l[0]=formatter.format(cal.getTime());
          	              
          	            d2.setTime(startDate.getTime()- 2 * 24 * 60 * 60 * 1000);
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[1]=formatter.format(cal.getTime());
          	            
          	            d2.setTime(startDate.getTime()- 3 * 24 * 60 * 60 * 1000);
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[2]=formatter.format(cal.getTime());
          	            
          	            d2.setTime(startDate.getTime());
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[3]=formatter.format(cal.getTime());
      	              
          	            d2.setTime(startDate.getTime());
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[4]=formatter.format(cal.getTime());
          	            
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
            	}
    			else
    			{
    				l[0]=Integer.parseInt(value)+1;
    				l[1]=Integer.parseInt(value)+1;
    				l[2]=Integer.parseInt(value)+1;
    				l[3]=value;
    				l[4]=value;
    			}
    		}
    		
    		else if(condition[i].contains("not"))
    		{
    			if(columns[i].startsWith("D_"))
            	{
    				l[0]="22-01-1991";
    				l[1]="22-01-1991";
    				l[2]=null;
    				l[3]=null;
    				l[4]=null;
            	}
    			else
    			{
    				l[0]="11";
    				l[1]="11";
    				l[2]=null;
    				l[3]=null;
    				l[4]=null;
    			}
    		}
    		else if(condition[i].contains("greaterequal"))
    		{

    			String value=condition[i].replace("greaterequal", "");
    			
    			if(columns[i].startsWith("D_"))
            	{
    				Date startDate;
                    DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd"); 
                    Calendar cal = Calendar.getInstance();
                    
                    try {
						startDate = (Date) formatter.parse(value);
						cal.setTime(startDate);
                        System.out.println(cal.getTime());
                        Date d2 = new Date();

                    d2.setTime(startDate.getTime());
      	            cal.setTime(d2);
                    System.out.println(cal.getTime());
      	            l[0]=formatter.format(cal.getTime());
      	              
      	            d2.setTime(startDate.getTime() );
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[1]=formatter.format(cal.getTime());
      	            
      	            d2.setTime(startDate.getTime()+ 1 * 24 * 60 * 60 * 1000);
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[2]=formatter.format(cal.getTime());
      	            
      	            d2.setTime(startDate.getTime() - 1 * 24 * 60 * 60 * 1000);
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[3]=formatter.format(cal.getTime());
  	              
      	            d2.setTime(startDate.getTime() - 1 * 24 * 60 * 60 * 1000);
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[4]=formatter.format(cal.getTime());
      	            
				} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
            	}
    			else
    			{
    				l[0]=value;
    				l[1]=value;
    				l[2]=Integer.parseInt(value)+1;
    				l[3]=Integer.parseInt(value)-1;
    				l[4]=Integer.parseInt(value)-1;
    			}
    		}
    		else if(condition[i].contains("lesserequal"))
    		{

    			String value=condition[i].replace("lesserequal", "");
    			
    			if(columns[i].startsWith("D_"))
            	{
    				Date startDate;
                    DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd"); 
                    Calendar cal = Calendar.getInstance();
                    
                    try {
						startDate = (Date) formatter.parse(value);
						cal.setTime(startDate);
                        System.out.println(cal.getTime());
                        Date d2 = new Date();

                    d2.setTime(startDate.getTime());
      	            cal.setTime(d2);
                    System.out.println(cal.getTime());
      	            l[0]=formatter.format(cal.getTime());
      	              
      	            d2.setTime(startDate.getTime() );
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[1]=formatter.format(cal.getTime());
      	            
      	            d2.setTime(startDate.getTime()- 1 * 24 * 60 * 60 * 1000);
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[2]=formatter.format(cal.getTime());
      	            
      	            d2.setTime(startDate.getTime() + 1 * 24 * 60 * 60 * 1000);
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[3]=formatter.format(cal.getTime());
  	              
      	            d2.setTime(startDate.getTime()+ 1 * 24 * 60 * 60 * 1000 );
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[4]=formatter.format(cal.getTime());
      	            
				} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
            	}
    			else
    			{
    				l[0]=value;
    				l[1]=value;
    				l[2]=Integer.parseInt(value)-1;
    				l[3]=Integer.parseInt(value)+1;
    				l[4]=Integer.parseInt(value)+1;
    			}
    		}
    		else if(condition[i].contains("greater"))
    		{
    			String value=condition[i].replace("greater", "");
    			
    			if(columns[i].startsWith("D_"))
            	{
    				Date startDate;
                    DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd"); 
                    Calendar cal = Calendar.getInstance();
                    
                    try {
						startDate = (Date) formatter.parse(value);
						cal.setTime(startDate);
                        System.out.println(cal.getTime());
                        Date d2 = new Date();

                    d2.setTime(startDate.getTime() + 1 * 24 * 60 * 60 * 1000);
      	            cal.setTime(d2);
                    System.out.println(cal.getTime());
      	            l[0]=formatter.format(cal.getTime());
      	              
      	            d2.setTime(startDate.getTime()+ 2 * 24 * 60 * 60 * 1000);
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[1]=formatter.format(cal.getTime());
      	            
      	            d2.setTime(startDate.getTime()+ 2 * 24 * 60 * 60 * 1000);
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[2]=formatter.format(cal.getTime());
      	            
      	            d2.setTime(startDate.getTime());
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[3]=formatter.format(cal.getTime());
  	              
      	            d2.setTime(startDate.getTime());
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[4]=formatter.format(cal.getTime());
      	            
				} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
            	}
    			else
    			{
    				l[0]=Integer.parseInt(value)+1;
    				l[1]=Integer.parseInt(value)+2;
    				l[2]=Integer.parseInt(value)+1;
    				l[3]=Integer.parseInt(value)-2;
    				l[4]=Integer.parseInt(value)-3;;
    			}
    		}
    		else if(condition[i].contains("lesser"))
    		{
    			String value=condition[i].replace("lesser", "");
    			
    			if(columns[i].startsWith("D_"))
            	{
    				Date startDate;
                    DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd"); 
                    Calendar cal = Calendar.getInstance();
                    
                    try {
							startDate = (Date) formatter.parse(value);
							cal.setTime(startDate);
                            System.out.println(cal.getTime());
                            Date d2 = new Date();

                        d2.setTime(startDate.getTime() - 1 * 24 * 60 * 60 * 1000);
          	            cal.setTime(d2);
                        System.out.println(cal.getTime());
          	            l[0]=formatter.format(cal.getTime());
          	              
          	            d2.setTime(startDate.getTime() - 2 * 24 * 60 * 60 * 1000);
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[1]=formatter.format(cal.getTime());
          	            
          	            d2.setTime(startDate.getTime()- 2 * 24 * 60 * 60 * 1000);
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[2]=formatter.format(cal.getTime());
          	            
          	            d2.setTime(startDate.getTime());
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[3]=formatter.format(cal.getTime());
      	              
          	            d2.setTime(startDate.getTime());
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[4]=formatter.format(cal.getTime());
          	            
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
            	}
    			else
    			{
    				l[0]=Integer.parseInt(value)-1;
    				l[1]=Integer.parseInt(value)-2;
    				l[2]=Integer.parseInt(value)-2;
    				l[3]=Integer.parseInt(value)+1;
    				l[4]=Integer.parseInt(value)+3;;
    			}
    		}
    		else if(condition[i].contains("equal"))
    		{
    			String value=condition[i].replace("equal", "");
    			
    			if(columns[i].startsWith("D_"))
            	{
    				Date startDate;
                    DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd"); 
                    Calendar cal = Calendar.getInstance();
                    
                    try {
							startDate = (Date) formatter.parse(value);
							cal.setTime(startDate);
                            System.out.println(cal.getTime());
                            Date d2 = new Date();

                        d2.setTime(startDate.getTime() );
          	            cal.setTime(d2);
                        System.out.println(cal.getTime());
          	            l[0]=formatter.format(cal.getTime());
          	              
          	            d2.setTime(startDate.getTime());
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[1]=formatter.format(cal.getTime());
          	            
          	            d2.setTime(startDate.getTime());
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[2]=formatter.format(cal.getTime());
          	            
          	            d2.setTime(startDate.getTime()- 2 * 24 * 60 * 60 * 1000);
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[3]=formatter.format(cal.getTime());
      	              
          	            d2.setTime(startDate.getTime()- 3 * 24 * 60 * 60 * 1000);
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[4]=formatter.format(cal.getTime());
          	            
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
            	}
    			else
    			{
    				l[0]=value;
    				l[1]=value;
    				l[2]=value;
    				l[3]=Integer.parseInt(value)+1;
    				l[4]=Integer.parseInt(value)+1;
    			}
    		}
    		
    		for(int j=0;j<5;j++)
    	      {		
    	if(columns[i].startsWith("D_"))
    	{
    		sql="UPDATE "+Variables.table+ " set "+columns[i]+" = to_date('" + l[j] + "', 'dd-MM-yyyy') where "
						+Variables.MIScolumn+" = to_date('" + Variables.MIS_date + "', 'dd-MM-yyyy')"+" and "+Variables.primaryKey+" = '"+Variables.pk[j]+"'";
    		System.out.println(sql);
    	}
    	else 
    	{
    		sql="UPDATE "+Variables.table+ " set "+columns[i]+" = '"+l[j]+"' where "
						+Variables.MIScolumn+" = to_date('" + Variables.MIS_date + "', 'dd-MM-yyyy')"+" and "+Variables.primaryKey+" = '"+Variables.pk[j]+"'";
    		System.out.println(sql);
    	}
    		
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

	private static void updateBlank(String filter) {
		// TODO Auto-generated method stub
		Connection conn =null;
		Statement st;
		String rows="";
		String sql="";
		String[] condition={};
		String[] columns={};
		Object[] l={};
		conn = dbConnect.getConnect(Variables.atomUser,Variables.AtomPassword);
		System.out.println("Connection established");
		if(filter.equalsIgnoreCase("global"))
		{
			 columns=Variables.gc;
			 condition=Variables.global_condition;
			 l=new Object[5];
		}
		else if(filter.equalsIgnoreCase("local"))
		{
			 columns=Variables.lc;
			 condition=Variables.local_condition;
			l=new Object[5];
		}
    //Assigning the values according to the global filter
    for(int i=0;i<columns.length;i++) 
    {
    		if(condition[i].contains("null"))
    		{
    			if(columns[i].startsWith("D_"))
            	{
    				l[0]="22-01-1991";
    				l[1]="22-01-1991";
    				l[2]=null;
    				l[3]=null;
    				l[4]="22-01-1991";
            	}
    			else
    			{
    				l[0]="11";
    				l[1]="11";
    				l[2]=null;
    				l[3]=null;
    				l[4]="11";
    			}
    		}
    		else if(condition[i].contains("notequal"))
    		{
    			String value=condition[i].replace("notequal", "");
    			
    			if(columns[i].startsWith("D_"))
            	{
    				Date startDate;
                    DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd"); 
                    Calendar cal = Calendar.getInstance();
                    
                    try {
							startDate = (Date) formatter.parse(value);
							cal.setTime(startDate);
                            System.out.println(cal.getTime());
                            Date d2 = new Date();

                        d2.setTime(startDate.getTime()- 2 * 24 * 60 * 60 * 1000);
          	            cal.setTime(d2);
                        System.out.println(cal.getTime());
          	            l[0]=formatter.format(cal.getTime());
          	              
          	            d2.setTime(startDate.getTime()- 2 * 24 * 60 * 60 * 1000);
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[1]=formatter.format(cal.getTime());
          	            
          	            d2.setTime(startDate.getTime()- 2 * 24 * 60 * 60 * 1000);
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[2]=formatter.format(cal.getTime());
          	            
          	            d2.setTime(startDate.getTime());
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[3]=formatter.format(cal.getTime());
      	              
          	            d2.setTime(startDate.getTime());
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[4]=formatter.format(cal.getTime());
          	            
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
            	}
    			else
    			{
    				l[0]=Integer.parseInt(value)+1;
    				l[1]=Integer.parseInt(value)+1;
    				l[2]=Integer.parseInt(value)+1;
    				l[3]=value;
    				l[4]=value;
    			}
    		}
    		else if(condition[i].contains("not"))
    		{
    			if(columns[i].startsWith("D_"))
            	{
    				l[0]=null;
    				l[1]=null;
    				l[2]="22-01-1991";
    				l[3]="22-01-1991";
    				l[4]=null;
            	}
    			else
    			{
    				l[0]=null;
    				l[1]=null;
    				l[2]="11";
    				l[3]="11";
    				l[4]=null;
    			}
    		}
    		else if(condition[i].contains("greaterequal"))
    		{

    			String value=condition[i].replace("greaterequal", "");
    			
    			if(columns[i].startsWith("D_"))
            	{
    				Date startDate;
                    DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd"); 
                    Calendar cal = Calendar.getInstance();
                    
                    try {
						startDate = (Date) formatter.parse(value);
						cal.setTime(startDate);
                        System.out.println(cal.getTime());
                        Date d2 = new Date();

                    d2.setTime(startDate.getTime()+ 1 * 24 * 60 * 60 * 1000);
      	            cal.setTime(d2);
                    System.out.println(cal.getTime());
      	            l[0]=formatter.format(cal.getTime());
      	              
      	            d2.setTime(startDate.getTime()+ 1 * 24 * 60 * 60 * 1000);
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[1]=formatter.format(cal.getTime());
      	            
      	            d2.setTime(startDate.getTime()+ 3 * 24 * 60 * 60 * 1000);
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[2]=formatter.format(cal.getTime());
      	            
      	            d2.setTime(startDate.getTime()- 1 * 24 * 60 * 60 * 1000);
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[3]=formatter.format(cal.getTime());
  	              
      	            d2.setTime(startDate.getTime() - 1 * 24 * 60 * 60 * 1000);
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[4]=formatter.format(cal.getTime());
      	            
				} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
            	}
    			else
    			{
    				l[0]=value;
    				l[1]=Integer.parseInt(value)+1;
    				l[2]=Integer.parseInt(value)+2;
    				l[3]=Integer.parseInt(value)-1;
    				l[4]=Integer.parseInt(value)-1;
    			}
    		}
    		else if(condition[i].contains("lesserequal"))
    		{

    			String value=condition[i].replace("lesserequal", "");
    			
    			if(columns[i].startsWith("D_"))
            	{
    				Date startDate;
                    DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd"); 
                    Calendar cal = Calendar.getInstance();
                    
                    try {
						startDate = (Date) formatter.parse(value);
						cal.setTime(startDate);
                        System.out.println(cal.getTime());
                        Date d2 = new Date();

                    d2.setTime(startDate.getTime()+ 1 * 24 * 60 * 60 * 1000);
      	            cal.setTime(d2);
                    System.out.println(cal.getTime());
      	            l[0]=formatter.format(cal.getTime());
      	              
      	            d2.setTime(startDate.getTime()+ 1 * 24 * 60 * 60 * 1000);
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[1]=formatter.format(cal.getTime());
      	            
      	            d2.setTime(startDate.getTime());
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[2]=formatter.format(cal.getTime());
      	            
      	            d2.setTime(startDate.getTime());
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[3]=formatter.format(cal.getTime());
  	              
      	            d2.setTime(startDate.getTime()+ 1 * 24 * 60 * 60 * 1000 );
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[4]=formatter.format(cal.getTime());
      	            
				} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
            	}
    			else
    			{
    				l[0]=value;
    				l[1]=Integer.parseInt(value)-1;
    				l[2]=Integer.parseInt(value)-2;
    				l[3]=Integer.parseInt(value)+1;
    				l[4]=Integer.parseInt(value)+2;
    			}
    		}
    		else if(condition[i].contains("greater"))
    		{
    			String value=condition[i].replace("greater", "");
    			
    			if(columns[i].startsWith("D_"))
            	{
    				Date startDate;
                    DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd"); 
                    Calendar cal = Calendar.getInstance();
                    
                    try {
						startDate = (Date) formatter.parse(value);
						cal.setTime(startDate);
                        System.out.println(cal.getTime());
                        Date d2 = new Date();

                    d2.setTime(startDate.getTime()+ 3 * 24 * 60 * 60 * 1000);
      	            cal.setTime(d2);
                    System.out.println(cal.getTime());
      	            l[0]=formatter.format(cal.getTime());
      	              
      	            d2.setTime(startDate.getTime()+ 2 * 24 * 60 * 60 * 1000);
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[1]=formatter.format(cal.getTime());
      	            
      	            d2.setTime(startDate.getTime()+ 1 * 24 * 60 * 60 * 1000);
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[2]=formatter.format(cal.getTime());
      	            
      	            d2.setTime(startDate.getTime()- 1 * 24 * 60 * 60 * 1000);
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[3]=formatter.format(cal.getTime());
  	              
      	            d2.setTime(startDate.getTime());
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[4]=formatter.format(cal.getTime());
      	            
				} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
            	}
    			else
    			{
    				l[0]=Integer.parseInt(value)+1;
    				l[1]=Integer.parseInt(value)+2;
    				l[2]=Integer.parseInt(value)+1;
    				l[3]=Integer.parseInt(value)-2;
    				l[4]=Integer.parseInt(value)-3;;
    			}
    		}
    		else if(condition[i].contains("lesser"))
    		{
    			String value=condition[i].replace("lesser", "");
    			
    			if(columns[i].startsWith("D_"))
            	{
    				Date startDate;
                    DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd"); 
                    Calendar cal = Calendar.getInstance();
                    
                    try {
							startDate = (Date) formatter.parse(value);
							cal.setTime(startDate);
                            System.out.println(cal.getTime());
                            Date d2 = new Date();

                        d2.setTime(startDate.getTime()- 1 * 24 * 60 * 60 * 1000);
          	            cal.setTime(d2);
                        System.out.println(cal.getTime());
          	            l[0]=formatter.format(cal.getTime());
          	              
          	            d2.setTime(startDate.getTime()- 2 * 24 * 60 * 60 * 1000);
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[1]=formatter.format(cal.getTime());
          	            
          	            d2.setTime(startDate.getTime() - 3 * 24 * 60 * 60 * 1000);
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[2]=formatter.format(cal.getTime());
          	            
          	            d2.setTime(startDate.getTime() + 2 * 24 * 60 * 60 * 1000);
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[3]=formatter.format(cal.getTime());
      	              
          	            d2.setTime(startDate.getTime());
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[4]=formatter.format(cal.getTime());
          	            
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
            	}
    			else
    			{
    				l[0]=Integer.parseInt(value)-1;
    				l[1]=Integer.parseInt(value)-2;
    				l[2]=Integer.parseInt(value)-2;
    				l[3]=Integer.parseInt(value)+1;
    				l[4]=Integer.parseInt(value)+3;;
    			}
    		}
    		else if(condition[i].contains("equal"))
    		{
    			String value=condition[i].replace("equal", "");
    			
    			if(columns[i].startsWith("D_"))
            	{
    				Date startDate;
                    DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd"); 
                    Calendar cal = Calendar.getInstance();
                    
                    try {
							startDate = (Date) formatter.parse(value);
							cal.setTime(startDate);
                            System.out.println(cal.getTime());
                            Date d2 = new Date();

                        d2.setTime(startDate.getTime());
          	            cal.setTime(d2);
                        System.out.println(cal.getTime());
          	            l[0]=formatter.format(cal.getTime());
          	              
          	            d2.setTime(startDate.getTime());
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[1]=formatter.format(cal.getTime());
          	            
          	            d2.setTime(startDate.getTime());
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[2]=formatter.format(cal.getTime());
          	            
          	            d2.setTime(startDate.getTime()- 2 * 24 * 60 * 60 * 1000);
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[3]=formatter.format(cal.getTime());
      	              
          	            d2.setTime(startDate.getTime()- 3 * 24 * 60 * 60 * 1000);
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[4]=formatter.format(cal.getTime());
          	            
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
            	}
    			else
    			{
    				l[0]=value;
    				l[1]=value;
    				l[2]=value;
    				l[3]=Integer.parseInt(value)+1;
    				l[4]=Integer.parseInt(value)+1;
    			}
    		}
    		
    		
    		for(int j=0;j<5;j++)
        	{
    	if(columns[i].startsWith("D_"))
    	{
    		sql="UPDATE "+Variables.table+ " set "+columns[i]+" = to_date('" + l[j] + "', 'dd-MM-yyyy') where "
						+Variables.MIScolumn+" = to_date('" + Variables.MIS_date + "', 'dd-MM-yyyy')"+" and "+Variables.primaryKey+" = '"+Variables.pk[j] + "'";
    	}
    	else 
    	{
    		sql="UPDATE "+Variables.table+ " set "+columns[i]+" = '"+l[j]+"' where "
						+Variables.MIScolumn+" = to_date('" + Variables.MIS_date + "', 'dd-MM-yyyy')"+" and "+Variables.primaryKey+" = '"+Variables.pk[j] + "'";
    	}
    		
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

	private static void updateNull(String filter) {
		// TODO Auto-generated method stub
		Connection conn =null;
		Statement st;
		String rows="";
		String sql="";
		String[] condition={};
		String[] columns={};
		Object[] l={};
		conn = dbConnect.getConnect(Variables.atomUser,Variables.AtomPassword);
		System.out.println("Connection established");
		if(filter.equalsIgnoreCase("global"))
		{
			 columns=Variables.gc;
			 condition=Variables.global_condition;
			 l=new Object[5];
		}
		else if(filter.equalsIgnoreCase("local"))
		{
			 columns=Variables.lc;
			 condition=Variables.local_condition;
			l=new Object[5];
		}
    //Assigning the values according to the global filter
    for(int i=0;i<columns.length;i++) 
    {
    		if(condition[i].contains("null"))
    		{
    			if(columns[i].startsWith("D_"))
            	{
    				l[0]=null;
    				l[1]=null;
    				l[2]="22-01-1991";
    				l[3]="22-01-1991";
    				l[4]="22-01-1991";
            	}
    			else
    			{
    				l[0]=null;
    				l[1]=null;
    				l[2]="11";
    				l[3]="11";
    				l[4]="11";
    			}
    		}
    		else if(condition[i].contains("notequal"))
    		{
    			String value=condition[i].replace("notequal", "");
    			
    			if(columns[i].startsWith("D_"))
            	{
    				Date startDate;
                    DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd"); 
                    Calendar cal = Calendar.getInstance();
                    
                    try {
							startDate = (Date) formatter.parse(value);
							cal.setTime(startDate);
                            System.out.println(cal.getTime());
                            Date d2 = new Date();

                        d2.setTime(startDate.getTime()- 1 * 24 * 60 * 60 * 1000);
          	            cal.setTime(d2);
                        System.out.println(cal.getTime());
          	            l[0]=formatter.format(cal.getTime());
          	              
          	            d2.setTime(startDate.getTime()- 2 * 24 * 60 * 60 * 1000);
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[1]=formatter.format(cal.getTime());
          	            
          	            d2.setTime(startDate.getTime()- 2 * 24 * 60 * 60 * 1000);
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[2]=formatter.format(cal.getTime());
          	            
          	            d2.setTime(startDate.getTime());
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[3]=formatter.format(cal.getTime());
      	              
          	            d2.setTime(startDate.getTime());
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[4]=formatter.format(cal.getTime());
          	            
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
            	}
    			else
    			{
    				l[0]=Integer.parseInt(value)+1;
    				l[1]=Integer.parseInt(value)+1;
    				l[2]=Integer.parseInt(value)+1;
    				l[3]=value;
    				l[4]=value;
    			}
    		}
    		else if(condition[i].contains("not"))
    		{
    			if(columns[i].startsWith("D_"))
            	{
    				l[0]="22-01-1991";
    				l[1]="22-01-1991";
    				l[2]=null;
    				l[3]=null;
    				l[4]=null;
            	}
    			else
    			{
    				l[0]="11";
    				l[1]="11";
    				l[2]=null;
    				l[3]=null;
    				l[4]=null;
    			}
    		}
    		else if(condition[i].contains("greaterequal"))
    		{

    			String value=condition[i].replace("greaterequal", "");
    			
    			if(columns[i].startsWith("D_"))
            	{
    				Date startDate;
                    DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd"); 
                    Calendar cal = Calendar.getInstance();
                    
                    try {
						startDate = (Date) formatter.parse(value);
						cal.setTime(startDate);
                        System.out.println(cal.getTime());
                        Date d2 = new Date();

                    d2.setTime(startDate.getTime());
      	            cal.setTime(d2);
                    System.out.println(cal.getTime());
      	            l[0]=formatter.format(cal.getTime());
      	              
      	            d2.setTime(startDate.getTime() );
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[1]=formatter.format(cal.getTime());
      	            
      	            d2.setTime(startDate.getTime()+ 1 * 24 * 60 * 60 * 1000);
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[2]=formatter.format(cal.getTime());
      	            
      	            d2.setTime(startDate.getTime() - 1 * 24 * 60 * 60 * 1000);
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[3]=formatter.format(cal.getTime());
  	              
      	            d2.setTime(startDate.getTime() - 1 * 24 * 60 * 60 * 1000);
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[4]=formatter.format(cal.getTime());
      	            
				} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
            	}
    			else
    			{
    				l[0]=value;
    				l[1]=value;
    				l[2]=Integer.parseInt(value)+1;
    				l[3]=Integer.parseInt(value)-1;
    				l[4]=Integer.parseInt(value)-1;
    			}
    		}
    		else if(condition[i].contains("lesserequal"))
    		{

    			String value=condition[i].replace("lesserequal", "");
    			
    			if(columns[i].startsWith("D_"))
            	{
    				Date startDate;
                    DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd"); 
                    Calendar cal = Calendar.getInstance();
                    
                    try {
						startDate = (Date) formatter.parse(value);
						cal.setTime(startDate);
                        System.out.println(cal.getTime());
                        Date d2 = new Date();

                    d2.setTime(startDate.getTime());
      	            cal.setTime(d2);
                    System.out.println(cal.getTime());
      	            l[0]=formatter.format(cal.getTime());
      	              
      	            d2.setTime(startDate.getTime() );
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[1]=formatter.format(cal.getTime());
      	            
      	            d2.setTime(startDate.getTime()- 1 * 24 * 60 * 60 * 1000);
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[2]=formatter.format(cal.getTime());
      	            
      	            d2.setTime(startDate.getTime() + 1 * 24 * 60 * 60 * 1000);
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[3]=formatter.format(cal.getTime());
  	              
      	            d2.setTime(startDate.getTime()+ 1 * 24 * 60 * 60 * 1000 );
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[4]=formatter.format(cal.getTime());
      	            
				} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
            	}
    			else
    			{
    				l[0]=value;
    				l[1]=value;
    				l[2]=Integer.parseInt(value)-1;
    				l[3]=Integer.parseInt(value)+1;
    				l[4]=Integer.parseInt(value)+1;
    			}
    		}
    		else if(condition[i].contains("greater"))
    		{
    			String value=condition[i].replace("greater", "");
    			
    			if(columns[i].startsWith("D_"))
            	{
    				Date startDate;
                    DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd"); 
                    Calendar cal = Calendar.getInstance();
                    
                    try {
						startDate = (Date) formatter.parse(value);
						cal.setTime(startDate);
                        System.out.println(cal.getTime());
                        Date d2 = new Date();

                    d2.setTime(startDate.getTime() + 1 * 24 * 60 * 60 * 1000);
      	            cal.setTime(d2);
                    System.out.println(cal.getTime());
      	            l[0]=formatter.format(cal.getTime());
      	              
      	            d2.setTime(startDate.getTime()+ 2 * 24 * 60 * 60 * 1000);
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[1]=formatter.format(cal.getTime());
      	            
      	            d2.setTime(startDate.getTime()+ 2 * 24 * 60 * 60 * 1000);
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[2]=formatter.format(cal.getTime());
      	            
      	            d2.setTime(startDate.getTime());
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[3]=formatter.format(cal.getTime());
  	              
      	            d2.setTime(startDate.getTime());
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[4]=formatter.format(cal.getTime());
      	            
				} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
            	}
    			else
    			{
    				l[0]=Integer.parseInt(value)+1;
    				l[1]=Integer.parseInt(value)+2;
    				l[2]=Integer.parseInt(value)+1;
    				l[3]=Integer.parseInt(value)-2;
    				l[4]=Integer.parseInt(value)-3;;
    			}
    		}
    		else if(condition[i].contains("lesser"))
    		{
    			String value=condition[i].replace("lesser", "");
    			
    			if(columns[i].startsWith("D_"))
            	{
    				Date startDate;
                    DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd"); 
                    Calendar cal = Calendar.getInstance();
                    
                    try {
							startDate = (Date) formatter.parse(value);
							cal.setTime(startDate);
                            System.out.println(cal.getTime());
                            Date d2 = new Date();

                        d2.setTime(startDate.getTime() - 1 * 24 * 60 * 60 * 1000);
          	            cal.setTime(d2);
                        System.out.println(cal.getTime());
          	            l[0]=formatter.format(cal.getTime());
          	              
          	            d2.setTime(startDate.getTime() - 2 * 24 * 60 * 60 * 1000);
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[1]=formatter.format(cal.getTime());
          	            
          	            d2.setTime(startDate.getTime()- 3 * 24 * 60 * 60 * 1000);
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[2]=formatter.format(cal.getTime());
          	            
          	            d2.setTime(startDate.getTime());
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[3]=formatter.format(cal.getTime());
      	              
          	            d2.setTime(startDate.getTime());
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[4]=formatter.format(cal.getTime());
          	            
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
            	}
    			else
    			{
    				l[0]=Integer.parseInt(value)-1;
    				l[1]=Integer.parseInt(value)-2;
    				l[2]=Integer.parseInt(value)-2;
    				l[3]=Integer.parseInt(value)+1;
    				l[4]=Integer.parseInt(value)+3;;
    			}
    		}
    		else if(condition[i].contains("equal"))
    		{
    			String value=condition[i].replace("equal", "");
    			
    			if(columns[i].startsWith("D_"))
            	{
    				Date startDate;
                    DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd"); 
                    Calendar cal = Calendar.getInstance();
                    
                    try {
							startDate = (Date) formatter.parse(value);
							cal.setTime(startDate);
                            System.out.println(cal.getTime());
                            Date d2 = new Date();

                        d2.setTime(startDate.getTime() );
          	            cal.setTime(d2);
                        System.out.println(cal.getTime());
          	            l[0]=formatter.format(cal.getTime());
          	              
          	            d2.setTime(startDate.getTime());
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[1]=formatter.format(cal.getTime());
          	            
          	            d2.setTime(startDate.getTime());
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[2]=formatter.format(cal.getTime());
          	            
          	            d2.setTime(startDate.getTime()- 2 * 24 * 60 * 60 * 1000);
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[3]=formatter.format(cal.getTime());
      	              
          	            d2.setTime(startDate.getTime()- 3 * 24 * 60 * 60 * 1000);
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[4]=formatter.format(cal.getTime());
          	            
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
            	}
    			else
    			{
    				l[0]=value;
    				l[1]=value;
    				l[2]=value;
    				l[3]=Integer.parseInt(value)+1;
    				l[4]=Integer.parseInt(value)+1;
    			}
    		}
    		
    		for(int j=0;j<5;j++)
        	{
    	if(columns[i].startsWith("D_"))
    	{
    		sql="UPDATE "+Variables.table+ " set "+columns[i]+" = to_date('" + l[j] + "', 'dd-MM-yyyy') where "
						+Variables.MIScolumn+" = to_date('" + Variables.MIS_date + "', 'dd-MM-yyyy')"+" and "+Variables.primaryKey+" = '"+Variables.pk[j] + "'";
    	System.out.println(sql);
    	}
    	else 
    	{
    		sql="UPDATE "+Variables.table+ " set "+columns[i]+" = '"+l[j]+"' where "
						+Variables.MIScolumn+" = to_date('" + Variables.MIS_date + "', 'dd-MM-yyyy')"+" and "+Variables.primaryKey+" = '"+Variables.pk[j] + "'";
    		System.out.println(sql);
    	}
    		
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

	private static void updateRange(String filter) {
		// TODO Auto-generated method stub
		Connection conn =null;
		Statement st;
		String rows="";
		String sql="";
		String[] condition={};
		String[] columns={};
		Object[] l={};
		conn = dbConnect.getConnect(Variables.atomUser,Variables.AtomPassword);
		System.out.println("Connection established");
		if(filter.equalsIgnoreCase("global"))
		{
			 columns=Variables.gc;
			 condition=Variables.global_condition;
			 l=new Object[5];
		}
		else if(filter.equalsIgnoreCase("local"))
		{
			 columns=Variables.lc;
			 condition=Variables.local_condition;
			l=new Object[5];
		}
    //Assigning the values according to the global filter
    for(int i=0;i<columns.length;i++) 
    {
    		if(condition[i].contains("null"))
    		{
    			if(columns[i].startsWith("D_"))
            	{
    				l[0]=null;
    				l[1]="22-01-1991";
    				l[2]="22-01-1991";
    				l[3]="22-01-1991";
    				l[4]=null;
            	}
    			else
    			{
    				l[0]=null;
    				l[1]="11";
    				l[2]="11";
    				l[3]="11";
    				l[4]=null;
    			}
    		}
    		else if(condition[i].contains("notequal"))
    		{
    			String value=condition[i].replace("notequal", "");
    			
    			if(columns[i].startsWith("D_"))
            	{
    				Date startDate;
                    DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd"); 
                    Calendar cal = Calendar.getInstance();
                    
                    try {
							startDate = (Date) formatter.parse(value);
							cal.setTime(startDate);
                            System.out.println(cal.getTime());
                            Date d2 = new Date();

                        d2.setTime(startDate.getTime()- 3 * 24 * 60 * 60 * 1000);
          	            cal.setTime(d2);
                        System.out.println(cal.getTime());
          	            l[0]=formatter.format(cal.getTime());
          	              
          	            d2.setTime(startDate.getTime()- 2 * 24 * 60 * 60 * 1000);
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[1]=formatter.format(cal.getTime());
          	            
          	            d2.setTime(startDate.getTime()- 1 * 24 * 60 * 60 * 1000);
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[2]=formatter.format(cal.getTime());
          	            
          	            d2.setTime(startDate.getTime());
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[3]=formatter.format(cal.getTime());
      	              
          	            d2.setTime(startDate.getTime());
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[4]=formatter.format(cal.getTime());
          	            
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
            	}
    			else
    			{
    				l[0]=Integer.parseInt(value)+1;
    				l[1]=Integer.parseInt(value)+1;
    				l[2]=Integer.parseInt(value)+1;
    				l[3]=value;
    				l[4]=Integer.parseInt(value)+1;
    			}
    		}
    		else if(condition[i].contains("not"))
    		{
    			if(columns[i].startsWith("D_"))
            	{
    				l[0]="22-01-1991";
    				l[1]=null;
    				l[2]=null;
    				l[3]=null;
    				l[4]="22-01-1991";
            	}
    			else
    			{
    				l[0]="11";
    				l[1]=null;
    				l[2]=null;
    				l[3]=null;
    				l[4]="11";
    			}
    		}
    		else if(condition[i].contains("greaterequal"))
    		{

    			String value=condition[i].replace("greaterequal", "");
    			
    			if(columns[i].startsWith("D_"))
            	{
    				Date startDate;
                    DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd"); 
                    Calendar cal = Calendar.getInstance();
                    
                    try {
						startDate = (Date) formatter.parse(value);
						cal.setTime(startDate);
                        System.out.println(cal.getTime());
                        Date d2 = new Date();

                    d2.setTime(startDate.getTime()+ 3 * 24 * 60 * 60 * 1000);
      	            cal.setTime(d2);
                    System.out.println(cal.getTime());
      	            l[0]=formatter.format(cal.getTime());
      	              
      	            d2.setTime(startDate.getTime() + 2 * 24 * 60 * 60 * 1000);
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[1]=formatter.format(cal.getTime());
      	            
      	            d2.setTime(startDate.getTime() + 1 * 24 * 60 * 60 * 1000);
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[2]=formatter.format(cal.getTime());
      	            
      	            d2.setTime(startDate.getTime() - 1 * 24 * 60 * 60 * 1000);
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[3]=formatter.format(cal.getTime());
  	              
      	            d2.setTime(startDate.getTime()- 1 * 24 * 60 * 60 * 1000 );
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[4]=formatter.format(cal.getTime());
      	            
				} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
            	}
    			else
    			{
    				l[0]=value;
    				l[1]=Integer.parseInt(value)+1;
    				l[2]=Integer.parseInt(value)+1;
    				l[3]=Integer.parseInt(value)-1;
    				l[4]=Integer.parseInt(value)-1;
    			}
    		}
    		else if(condition[i].contains("lesserequal"))
    		{

    			String value=condition[i].replace("lesserequal", "");
    			
    			if(columns[i].startsWith("D_"))
            	{
    				Date startDate;
                    DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd"); 
                    Calendar cal = Calendar.getInstance();
                    
                    try {
						startDate = (Date) formatter.parse(value);
						cal.setTime(startDate);
                        System.out.println(cal.getTime());
                        Date d2 = new Date();

                    d2.setTime(startDate.getTime());
      	            cal.setTime(d2);
                    System.out.println(cal.getTime());
      	            l[0]=formatter.format(cal.getTime());
      	              
      	            d2.setTime(startDate.getTime() - 1 * 24 * 60 * 60 * 1000);
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[1]=formatter.format(cal.getTime());
      	            
      	            d2.setTime(startDate.getTime()- 1 * 24 * 60 * 60 * 1000);
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[2]=formatter.format(cal.getTime());
      	            
      	            d2.setTime(startDate.getTime() + 1 * 24 * 60 * 60 * 1000);
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[3]=formatter.format(cal.getTime());
  	              
      	            d2.setTime(startDate.getTime() + 1 * 24 * 60 * 60 * 1000);
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[4]=formatter.format(cal.getTime());
      	            
				} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
            	}
    			else
    			{
    				l[0]=value;
    				l[1]=Integer.parseInt(value)-1;
    				l[2]=Integer.parseInt(value)-1;
    				l[3]=Integer.parseInt(value)+1;
    				l[4]=Integer.parseInt(value)+1;
    			}
    		}
    		else if(condition[i].contains("greater"))
    		{
    			String value=condition[i].replace("greater", "");
    			
    			if(columns[i].startsWith("D_"))
            	{
    				Date startDate;
                    DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd"); 
                    Calendar cal = Calendar.getInstance();
                    
                    try {
						startDate = (Date) formatter.parse(value);
						cal.setTime(startDate);
                        System.out.println(cal.getTime());
                        Date d2 = new Date();

                    d2.setTime(startDate.getTime() + 1 * 24 * 60 * 60 * 1000);
      	            cal.setTime(d2);
                    System.out.println(cal.getTime());
      	            l[0]=formatter.format(cal.getTime());
      	              
      	            d2.setTime(startDate.getTime() + 3 * 24 * 60 * 60 * 1000);
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[1]=formatter.format(cal.getTime());
      	            
      	            d2.setTime(startDate.getTime()+ 1 * 24 * 60 * 60 * 1000);
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[2]=formatter.format(cal.getTime());
      	            
      	            d2.setTime(startDate.getTime()- 1 * 24 * 60 * 60 * 1000);
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[3]=formatter.format(cal.getTime());
  	              
      	            d2.setTime(startDate.getTime() - 3 * 24 * 60 * 60 * 1000);
      	            cal.setTime(d2);
      	            System.out.println(cal.getTime());
      	            l[4]=formatter.format(cal.getTime());
      	            
				} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
            	}
    			else
    			{
    				l[0]=Integer.parseInt(value)+1;
    				l[1]=Integer.parseInt(value)+3;
    				l[2]=Integer.parseInt(value)+1;
    				l[3]=Integer.parseInt(value)-1;
    				l[4]=Integer.parseInt(value)-3;;
    			}
    		}
    		else if(condition[i].contains("lesser"))
    		{
    			String value=condition[i].replace("lesser", "");
    			
    			if(columns[i].startsWith("D_"))
            	{
    				Date startDate;
                    DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd"); 
                    Calendar cal = Calendar.getInstance();
                    
                    try {
							startDate = (Date) formatter.parse(value);
							cal.setTime(startDate);
                            System.out.println(cal.getTime());
                            Date d2 = new Date();

                        d2.setTime(startDate.getTime() - 1 * 24 * 60 * 60 * 1000);
          	            cal.setTime(d2);
                        System.out.println(cal.getTime());
          	            l[0]=formatter.format(cal.getTime());
          	              
          	            d2.setTime(startDate.getTime()- 3 * 24 * 60 * 60 * 1000);
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[1]=formatter.format(cal.getTime());
          	            
          	            d2.setTime(startDate.getTime()- 1 * 24 * 60 * 60 * 1000 );
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[2]=formatter.format(cal.getTime());
          	            
          	            d2.setTime(startDate.getTime()+ 1 * 24 * 60 * 60 * 1000 );
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[3]=formatter.format(cal.getTime());
      	              
          	            d2.setTime(startDate.getTime() + 3 * 24 * 60 * 60 * 1000);
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[4]=formatter.format(cal.getTime());
          	            
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
            	}
    			else
    			{
    				l[0]=Integer.parseInt(value)-1;
    				l[1]=Integer.parseInt(value)-3;
    				l[2]=Integer.parseInt(value)-1;
    				l[3]=Integer.parseInt(value)+1;
    				l[4]=Integer.parseInt(value)+3;
    			}
    		}
    		else if(condition[i].contains("equal"))
    		{
    			String value=condition[i].replace("equal", "");
    			
    			if(columns[i].startsWith("D_"))
            	{
    				Date startDate;
                    DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd"); 
                    Calendar cal = Calendar.getInstance();
                    
                    try {
							startDate = (Date) formatter.parse(value);
							cal.setTime(startDate);
                            System.out.println(cal.getTime());
                            Date d2 = new Date();

                        d2.setTime(startDate.getTime() );
          	            cal.setTime(d2);
                        System.out.println(cal.getTime());
          	            l[0]=formatter.format(cal.getTime());
          	              
          	            d2.setTime(startDate.getTime());
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[1]=formatter.format(cal.getTime());
          	            
          	            d2.setTime(startDate.getTime());
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[2]=formatter.format(cal.getTime());
          	            
          	            d2.setTime(startDate.getTime()+ 1 * 24 * 60 * 60 * 1000);
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[3]=formatter.format(cal.getTime());
      	              
          	            d2.setTime(startDate.getTime() + 1 * 24 * 60 * 60 * 1000 );
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[4]=formatter.format(cal.getTime());
          	            
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
            	}
    			else
    			{
    				l[0]=value;
    				l[1]=value;
    				l[2]=value;
    				l[3]=Integer.parseInt(value)+1;
    				l[4]=Integer.parseInt(value)+1;
    			}
    		}
    		
    for(int j=0;j<5;j++)
      {
    	if(columns[i].startsWith("D_"))
    	{
    		sql="UPDATE "+Variables.table+ " set "+columns[i]+" = to_date('" + l[j] + "', 'dd-MM-yyyy') where "
						+Variables.MIScolumn+" = to_date('" + Variables.MIS_date + "', 'dd-MM-yyyy')"+" and "+Variables.primaryKey+" = '"+Variables.pk[j] + "'";
    	}
    	else 
    	{
    		sql="UPDATE "+Variables.table+ " set "+columns[i]+" = '"+l[j]+"' where "
						+Variables.MIScolumn+" = to_date('" + Variables.MIS_date + "', 'dd-MM-yyyy')"+" and "+Variables.primaryKey+" = '"+Variables.pk[j] + "'";
    	}
    		
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

	private static void updateDataLength(String filter) {
		// TODO Auto-generated method stub
			Connection conn =null;
			Statement st;
			String rows="";
			String sql="";
			String[] condition={};
			String[] columns={};
			Object[] l={};
			conn = dbConnect.getConnect(Variables.atomUser,Variables.AtomPassword);
			System.out.println("Connection established");
			if(filter.equalsIgnoreCase("global"))
			{
				 columns=Variables.gc;
				 condition=Variables.global_condition;
				 System.out.println("gc cols length " + columns.length);
				 //l=new Object[Variables.gc.length];
				 l = new Object[5];
			}
			else if(filter.equalsIgnoreCase("local"))
			{
				 columns=Variables.lc;
				 condition=Variables.local_condition;
				 System.out.println("lc cols length " + columns.length);
				 l = new Object[5];
			}
        //Assigning the values according to the global filter
        for(int i=0;i<columns.length;i++) 
        {
        		if(condition[i].contains("null"))
        		{
        			if(columns[i].startsWith("D_"))
                	{
        				l[0]=null;
        				l[1]="22-01-1991";
        				l[2]=null;
        				l[3]="22-01-1991";
        				l[4]=null;
                	}
        			else
        			{
        				l[0]=null;
        				l[1]="11";
        				l[2]=null;
        				l[3]="11";
        				l[4]=null;
        			}
        		}
        		else if(condition[i].contains("notequal"))
        		{
        			String value=condition[i].replace("notequal", "");
        			
        			if(columns[i].startsWith("D_"))
                	{
        				Date startDate;
                        DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd"); 
                        Calendar cal = Calendar.getInstance();
                        
                        try {
								startDate = (Date) formatter.parse(value);
								cal.setTime(startDate);
                                System.out.println(cal.getTime());
                                Date d2 = new Date();

                            d2.setTime(startDate.getTime()- 1 * 24 * 60 * 60 * 1000);
              	            cal.setTime(d2);
                            System.out.println(cal.getTime());
              	            l[0]=formatter.format(cal.getTime());
              	              
              	            d2.setTime(startDate.getTime()- 2 * 24 * 60 * 60 * 1000);
              	            cal.setTime(d2);
              	            System.out.println(cal.getTime());
              	            l[1]=formatter.format(cal.getTime());
              	            
              	            d2.setTime(startDate.getTime()- 2 * 24 * 60 * 60 * 1000);
              	            cal.setTime(d2);
              	            System.out.println(cal.getTime());
              	            l[2]=formatter.format(cal.getTime());
              	            
              	            d2.setTime(startDate.getTime());
              	            cal.setTime(d2);
              	            System.out.println(cal.getTime());
              	            l[3]=formatter.format(cal.getTime());
          	              
              	            d2.setTime(startDate.getTime());
              	            cal.setTime(d2);
              	            System.out.println(cal.getTime());
              	            l[4]=formatter.format(cal.getTime());
              	            
						} catch (ParseException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
                	}
        			else
        			{
        				l[0]=Integer.parseInt(value)+1;
        				l[1]=Integer.parseInt(value)+1;
        				l[2]=Integer.parseInt(value)+1;
        				l[3]=value;
        				l[4]=value;
        			}
        		}
        		else if(condition[i].contains("not"))
        		{
        			if(columns[i].startsWith("D_"))
                	{
        				l[0]="22-01-1991";
        				l[1]=null;
        				l[2]="22-01-1991";
        				l[3]=null;
        				l[4]="22-01-1991";
                	}
        			else
        			{
        				l[0]="11";
        				l[1]=null;
        				l[2]="11";
        				l[3]=null;
        				l[4]="11";
        			}
        		}
        		else if(condition[i].contains("greaterequal"))
        		{

        			String value=condition[i].replace("greaterequal", "");
        			
        			if(columns[i].startsWith("D_"))
                	{
        				Date startDate;
                        DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd"); 
                        Calendar cal = Calendar.getInstance();
                        
                        try {
							startDate = (Date) formatter.parse(value);
							cal.setTime(startDate);
                            System.out.println(cal.getTime());
                            Date d2 = new Date();

                        d2.setTime(startDate.getTime());
          	            cal.setTime(d2);
                        System.out.println(cal.getTime());
          	            l[0]=formatter.format(cal.getTime());
          	              
          	            d2.setTime(startDate.getTime() + 1 * 24 * 60 * 60 * 1000);
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[1]=formatter.format(cal.getTime());
          	            
          	            d2.setTime(startDate.getTime()+ 1 * 24 * 60 * 60 * 1000);
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[2]=formatter.format(cal.getTime());
          	            
          	            d2.setTime(startDate.getTime() - 1 * 24 * 60 * 60 * 1000);
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[3]=formatter.format(cal.getTime());
      	              
          	            d2.setTime(startDate.getTime() - 1 * 24 * 60 * 60 * 1000);
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[4]=formatter.format(cal.getTime());
          	            
					} catch (ParseException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
                	}
        			else
        			{
        				l[0]=value;
        				l[1]=Integer.parseInt(value)+1;
        				l[2]=Integer.parseInt(value)+1;
        				l[3]=Integer.parseInt(value)-1;
        				l[4]=Integer.parseInt(value)-1;
        			}
        		}
        		else if(condition[i].contains("lesserequal"))
        		{

        			String value=condition[i].replace("lesserequal", "");
        			
        			if(columns[i].startsWith("D_"))
                	{
        				Date startDate;
                        DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd"); 
                        Calendar cal = Calendar.getInstance();
                        
                        try {
							startDate = (Date) formatter.parse(value);
							cal.setTime(startDate);
                            System.out.println(cal.getTime());
                            Date d2 = new Date();

                        d2.setTime(startDate.getTime());
          	            cal.setTime(d2);
                        System.out.println(cal.getTime());
          	            l[0]=formatter.format(cal.getTime());
          	              
          	            d2.setTime(startDate.getTime() - 1 * 24 * 60 * 60 * 1000);
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[1]=formatter.format(cal.getTime());
          	            
          	            d2.setTime(startDate.getTime());
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[2]=formatter.format(cal.getTime());
          	            
          	            d2.setTime(startDate.getTime() + 1 * 24 * 60 * 60 * 1000);
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[3]=formatter.format(cal.getTime());
      	              
          	            d2.setTime(startDate.getTime() + 1 * 24 * 60 * 60 * 1000);
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[4]=formatter.format(cal.getTime());
          	            
					} catch (ParseException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
                	}
        			else
        			{
        				l[0]=value;
        				l[1]=Integer.parseInt(value)-1;
        				l[2]=value;
        				l[3]=Integer.parseInt(value)+1;
        				l[4]=Integer.parseInt(value)+1;
        			}
        		}
        		else if(condition[i].contains("greater"))
        		{
        			String value=condition[i].replace("greater", "");
        			
        			if(columns[i].startsWith("D_"))
                	{
        				Date startDate;
                        DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd"); 
                        Calendar cal = Calendar.getInstance();
                        
                        try {
							startDate = (Date) formatter.parse(value);
							cal.setTime(startDate);
                            System.out.println(cal.getTime());
                            Date d2 = new Date();

                        d2.setTime(startDate.getTime() + 1 * 24 * 60 * 60 * 1000);
          	            cal.setTime(d2);
                        System.out.println(cal.getTime());
          	            l[0]=formatter.format(cal.getTime());
          	              
          	            d2.setTime(startDate.getTime()+ 1 * 24 * 60 * 60 * 1000);
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[1]=formatter.format(cal.getTime());
          	            
          	            d2.setTime(startDate.getTime() + 2 * 24 * 60 * 60 * 1000);
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[2]=formatter.format(cal.getTime());
          	            
          	            d2.setTime(startDate.getTime() - 1 * 24 * 60 * 60 * 1000);
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[3]=formatter.format(cal.getTime());
      	              
          	            d2.setTime(startDate.getTime() - 3 * 24 * 60 * 60 * 1000);
          	            cal.setTime(d2);
          	            System.out.println(cal.getTime());
          	            l[4]=formatter.format(cal.getTime());
          	            
					} catch (ParseException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
                	}
        			else
        			{
        				l[0]=Integer.parseInt(value)+1;
        				l[1]=Integer.parseInt(value)+1;
        				l[2]=Integer.parseInt(value)+2;
        				l[3]=Integer.parseInt(value)-1;
        				l[4]=Integer.parseInt(value)-3;;
        			}
        		}
        		else if(condition[i].contains("lesser"))
        		{
        			String value=condition[i].replace("lesser", "");
        			
        			if(columns[i].startsWith("D_"))
                	{
        				Date startDate;
                        DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd"); 
                        Calendar cal = Calendar.getInstance();
                        
                        try {
								startDate = (Date) formatter.parse(value);
								cal.setTime(startDate);
                                System.out.println(cal.getTime());
                                Date d2 = new Date();

                            d2.setTime(startDate.getTime() - 1 * 24 * 60 * 60 * 1000);
              	            cal.setTime(d2);
                            System.out.println(cal.getTime());
              	            l[0]=formatter.format(cal.getTime());
              	              
              	            d2.setTime(startDate.getTime()- 1 * 24 * 60 * 60 * 1000);
              	            cal.setTime(d2);
              	            System.out.println(cal.getTime());
              	            l[1]=formatter.format(cal.getTime());
              	            
              	            d2.setTime(startDate.getTime() - 2 * 24 * 60 * 60 * 1000);
              	            cal.setTime(d2);
              	            System.out.println(cal.getTime());
              	            l[2]=formatter.format(cal.getTime());
              	            
              	            d2.setTime(startDate.getTime()+ 1 * 24 * 60 * 60 * 1000);
              	            cal.setTime(d2);
              	            System.out.println(cal.getTime());
              	            l[3]=formatter.format(cal.getTime());
          	              
              	            d2.setTime(startDate.getTime() + 3 * 24 * 60 * 60 * 1000);
              	            cal.setTime(d2);
              	            System.out.println(cal.getTime());
              	            l[4]=formatter.format(cal.getTime());
              	            
						} catch (ParseException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
                	}
        			else
        			{
        				l[0]=Integer.parseInt(value)-1;
        				l[1]=Integer.parseInt(value)-1;
        				l[2]=Integer.parseInt(value)-2;
        				l[3]=Integer.parseInt(value)+1;
        				l[4]=Integer.parseInt(value)+3;;
        			}
        		}
        		else if(condition[i].contains("equal"))
        		{
        			String value=condition[i].replace("equal", "");
        			
        			if(columns[i].startsWith("D_"))
                	{
        				Date startDate;
                        DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd"); 
                        Calendar cal = Calendar.getInstance();
                        
                        try {
								startDate = (Date) formatter.parse(value);
								cal.setTime(startDate);
                                System.out.println(cal.getTime());
                                Date d2 = new Date();

                            d2.setTime(startDate.getTime() );
              	            cal.setTime(d2);
                            System.out.println(cal.getTime());
              	            l[0]=formatter.format(cal.getTime());
              	              
              	            d2.setTime(startDate.getTime());
              	            cal.setTime(d2);
              	            System.out.println(cal.getTime());
              	            l[1]=formatter.format(cal.getTime());
              	            
              	            d2.setTime(startDate.getTime());
              	            cal.setTime(d2);
              	            System.out.println(cal.getTime());
              	            l[2]=formatter.format(cal.getTime());
              	            
              	            d2.setTime(startDate.getTime()+ 1 * 24 * 60 * 60 * 1000);
              	            cal.setTime(d2);
              	            System.out.println(cal.getTime());
              	            l[3]=formatter.format(cal.getTime());
          	              
              	            d2.setTime(startDate.getTime()+ 1 * 24 * 60 * 60 * 1000 );
              	            cal.setTime(d2);
              	            System.out.println(cal.getTime());
              	            l[4]=formatter.format(cal.getTime());
              	            
						} catch (ParseException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
                	}
        			else
        			{
        				l[0]=value;
        				l[1]=value;
        				l[2]=value;
        				l[3]=Integer.parseInt(value)+1;
        				l[4]=Integer.parseInt(value)+1;
        			}
        		}
        	for(int j=0;j<5;j++)
        	{
        		if(columns[i].startsWith("D_"))
            	{
            		sql="UPDATE "+Variables.table+ " set "+columns[i]+" = to_date('" + l[j] + "', 'dd-MM-yyyy') where "
    							+Variables.MIScolumn+" = to_date('" + Variables.MIS_date + "', 'dd-MM-yyyy')"+" and "+Variables.primaryKey+" = '"+Variables.pk[j] + "'";
            	}
            	else 
            	{
            		sql="UPDATE "+Variables.table+ " set "+columns[i]+" = '"+l[j]+"' where "
    							+Variables.MIScolumn+" = to_date('" + Variables.MIS_date + "', 'dd-MM-yyyy')"+" and "+Variables.primaryKey+" = '"+Variables.pk[j]+"'";
            		System.out.println(sql);
            	}
            		
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

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}

