/************************************************************************************************
* Written by					:  	sai.ram.tatiraju@oracle.com
* Class Name					:  	LoadDQRule 
* Class Description			    : 	This class will contain the main function in which we call all 
* 									the remaining functions.
* Date Created					:  	13-May-2016
* Date Modified					: 	21-June-2016
* Modified by					: 	sai.ram.tatiraju@oracle.com
*************************************************************************************************/

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;




public class LoadDQRule
{
	
	public static void main(String args[]) throws IOException, ParseException

    {
		Object final_res_arr[];
		int count=0; 
		 
		 BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
	     System.out.print("Enter the atomUserId : ");
	     Variables.atomUser = br.readLine();
			
	     System.out.print("Enter the atomUserpassword : ");
	     Variables.AtomPassword = br.readLine();
	     
	     System.out.print("Enter the configUserId : ");
	     Variables.configUser = br.readLine();
			
	     System.out.print("Enter the configUserpassword : ");
	     Variables.ConfigPassword = br.readLine();
			
	     System.out.print("Enter the infodom : ");
	     Variables.infodom = br.readLine();
			

			
		 /**************** Calling the Functions***********************/
			
	     System.out.println("**************Fn_Readfile calling**************");
	     LoadSQL.Fn_Readfile(Variables.atomUser , Variables.AtomPassword);
	        
		 System.out.println("**************Fn_DQ_Names calling**************");
		 String[] dqNames= ListDQNames.Fn_DQ_Names(Variables.atomUser, Variables.AtomPassword);
		 System.out.println(dqNames.length+" is the length of DQ names");
			
		
		
		 for(int i =0; i<dqNames.length; i++)
		 {
			
			 
		 System.out.println("**************Fn_connectoUnixserver calling**************");
				
		 System.out.println("**************Fn_Check_Type calling**************");
		 //String type =CheckType.Fn_Check_Type(dqNames[i],Variables.atomUser ,Variables.AtomPassword);
		 String type =CheckType.Fn_Check_Type("colcheck_41",Variables.atomUser ,Variables.AtomPassword);
		 
		 System.out.println("The check type is "+  type);
		 
		 
		//Single Check
		 if (Variables.noofcheck.equalsIgnoreCase("single"))
		 {
			
		 System.out.println("**************Fn_getValues calling**************");
		 System.out.println("dq name: "+dqNames[i]);
		 
		 //ObtainValues.Fn_getValues(dqNames[i], type);
		 ObtainValues.Fn_getValues("colcheck_41", type);
			
	     System.out.println("**************Fn_Group_Creation calling**************");
	     //String grp_name = CreateDQGroup.Fn_Group_Creation(dqNames[i], Variables.folder, Variables.infodom, Variables.atomUser , Variables.AtomPassword);
	     String grp_name = CreateDQGroup.Fn_Group_Creation("colcheck_41", Variables.folder, Variables.infodom, Variables.atomUser , Variables.AtomPassword);		
		 
	     System.out.println("**************Fn_Batch_Creation calling**************");
		 String batch_name = CreateBatch.Fn_Batch_Creation(grp_name, Variables.folder , Variables.infodom, Variables.configUser , Variables.ConfigPassword);
			
		 System.out.println("**************Fn_LoadTestData calling**************");
		 LoadUpdateTable.Fn_LoadTestData(Variables.atomUser, Variables.AtomPassword);
		 
		 System.out.println("**************Fn_UpdateMISDate calling**************");
		 LoadUpdateTable.Fn_UpdateMISDate();
		 
		 
		 System.out.println("**************Fn_UpdateTestData calling**************");
		 LoadUpdateTable.Fn_UpdateTestData("02-01-2013",  type);//changed the mis date
		 
		 System.out.println("**************Fn_connectoUnixserver calling**************");
		 Unixserver.Fn_connectoUnixserver(batch_name , Variables.MIS_date);
		 
		 System.out.println("**************Fn_Validate calling**************");
		 //Validation.Fn_Validate(Variables.atomUser, Variables.AtomPassword,type, dqNames[i] );
		 Validation.Fn_Validate(Variables.atomUser, Variables.AtomPassword,type, "colcheck_41" );
		
		
		 }
		 
		 //Multiple Check
		 else if (Variables.noofcheck.equalsIgnoreCase("multiple"))
			 {				
			 	   System.out.println("******************Entering Multiple Check*******************");
			 	   
			 		//splitting Check Type
				 Variables.checktypes = type.split(";");	 
				 System.out.println("The check type is "+  Variables.checktypes[i]);
				 
				   //Get values of range check
				 System.out.println("**************Fn_getValues calling**************");
				 ObtainValues.Fn_getValues(dqNames[i], Variables.checktypes[count]);
				 
				   //Load Test data for Range
				 System.out.println("**************Fn_LoadTestData calling**************");
				 LoadUpdateTable.Fn_LoadTestData(Variables.atomUser, Variables.AtomPassword);
				 
				   //Update MIS of Range
				 System.out.println("**************Fn_UpdateMISDate calling**************");
				 LoadUpdateTable.Fn_UpdateMISDate();
				 
					 //Update Test Data for Range
				 System.out.println("**************Fn_UpdateTestData calling**************");
				 LoadUpdateTable.Fn_UpdateTestData("01-01-2013",  Variables.checktypes[count]);
				 
					 //Validate Range
				System.out.println("**************Fn_Validate calling**************");
				Validation.Fn_Validate(Variables.atomUser, Variables.AtomPassword,Variables.checktypes[count], dqNames[i] );
					
					
					count++;
					
					//Get values of Col_Ref
				System.out.println("**************Fn_getValues calling**************");
				ObtainValues.Fn_getValues(dqNames[i], Variables.checktypes[count]);
				
					//Validate Col_Ref on range Data
				System.out.println("**************Fn_Validate calling**************");
				Validation.Fn_Validate(Variables.atomUser, Variables.AtomPassword,Variables.checktypes[count], dqNames[i] );
					
					//Update Test data for Col_Ref
				System.out.println("**************Fn_UpdateTestData calling**************");
				LoadUpdateTable.Fn_UpdateTestData("02-01-2013",  Variables.checktypes[count]);
					 
					 //Validate Col_Ref on col Data
				System.out.println("**************Fn_Validate calling**************");
				Validation.Fn_Validate(Variables.atomUser, Variables.AtomPassword,Variables.checktypes[count], dqNames[i] ); 	
					
					count=0;
					
					//get values for range
				System.out.println("**************Fn_getValues calling**************");
				ObtainValues.Fn_getValues(dqNames[i], Variables.checktypes[count]);
				 
					//Validate Range on Col Data
				System.out.println("**************Fn_Validate calling**************");
				Validation.Fn_Validate(Variables.atomUser, Variables.AtomPassword,Variables.checktypes[count], dqNames[i] );
				 
				//changing date to 01-01-2013 through MIS query
				System.out.println("**************mis_query calling**************");
				MISQuery.mis_query();
				
				//calling Comb_validation
				System.out.println("**************comb_validation calling**************");
				final_res_arr = comb_validation.validation();
				
				System.out.println("**************Fn_Group_Creation calling**************");
			    String grp_name = CreateDQGroup.Fn_Group_Creation(dqNames[i], Variables.folder, Variables.infodom, Variables.atomUser , Variables.AtomPassword);
							
				System.out.println("**************Fn_Batch_Creation calling**************");
				String batch_name = CreateBatch.Fn_Batch_Creation(grp_name, Variables.folder , Variables.infodom, Variables.configUser , Variables.ConfigPassword);
					
				
				System.out.println("**************Fn_connectoUnixserver calling**************");
				Unixserver.Fn_connectoUnixserver(batch_name , "01-01-2013");
				 
				//Test Result for multiple check
				System.out.println("*********************Test Result calling**********************");
				TestResults.FnTest_results(final_res_arr);
			 }
		 	
		 System.out.println("*********************Multiple check completed**********************");
		 }
    }
}