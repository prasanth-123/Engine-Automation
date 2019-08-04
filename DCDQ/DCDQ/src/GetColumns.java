/************************************************************************************************
* Written by					:  	sai.ram.tatiraju@oracle.com
* Class Name					:  	Get Columns
* Class Description			    : 	This class will extract the columns and values out of the
* 									filter expressions.
* Date Created					:  	17-May-2016
* Date Modified					: 	21-June-2016
* Modified by					: 	sai.ram.tatiraju@oracle.com
*************************************************************************************************/

public class GetColumns {

	
	public static void extractcolumns_global()
	{
		String[] global_columns=Variables.global.split("and");
		Variables.gc= new String[Variables.global.split("and").length];
		Variables.global_condition= new String[Variables.global.split("and").length];
	
		//To get the global filter data
		for(int i=0;i<global_columns.length;i++)
		{
			
			//To get the condition of filter
			if(global_columns[i].contains(">="))
			{
				Variables.global_condition[i]="greaterequal"+global_columns[i].split("=")[1].trim().replaceAll("'", "");
			}
			else if(global_columns[i].contains("<="))
			{
				Variables.global_condition[i]="lesserequal"+global_columns[i].split("=")[1].trim().replaceAll("'", "");
			}
			else if(global_columns[i].contains("!="))
			{
				Variables.global_condition[i]="notequal"+global_columns[i].split("=")[1].trim().replaceAll("'", "");
			}
			else if(global_columns[i].contains("="))
			{
				Variables.global_condition[i]="equal"+global_columns[i].split("=")[1].trim().replaceAll("'", "");
			}
			else if(global_columns[i].contains(">"))
			{
				Variables.global_condition[i]="greater"+global_columns[i].split(">")[1].trim().replaceAll("'", "");
			}
			else if(global_columns[i].contains("<"))
			{
				Variables.global_condition[i]="lesser"+global_columns[i].split("<")[1].trim().replaceAll("'", "");
			}
			else if(global_columns[i].toUpperCase().contains("NOT NULL"))
			{
				Variables.global_condition[i]="not";
			}
			else if(global_columns[i].toUpperCase().contains("NULL"))
			{
				Variables.global_condition[i]="null";
			}
				
			//To get the column on which filter is applied
			Variables.gc[i]=global_columns[i].trim().split(" ")[0].trim();
				if(Variables.gc[i].toUpperCase().contains(Variables.table.toUpperCase()))
				{
					Variables.gc[i]=Variables.gc[i].toString().split("\\.")[1];
				}
			System.out.println(Variables.gc[i]);
		}
		
		
	}
	
	public static void extractcolumns_local()
	{
		String[] local_columns=Variables.local.split("and");
		Variables.lc= new String[Variables.local.split("and").length];
		Variables.local_condition= new String[Variables.local.split("and").length];
		//To get the local filter data
		for(int i=0;i<local_columns.length;i++)
		{
			//To get the condition of fiter
			if(local_columns[i].contains(">="))
			{
				Variables.local_condition[i]="greaterequal"+local_columns[i].split("=")[1].trim().replaceAll("'", "");
			}
			else if(local_columns[i].contains("<="))
			{
				Variables.local_condition[i]="lesserequal"+local_columns[i].split("=")[1].trim().replaceAll("'", "");
			}
			else if(local_columns[i].contains("!="))
			{
				Variables.local_condition[i]="notequal"+local_columns[i].split("=")[1].trim().replaceAll("'", "");
			}
			else if(local_columns[i].contains("="))
			{
				Variables.local_condition[i]="equal"+local_columns[i].split("=")[1].trim().replaceAll("'", "");
			}
			else if(local_columns[i].contains(">"))
			{
				Variables.local_condition[i]="greater"+local_columns[i].split(">")[1].trim().replaceAll("'", "");
			}
			else if(local_columns[i].contains("<"))
			{
				Variables.local_condition[i]="lesser"+local_columns[i].split("<")[1].trim().replaceAll("'", "");
			}
			else if(local_columns[i].toUpperCase().contains("NOT NULL"))
			{
				Variables.local_condition[i]="not";
			}
			else if(local_columns[i].toUpperCase().contains("NULL"))
			{
				Variables.local_condition[i]="null";
			}
			
			//To get the column on which filter is applied
				Variables.lc[i]=local_columns[i].split(" ")[0].trim();
				if(Variables.lc[i].toUpperCase().contains(Variables.table.toUpperCase()))
				{
					Variables.lc[i]=Variables.lc[i].toString().split("\\.")[1];
				}
				System.out.println(Variables.lc[i]);
		}
	}
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Variables.global="STG_NON_SEC_EXPOSURES.D_EXPOSURE_START_DATE = '$PARAM1' and STG_NON_SEC_EXPOSURES.V_PROD_CODE = '$PARAM2' ";
		Variables.local="";
		Variables.table="STG_NON_SEC_EXPOSURES";
		for(int i=0;i<Variables.gc.length;i++){
			System.out.println(Variables.gc[i]);
			System.out.println(Variables.global_condition[i]);
		}
	}

}

