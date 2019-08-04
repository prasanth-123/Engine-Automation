/************************************************************************************************
* Written by					:  	sai.ram.tatiraju@oracle.com
* Class Name					:  	comb_Validation
* Class Description			    : 	This class will have the validation functions for all the
* 									Combinational specific checks in DQ.
* Date Created					:  	27-April-2016
* Date Modified					: 	28-June-2016
* Modified by					: 	sai.ram.tatiraju@oracle.com
*************************************************************************************************/
public class comb_validation {

	
	public static Object[] validation() {
		

		Object[] arr_col_vals = new Object[10];
		String[] arr_pk_vals = new String[10];
		Object[] arr_check_1 = new Object[10];
		Object[] arr_check_2 = new Object[10];
		
		System.out.println("Final updated values are");
		for(int i=0;i<10;i++){
			
		/*	if(check_1.equalsIgnoreCase("DATALENGTH"))
				arr_check_1[i] = mul_upd_data.get(i);
			else if (check_1.equalsIgnoreCase("COL_REF"))
				arr_check_1[i] = mul_upd_col_ref.get(i);
			else if(check_1.equalsIgnoreCase("LOV"))
				arr_check_1[i] = mul_upd_lov.get(i);
			else if(check_1.equalsIgnoreCase("NULL"))
				arr_check_1[i] = mul_upd_null.get(i);
			else if(check_1.equalsIgnoreCase("REF"))
				
			*/
			
			arr_col_vals[i]= Variables.mul_col_vals.get(i);
			arr_pk_vals[i] = Variables.mul_pk_vals.get(i);
			arr_check_1[i] = Variables.mul_upd_range.get(i);
			arr_check_2[i] = Variables.mul_upd_null.get(i);
			
			//System.out.print(arr_col_vals[i] + " " + arr_check_1[i] + " " + arr_check_2[i]);
			
			
			
			if(arr_check_1[i]!=arr_col_vals[i]){
				
				if(arr_check_2[i]!=arr_col_vals[i]){
					
					arr_col_vals[i]=arr_check_2[i];
					//System.out.print(arr_col_vals[i]);
				}
				
				else if(arr_check_2[i]==arr_col_vals[i]){
					
					arr_col_vals[i]=arr_check_1[i];
					//System.out.print(arr_col_vals[i]);
				}
			}
			
			else if(arr_check_1[i]==arr_col_vals[i]){
				//System.out.print(arr_col_vals[i]);
				arr_col_vals[i]=arr_check_2[i];
				
			}
			
			System.out.print( " "+arr_col_vals[i] + " ");
			//System.out.println("");
		}
		
		System.out.println("");
		
		return arr_col_vals;
	}

}
