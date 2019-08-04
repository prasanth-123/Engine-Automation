/************************************************************************************************
* Written by					:  	sai.ram.tatiraju@oracle.com
* Class Name					:  	Variables
* Class Description			    : 	This class will contain all the global variables.
* Date Created					:  	14-April-2016
* Date Modified					: 	23-June-2016
* Modified by					: 	sai.ram.tatiraju@oracle.com
*************************************************************************************************/
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;


public class Variables {
	
	
	public static String table="STG_NON_SEC_EXPOSURES";
    public static String column;
    public static String rule_id;
    public static String min;
    public static String max;
    public static String severity;
    public static String local;
    public static String global;
    public static String onSource;
    public static String min_inclu;
    public static String col_ref_name ;
    public static String col_ref_operator;
    public static String col_fil_type;
    public static String max_inclu;
    public static String lov_value;
    public static String lov_fil_type;
    public static String def_val;
    public static String dup_col;
    public static String assType;
    public static String folder;
    public static String infodom;
    public static String configUser = "ofsaaconf5";
    public static String ConfigPassword = "password123";
    public static String atomUser;
    public static String AtomPassword;
    public static String SysID;
    public static String assValue;
    public static String primaryKey="v_exposure_id";
    public static String MIScolumn = "fic_mis_date";
    public static Object a1=0;
    public static Object a2=0;
    public static Object a3=0;
    public static Object a4=0;
    public static Object a5=0;	
    public static String ref_table;
    public static String ref_column;
    public static String MIS_date;
    public static String[] pk;
    public static boolean date_flag = false;
	public static String[] gc;
	public static String[] global_condition;
	public static String[] lc;
	public static String[] local_condition;
	public static String[] pk_filt;
	public static String val;
	public static Object[] col_vals;
	public static ArrayList <Object> mul_col_vals = new ArrayList<Object>();
	public static ArrayList <String> mul_pk_vals = new ArrayList<String>();
	public static ArrayList <Object> mul_upd_range = new ArrayList<Object>();
	public static ArrayList <Object> mul_upd_col_ref = new ArrayList<Object>();
	public static ArrayList <Object> mul_upd_null = new ArrayList<Object>();
	public static ArrayList <Object> mul_upd_lov = new ArrayList<Object>();
	public static ArrayList <Object> mul_upd_data = new ArrayList<Object>();
	public static ArrayList <Object> mul_upd_ref = new ArrayList<Object>();
	public static int mul_len = 5;
	public static String noofcheck ="single";
	public static String checktypes[]= new String[9] ;
	//public static boolean mulcheck_flag = true;
	
    
	
	
    // This function will find the index of of pk_filt[i](subset of pk[]) in pk[].
	public static int get_index(int i) {
		int var=0;
		for(int k=0; k<5;k++)
		{
			
			if(Variables.pk_filt[i].equalsIgnoreCase(Variables.pk[k]))
				{var =k;
				break;
				}
		}
		return var;
	}
	
	
	//This function will get the assignment value fetched from rev_dq_assignments table
	public static void get_val(String DQ_Name, int n) {
	
	
				
		String query = "select v_assigned_value from rev_dq_assignments where v_chk_identifier = " + n +" and n_condtion_sys_id_num in (select n_rule_sys_id from dq_check_master where v_dq_check_id = '" + DQ_Name + "')";	
		System.out.println(query);
	try {
		Connection conn= null;
		conn = dbConnect.getConnect( Variables.atomUser , Variables.AtomPassword);
		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery(query);
		
		while (rs.next()) {
			 
		Variables.val = rs.getString("v_assigned_value");
		 } 
		System.out.println(val);
	 } 
		 catch (SQLException e) {
		e.printStackTrace();
	}
	
	
	}
	
	//This function will fetch the assignment values when the assignment type is "another column"
	public static void assType() {
	
	
	if(Variables.assType.equalsIgnoreCase("3"))
		
	{
		
		for(int j=0;j<5;j++)
		{
			
			String sql_1 = "select " + Variables.val + " from " + Variables.table + " where " + Variables.primaryKey + " = '" + Variables.pk[j] + "'" ;
			
			try {
				
				Connection conn;
				conn = dbConnect.getConnect( Variables.atomUser , Variables.AtomPassword);
				Statement st = conn.createStatement();
				ResultSet rs = st.executeQuery(sql_1);
				
				while (rs.next()) {
					 
					col_vals[j] = rs.getString(1);
					
				 } 
				System.out.println(col_vals[j]);
			 } 
				 catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		
		
		
	}
	
	
	}
	
	// This function will print the updated column values in case of multiple check
	public static void print() {
		
		for(int i=0;i<mul_col_vals.size();i++)
		{
			System.out.print(mul_col_vals.get(i) + "  ");
		}
		System.out.println();
		for(int i=0;i<mul_pk_vals.size();i++)
		{
			System.out.print(mul_pk_vals.get(i) + " ");
		}
		System.out.println();
		for(int i=0;i<mul_upd_range.size();i++)
		{
			System.out.print(mul_upd_range.get(i) + " ");
		}
		System.out.println();
		for(int i=0;i<mul_upd_null.size();i++)
		{
			System.out.print(mul_upd_null.get(i) + " ");
		}
		System.out.println();
	}

}
