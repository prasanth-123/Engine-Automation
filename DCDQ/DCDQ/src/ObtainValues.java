/************************************************************************************************
* Written by					:  	sai.ram.tatiraju@oracle.com
* Class Name					:  	Get Columns
* Class Description			    : 	This class will extract the values related to the particular 
* 									rule.
* Date Created					:  	27-May-2016
* Date Modified					: 	21-June-2016
* Modified by					: 	sai.ram.tatiraju@oracle.com
*************************************************************************************************/


import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public class ObtainValues 
{
	
	public static enum Type 
	{
	    RANGE , DATALENGTH , COL_REF, ISNULL , LOV , BLANK , REF , DUPLICATE , GEN
	}
	

	public static void Fn_getValues(String DQName, String type)
	{
		System.out.println("**************getValues starting**************");
		Connection conn = dbConnect.getConnect(Variables.atomUser, Variables.AtomPassword);
		String sql = "Select t.V_DQ_SRC_TBL, t.V_DQ_SRC_COL, t.n_rule_sys_id,";
		String tail=" ,t.V_FOLDER_NAME,t.V_INFODOM, t.N_RULE_SYS_ID from dq_check_master t where t.v_dq_check_id='"+DQName+"'";
	
		
		
		switch (Type.valueOf(type.toUpperCase())) {
		
		case DATALENGTH:
			System.out.println("*************Check type is "+type.toUpperCase());
			sql = sql+" t.N_MIN_LENGTH, t.N_MAX_LENGTH , t.F_DL_SEVERITY, t.V_DL_WHERE,"
					+ "t.V_RULE_FILTER, t.F_ON_SOURCE "+tail;
			System.out.println("*************Check type is "+type.toUpperCase());
			System.out.println(sql);
			
			try {
					Statement st = conn.createStatement();
					ResultSet rs = st.executeQuery(sql);
					if(rs.next())
					{
					Variables.table=rs.getString(1);
					Variables.column=rs.getString(2);
					Variables.rule_id=rs.getString(3);
					Variables.min=rs.getString(4);
					Variables.max=rs.getString(5);
					Variables.severity=rs.getString(6);
					Variables.local=rs.getString(7);
					Variables.global=rs.getString(8);
					Variables.onSource=rs.getString(9);
					Variables.folder=rs.getString(10);
					Variables.infodom=rs.getString(11);
					Variables.SysID = rs.getString(12);
					
					}
					
					
					
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			
			break;
			
		case RANGE:
			System.out.println("*************Check type is "+type.toUpperCase());
			sql = sql+" t.N_MIN_VALUE, t.F_MIN_INCLUSIVE, t.N_MAX_VALUE, "
					+ "t.F_MAX_INCLUSIVE , t.V_DQ_SEVERITY, t.V_RANGE_WHERE,"
					+ "t.V_RULE_FILTER, t.F_ON_SOURCE "+tail;
			System.out.println("*************Check type is "+type.toUpperCase());
			System.out.println(sql);
			
			try {
					Statement st = conn.createStatement();
					ResultSet rs = st.executeQuery(sql);
					if(rs.next())
					{
						Variables.table=rs.getString(1);
						Variables.column=rs.getString(2);
						Variables.rule_id=rs.getString(3);
						Variables.min=rs.getString(4);
						Variables.min_inclu=rs.getString(5);
						Variables.max=rs.getString(6);
						Variables.max_inclu=rs.getString(7);
						Variables.severity=rs.getString(8);
						Variables.local=rs.getString(9);
						Variables.global=rs.getString(10);
						Variables.onSource=rs.getString(11);
						Variables.folder=rs.getString(12);
						Variables.infodom=rs.getString(13);
						Variables.SysID = rs.getString(14);
					}
					
					if(Variables.severity.equalsIgnoreCase("I")||Variables.severity.equalsIgnoreCase("W"))
					{
						String query =" Select t.V_DEF_VAL "+tail;
						st = conn.createStatement();
						rs = st.executeQuery(query);
						if(rs.next())
						Variables.def_val=rs.getString(1);
					}
					
					if(Variables.severity.equalsIgnoreCase("W"))
					{	
						String sql1 = "select v_assignment_type from rev_dq_assignments where n_condtion_sys_id_num in (select n_rule_sys_id from dq_check_master where v_dq_check_id = '" + DQName + "')";
						st = conn.createStatement();
						rs = st.executeQuery(sql1);
						if(rs.next())
						{
							Variables.assType = rs.getString(1);
						}
					}
				
					
			} catch (SQLException e) {
				e.printStackTrace();
			}
			
		
			break;
		case COL_REF:
			
			sql = sql+" t.V_COL_REF_NAME, t.V_COL_REF_OPERATOR, t.N_COL_REF_FILTER_TYPE , t.F_CR_SEVERITY, t.V_COL_REF_WHERE,"
					+ "t.V_RULE_FILTER, t.F_ON_SOURCE "+tail;
			System.out.println("*************Check type is "+type.toUpperCase());
			System.out.println(sql);
		
			
			
			try {
					Statement st = conn.createStatement();
					ResultSet rs = st.executeQuery(sql);
					if(rs.next())
					{
					Variables.table=rs.getString(1);
					Variables.column=rs.getString(2);
					Variables.rule_id=rs.getString(3);
					Variables.col_ref_name=rs.getString(4);
					Variables.col_ref_operator=rs.getString(5);
					Variables.col_fil_type=rs.getString(6);
					Variables.severity=rs.getString(7);
					Variables.local=rs.getString(8);
					Variables.global=rs.getString(9);
					Variables.onSource=rs.getString(10);
					Variables.folder=rs.getString(11);
					Variables.infodom=rs.getString(12);
					Variables.SysID = rs.getString(13);
					//System.out.println("end");
					}
					
					
					if(Variables.severity.equalsIgnoreCase("I")||Variables.severity.equalsIgnoreCase("W"))
					{
						String query =" Select t.V_COL_REF_VAL "+tail;
					
						rs = st.executeQuery(query);
						if(rs.next())
						Variables.def_val=rs.getString(1);
						
					}
					
					String query1 = " select V_ASSIGNED_VALUE from rev_dq_assignments t"
								+ " where t.N_CONDTION_SYS_ID_NUM = '"+Variables.SysID+"'" ;
					

					rs = st.executeQuery(query1);
					if(rs.next())
						Variables.assValue=rs.getString(1);
					
					if(Variables.severity.equalsIgnoreCase("W"))
					{	
						String sql1 = "select v_assignment_type from rev_dq_assignments where n_condtion_sys_id_num in (select n_rule_sys_id from dq_check_master where v_dq_check_id = '" + DQName + "')";
						st = conn.createStatement();
						rs = st.executeQuery(sql1);
						if(rs.next())
						{
							Variables.assType = rs.getString(1);
						}
					}
			
					
					
			
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			

			
			break;
		case ISNULL:
			sql = sql+" t.F_NV_SEV, t.V_NV_WHERE, t.V_RULE_FILTER, t.F_ON_SOURCE "+tail;
			System.out.println("*************Check type is "+type.toUpperCase());
			System.out.println(sql);
			
			try {
					Statement st = conn.createStatement();
					ResultSet rs = st.executeQuery(sql);
					if(rs.next())
					{
					Variables.table=rs.getString(1);
					Variables.column=rs.getString(2);
					Variables.rule_id=rs.getString(3);
					Variables.severity=rs.getString(4);
					Variables.local=rs.getString(5);
					Variables.global=rs.getString(6);
					Variables.onSource=rs.getString(7);
					Variables.folder=rs.getString(8);
					Variables.infodom=rs.getString(9);
					Variables.SysID = rs.getString(10);
					}
					System.out.println("***" + Variables.severity);
					
					
					if(Variables.severity.equalsIgnoreCase("W")||Variables.severity.equalsIgnoreCase("I"))
					{	
						String sql1 = "select v_assignment_type from rev_dq_assignments where n_condtion_sys_id_num in (select n_rule_sys_id from dq_check_master where v_dq_check_id = '" + DQName + "')";
						st = conn.createStatement();
						rs = st.executeQuery(sql1);
						if(rs.next())
						{
							Variables.assType = rs.getString(1);
						}
					}
				
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			break;
		case LOV:
			sql = sql+" t.V_LOV_VALUES, t.N_LOV_FILTER_TYPE, t.F_LOV_SEVERITY, t.F_LOV_WHERE,"
					+ "t.V_RULE_FILTER, t.F_ON_SOURCE "+tail;
			System.out.println("*************Check type is "+type.toUpperCase());
			System.out.println(sql);
			
			try {
					Statement st = conn.createStatement();
					ResultSet rs = st.executeQuery(sql);
					if(rs.next())
					{
					Variables.table=rs.getString(1);
					Variables.column=rs.getString(2);
					Variables.rule_id=rs.getString(3);
					Variables.lov_value = rs.getString(4);
					Variables.lov_fil_type=rs.getString(5);
					Variables.severity=rs.getString(6);
					Variables.local=rs.getString(7);
					Variables.global=rs.getString(8);
					Variables.onSource=rs.getString(9);
					Variables.folder=rs.getString(10);
					Variables.infodom=rs.getString(11);
					Variables.SysID = rs.getString(12);
					}
					System.out.println(Variables.severity);
					if(Variables.severity.equalsIgnoreCase("I")||Variables.severity.equalsIgnoreCase("W"))
					{
						String query =" Select t.V_LOV_FRMDEF_VAL "+tail;
						st = conn.createStatement();
						rs = st.executeQuery(query);
						if(rs.next())
						Variables.def_val=rs.getString(1);
					}
					
					if(Variables.severity.equalsIgnoreCase("W"))
					{	
						String sql1 = "select v_assignment_type from rev_dq_assignments where n_condtion_sys_id_num in (select n_rule_sys_id from dq_check_master where v_dq_check_id = '" + DQName + "')";
						st = conn.createStatement();
						rs = st.executeQuery(sql1);
						if(rs.next())
						{
							Variables.assType = rs.getString(1);
						}
					}
				
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			break;
		case BLANK:
			sql = sql+" t.F_BV_SEVERITY, t.V_BV_WHERE, t.V_RULE_FILTER, t.F_ON_SOURCE "+tail;
			System.out.println("*************Check type is "+type.toUpperCase());
			System.out.println(sql);
			
			try {
					Statement st = conn.createStatement();
					ResultSet rs = st.executeQuery(sql);
					if(rs.next())
					{
					Variables.table=rs.getString(1);
					Variables.column=rs.getString(2);
					Variables.rule_id=rs.getString(3);
					Variables.severity=rs.getString(4);
					Variables.local=rs.getString(5);
					Variables.global=rs.getString(6);
					Variables.onSource=rs.getString(7);
					Variables.folder=rs.getString(8);
					Variables.infodom=rs.getString(9);
					Variables.SysID = rs.getString(10);
					}
					if(Variables.severity.equalsIgnoreCase("I")||Variables.severity.equalsIgnoreCase("W"))
					{
						String query =" Select t.V_BLK_DEF_VAL "+tail;
						st = conn.createStatement();
						rs = st.executeQuery(query);
						if(rs.next())
						Variables.def_val=rs.getString(1);
					}
					
					if(Variables.severity.equalsIgnoreCase("W"))
					{	
						String sql1 = "select v_assignment_type from rev_dq_assignments where n_condtion_sys_id_num in (select n_rule_sys_id from dq_check_master where v_dq_check_id = '" + DQName + "')";
						st = conn.createStatement();
						rs = st.executeQuery(sql1);
						if(rs.next())
						{
							Variables.assType = rs.getString(1);
						}
					}
				
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			break;
		case REF:
			sql = sql+" t.F_RI_SEVERITY, t.V_REF_FILTER, t.V_RULE_FILTER, t.F_ON_SOURCE, t.v_ref_table, t.v_ref_column "+tail;
			System.out.println("*************Check type is "+type.toUpperCase());
			System.out.println(sql);
			
			try {
					Statement st = conn.createStatement();
					ResultSet rs = st.executeQuery(sql);
					if(rs.next())
					{
					Variables.table=rs.getString(1);
					Variables.column=rs.getString(2);
					Variables.rule_id=rs.getString(3);
					Variables.severity=rs.getString(4);
					Variables.local=rs.getString(5);
					Variables.global=rs.getString(6);
					Variables.onSource=rs.getString(7);
					Variables.ref_table= rs.getString(8);
					Variables.ref_column= rs.getString(9);
					Variables.folder=rs.getString(10);
					Variables.infodom=rs.getString(11);
					Variables.SysID = rs.getString(12);
					}
					
					
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			break;
		case DUPLICATE:
			sql = sql+" t.V_DUP_COL_LIST, t.F_DUP_SEVERITY, t.V_DUP_WHERE, t.V_RULE_FILTER, t.F_ON_SOURCE "+tail;
			System.out.println("*************Check type is "+type.toUpperCase());
			System.out.println(sql);
			
			try {
					Statement st = conn.createStatement();
					ResultSet rs = st.executeQuery(sql);
					if(rs.next())
					{
					Variables.table=rs.getString(1);
					Variables.column=rs.getString(2);
					Variables.rule_id=rs.getString(3);
					Variables.dup_col=rs.getString(4);
					Variables.severity=rs.getString(5);
					Variables.local=rs.getString(6);
					Variables.global=rs.getString(7);
					Variables.onSource=rs.getString(8);
					Variables.folder=rs.getString(9);
					Variables.infodom=rs.getString(10);
					Variables.SysID = rs.getString(11);
					}
				
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			break;
			
		case GEN:
			sql = sql+" t.v_access_cd, t.V_RULE_FILTER, t.F_ON_SOURCE "+tail;
			System.out.println("*************Check type is "+type.toUpperCase());
			System.out.println(sql);
			try {
				Statement st = conn.createStatement();
				ResultSet rs = st.executeQuery(sql);
				if(rs.next())
				{
				Variables.table=rs.getString(1);
				Variables.column=rs.getString(2);
				Variables.rule_id=rs.getString(3);
				Variables.severity=rs.getString(4);
				Variables.global=rs.getString(5);
				Variables.onSource=rs.getString(6);
				Variables.folder=rs.getString(7);
				Variables.infodom=rs.getString(8);
				Variables.SysID = rs.getString(9);
				}
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		}
	
	}
	
	
public static void main(String[] args) {
	Variables.atomUser= "ofsatmcap5";
	Variables.AtomPassword ="password123";
	Fn_getValues("gencheck_3", "GEN") ;
		
	}
	

}
