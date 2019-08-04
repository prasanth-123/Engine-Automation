import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public class ObtainValues 
{
	
	public static enum Type 
	{
	    RANGE , DATALENGTH , COL_REF, ISNULL , LOV , BLANK , REF , DUPLICATE 
	}
	

	public static void main(String[] args) 
	{
		// TODO Auto-generated method stub
		
	}
	
	public static void getValues(String DQName, String type, String dbName, String dbPassword)
	{
		Connection conn = dbConnect.getConnect(dbName, dbPassword);
		String sql = "Select t.V_DQ_SRC_TBL, t.V_DQ_SRC_COL, t.n_rule_sys_id,";
		String tail=" from dq_check_master t where t.v_dq_check_id='"+DQName+"'";
		String table,column,rule_id,min,max,severity,local,global,onSource,min_inclu,col_ref_name,col_ref_operator,col_fil_type,
		max_inclu,lov_value,lov_fil_type,def_val,dup_col,folder_name="";
		
		switch (Type.valueOf(type.toUpperCase())) {
		
		case DATALENGTH:
			sql = sql+" t.N_MIN_LENGTH, t.N_MAX_LENGTH , t.F_DL_SEVERITY, t.V_DL_WHERE,"
					+ "t.V_RULE_FILTER, t.F_ON_SOURCE , t.V_FOLDER_NAME "+tail;
			
			try {
					Statement st = conn.createStatement();
					ResultSet rs = st.executeQuery(sql);
					table=rs.getString(1);
					column=rs.getString(2);
					rule_id=rs.getString(3);
					min=rs.getString(4);
					max=rs.getString(5);
					severity=rs.getString(6);
					local=rs.getString(7);
					global=rs.getString(8);
					onSource=rs.getString(9);
					folder_name = rs.getString(10);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		case RANGE:
			sql = sql+" t.N_MIN_VALUE, t.F_MIN_INCLUSIVE, t.N_MAX_VALUE, "
					+ "t.F_MAX_INCLUSIVE , t.V_DQ_SEVERITY, t.V_RANGE_WHERE,"
					+ "t.V_RULE_FILTER, t.F_ON_SOURCE, t.V_FOLDER_NAME "+tail;
			
			try {
					Statement st = conn.createStatement();
					ResultSet rs = st.executeQuery(sql);
					table=rs.getString(1);
					column=rs.getString(2);
					rule_id=rs.getString(3);
					min=rs.getString(4);
					min_inclu=rs.getString(5);
					max=rs.getString(6);
					max_inclu=rs.getString(7);
					severity=rs.getString(8);
					local=rs.getString(9);
					global=rs.getString(10);
					onSource=rs.getString(11);
					folder_name = rs.getString(12);
					if(severity.equalsIgnoreCase("I")||severity.equalsIgnoreCase("W"))
					{
						String query =" Select t.V_DEF_VAL "+tail;
						st = conn.createStatement();
						rs = st.executeQuery(query);
						def_val=rs.getString(1);
					}
				
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			break;
		case COL_REF:
			sql = sql+" t.V_COL_REF_NAME, t.V_COL_REF_OPERATOR, t.N_COL_REF_FILTER_TYPE , t.F_CR_SEVERITY, t.V_COL_REF_WHERE,"
					+ "t.V_RULE_FILTER, t.F_ON_SOURCE , t.V_FOLDER_NAME"+tail;
			
			try {
					Statement st = conn.createStatement();
					ResultSet rs = st.executeQuery(sql);
					table=rs.getString(1);
					column=rs.getString(2);
					rule_id=rs.getString(3);
					col_ref_name=rs.getString(4);
					col_ref_operator=rs.getString(4);
					col_fil_type=rs.getString(5);
					severity=rs.getString(5);
					local=rs.getString(6);
					global=rs.getString(7);
					onSource=rs.getString(8);
					folder_name = rs.getString(9);
					if(severity.equalsIgnoreCase("I")||severity.equalsIgnoreCase("W"))
					{
						String query =" Select t.V_COL_REF_VAL "+tail;
						st = conn.createStatement();
						rs = st.executeQuery(query);
						def_val=rs.getString(1);
					}
				
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			break;
		case ISNULL:
			sql = sql+" t.F_NV_SEV, t.V_NV_WHERE, t.V_RULE_FILTER, t.F_ON_SOURCE, t.V_FOLDER_NAME "+tail;
			
			try {
					Statement st = conn.createStatement();
					ResultSet rs = st.executeQuery(sql);
					table=rs.getString(1);
					column=rs.getString(2);
					rule_id=rs.getString(3);
					severity=rs.getString(4);
					local=rs.getString(5);
					global=rs.getString(6);
					onSource=rs.getString(7);
					folder_name = rs.getString(8);
					if(severity.equalsIgnoreCase("I")||severity.equalsIgnoreCase("W"))
					{
						String query =" Select t.V_NVL_DEF_VAL "+tail;
						st = conn.createStatement();
						rs = st.executeQuery(query);
						def_val=rs.getString(1);
					}
				
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			break;
		case LOV:
			sql = sql+" t.V_LOV_VALUES, t.N_LOV_FILTER_TYPE, t.F_LOV_SEVERITY, t.F_LOV_WHERE,"
					+ "t.V_RULE_FILTER, t.F_ON_SOURCE, t.V_FOLDER_NAME "+tail;
			
			try {
					Statement st = conn.createStatement();
					ResultSet rs = st.executeQuery(sql);
					table=rs.getString(1);
					column=rs.getString(2);
					rule_id=rs.getString(3);
					lov_value = rs.getString(4);
					lov_fil_type=rs.getString(5);
					severity=rs.getString(6);
					local=rs.getString(7);
					global=rs.getString(8);
					onSource=rs.getString(9);
					folder_name = rs.getString(10);
					if(severity.equalsIgnoreCase("I")||severity.equalsIgnoreCase("W"))
					{
						String query =" Select t.V_LOV_FRMDEF_VAL "+tail;
						st = conn.createStatement();
						rs = st.executeQuery(query);
						def_val=rs.getString(1);
					}
				
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			break;
		case BLANK:
			sql = sql+" t.F_BV_SEVERITY, t.V_BV_WHERE, t.V_RULE_FILTER, t.F_ON_SOURCE, t.V_FOLDER_NAME "+tail;
			
			try {
					Statement st = conn.createStatement();
					ResultSet rs = st.executeQuery(sql);
					table=rs.getString(1);
					column=rs.getString(2);
					rule_id=rs.getString(3);
					severity=rs.getString(5);
					local=rs.getString(6);
					global=rs.getString(7);
					onSource=rs.getString(8);
					folder_name = rs.getString(9);
					if(severity.equalsIgnoreCase("I")||severity.equalsIgnoreCase("W"))
					{
						String query =" Select t.V_BLK_DEF_VAL "+tail;
						st = conn.createStatement();
						rs = st.executeQuery(query);
						def_val=rs.getString(1);
					}
				
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			break;
		case REF:
			sql = sql+" t.F_RI_SEVERITY, t.V_REF_FILTER, t.V_RULE_FILTER, t.F_ON_SOURCE, t.V_FOLDER_NAME "+tail;
			
			try {
					Statement st = conn.createStatement();
					ResultSet rs = st.executeQuery(sql);
					table=rs.getString(1);
					column=rs.getString(2);
					rule_id=rs.getString(3);
					severity=rs.getString(4);
					local=rs.getString(5);
					global=rs.getString(6);
					onSource=rs.getString(7);
					folder_name = rs.getString(8);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			break;
		case DUPLICATE:
			sql = sql+" t.V_DUP_COL_LIST, t.F_DUP_SEVERITY, t.V_DUP_WHERE, t.V_RULE_FILTER, t.F_ON_SOURCE, t.V_FOLDER_NAME "+tail;
			
			try {
					Statement st = conn.createStatement();
					ResultSet rs = st.executeQuery(sql);
					table=rs.getString(1);
					column=rs.getString(2);
					rule_id=rs.getString(3);
					dup_col=rs.getString(4);
					severity=rs.getString(5);
					local=rs.getString(6);
					global=rs.getString(7);
					onSource=rs.getString(8);
					folder_name = rs.getString(9);
				
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			break;
		}
	}

}
