import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * 
 */

public class LoadDQRule
{
	
	static String infodom,dbuser,dbpassword,type;
	public static void main(String args[])
	
    {
		 BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
	        try {
	        		System.out.print("Enter the infodom userId : ");
	        		dbuser = br.readLine();
	        		System.out.print("Enter the infodom password : ");
	        		dbpassword = br.readLine();
				} catch (IOException e) {
				e.printStackTrace();
			}
	        LoadSQL.Readfile(dbuser, dbpassword);
	        
    }
	
}
