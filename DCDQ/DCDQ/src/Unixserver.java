/************************************************************************************************
* Written by					:  	sai.ram.tatiraju@oracle.com
* Class Name					:  	Unixserver 
* Class Description			    : 	This class will connect to the unix server and execute the shell 
* 									script in the specified path. The shell script will in turn call
*                                   the three batch shell script files in order to run the batch.
* Date Created					:  	23-April-2016
* Date Modified					: 	21-June-2016
* Modified by					: 	sai.ram.tatiraju@oracle.com
*************************************************************************************************/
import java.io.InputStream;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;




public class Unixserver {

	
	public static void Fn_connectoUnixserver(String batch_name, String date) throws ParseException{
		
		DateFormat df1 = new SimpleDateFormat("dd-MM-yyyy");
		DateFormat df2 = new SimpleDateFormat("MM-dd-yyyy");
		
		date = df2.format(df1.parse(date));
		
    	String host="10.184.155.50"; 
        String user="ofsaaweb";
        String password="ficweb123";
        String command1= "$HOME/xyz.sh " + date + " " + batch_name ;
        
        try{
             
            java.util.Properties config = new java.util.Properties();
            config.put("StrictHostKeyChecking", "no");
            JSch jsch = new JSch();
            Session session=jsch.getSession(user, host, 22);
            session.setPassword(password);
            session.setConfig(config);
            session.connect();
            System.out.println("Connected");
             
            Channel channel=session.openChannel("exec");
            ((ChannelExec)channel).setCommand(command1);
            channel.setInputStream(null);
            ((ChannelExec)channel).setErrStream(System.err);
            InputStream in=channel.getInputStream();
            channel.connect();
            byte[] tmp=new byte[1024];
            while(true){
              while(in.available()>0){
                int i=in.read(tmp, 0, 1024);
                if(i<0)
                {
                	System.out.print("*******compare complete********");	
                	break;
                }
                //System.out.print("***************");
                
               String a =  new String(tmp, 0, i);
               System.out.println(a); //.replaceAll("--- a", "Source file location->"));
               
                }

              if(channel.isClosed()){
                System.out.println("exit-status: "+channel.getExitStatus());
                break;
                }
             
           }
            channel.disconnect();
            session.disconnect();
            System.out.println("DONE");
        }   
        	catch(Exception e){
            e.printStackTrace();
           }
 
  
		
	}
	
	
	public static void main(String[] args) throws ParseException 
	{
		 Fn_connectoUnixserver("OFSCAPADQINFO_Range_col_ref_01_Batch", "01-02-2013");

	}
	
}
