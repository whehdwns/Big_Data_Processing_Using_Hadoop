package bdpuh.assignment9;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

public class UserAdmin {

    public static void main(String[] args) throws IOException {
        
	Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        Table htable = connection.getTable(TableName.valueOf("User"));
            //add
        if(args[0].equals("add")){
            String user_id = args[1];
            String email_id = args[2];
            String mypasswd = args[3];
            String status = args[4];
            String birth_date = args[5];
            String security_question = args[6];
            String security_answer = args[7];
            try{
                Put put = new Put(Bytes.toBytes(user_id));
                put.addColumn(Bytes.toBytes("creds"), Bytes.toBytes("email"), Bytes.toBytes(email_id));
                put.addColumn(Bytes.toBytes("creds"), Bytes.toBytes("password"), Bytes.toBytes(mypasswd));
                put.addColumn(Bytes.toBytes("prefs"), Bytes.toBytes("status"), Bytes.toBytes(status));
                put.addColumn(Bytes.toBytes("prefs"), Bytes.toBytes("date_of_birth"), Bytes.toBytes(birth_date));
                put.addColumn(Bytes.toBytes("prefs"), Bytes.toBytes("security_question"), Bytes.toBytes(security_question));
                put.addColumn(Bytes.toBytes("prefs"), Bytes.toBytes("security_answer"), Bytes.toBytes(security_answer));
                htable.put(put);
                System.out.println("Add User Successfully");
            }finally{
	        htable.close();
                connection.close();
	    }
        }
        //delete
        else if(args[0].equals("delete")){
            String user_id = args[0];
            try{
                Delete delete = new Delete(Bytes.toBytes(user_id));
                htable.delete(delete);
                System.out.println("Delete User Successfully");
            }finally{
                htable.close();
                connection.close();
            }
        }
        //show - retreive information from the table
        else if(args[0].equals("show")){
            String user_id = args[1];
            try{
                Get get = new Get(toBytes(user_id));
                Result result = htable.get(get);
                System.out.println("Show User Successfully");
                print_table_list(result);
            }finally{
                htable.close();
                connection.close();
            }
        }
        //listall
        else if(args[0].equals("listall")){
            try{
                Scan scan = new Scan(); 
                ResultScanner scanner = htable.getScanner(scan); 
                System.out.println("List all Successfully");
                for (Result rr : scanner){
                        print_table_list(rr);
                }
            }finally{
                htable.close();
                connection.close();
            }
        }
        //login
        else if(args[0].equals("login")){
        	String user_id = args[1];
                String mypasswd = args[2];
                String ip = args[3];
            try{
                Get get = new Get(toBytes(user_id));
                Result result = htable.get(get);
                Put put = new Put(Bytes.toBytes(user_id));
                Date date = new Date();
                SimpleDateFormat datestamp = new SimpleDateFormat("yyyy/MM/dd");
                SimpleDateFormat timestamp = new SimpleDateFormat("hh:mm:ss");
                String datest = datestamp.format(date);
                String timest = timestamp.format(date);

                put.addColumn(Bytes.toBytes("lastlogin"), Bytes.toBytes("ip"),Bytes.toBytes(ip));
                put.addColumn(Bytes.toBytes("lastlogin"), Bytes.toBytes("date"),Bytes.toBytes(datest));
                put.addColumn(Bytes.toBytes("lastlogin"), Bytes.toBytes("time"),Bytes.toBytes(timest));

                String user_pass = Bytes.toString( result.getValue(Bytes.toBytes("creds"), Bytes.toBytes("password")) );

                if(mypasswd.equals(user_pass)){
                        System.out.println("Login Successfully");
                        put.addColumn(Bytes.toBytes("lastlogin"), Bytes.toBytes("success"),Bytes.toBytes("yes"));
                }else{
                        System.out.println("Login Fail");
                        put.addColumn(Bytes.toBytes("lastlogin"), Bytes.toBytes("fail"),Bytes.toBytes("no"));
                }
                htable.put(put);
            }finally{
                htable.close();
                connection.close();
            }
        }
    }


    public static void print_table_list(Result result){
        System.out.println("rowid="+ Bytes.toString(result.getRow()));
        System.out.println("creds:email="+
                Bytes.toString( result.getValue(Bytes.toBytes("creds"), Bytes.toBytes("email")) ));
        System.out.println("creds:password="+
                Bytes.toString( result.getValue(Bytes.toBytes("creds"), Bytes.toBytes("password")) ));
        System.out.println("prefs:status="+
                Bytes.toString( result.getValue(Bytes.toBytes("prefs"), Bytes.toBytes("status")) ));
        System.out.println("prefs:date_of_birth="+
                Bytes.toString( result.getValue(Bytes.toBytes("prefs"), Bytes.toBytes("date_of_birth")) ));
        System.out.println("prefs:security_question="+
                Bytes.toString( result.getValue(Bytes.toBytes("prefs"), Bytes.toBytes("security_question")) ));
        System.out.println("prefs:security_answer="+
                Bytes.toString( result.getValue(Bytes.toBytes("prefs"), Bytes.toBytes("security_answer")) ));
        System.out.println("last_login:ip="+
                Bytes.toString( result.getValue(Bytes.toBytes("lastlogin"), Bytes.toBytes("ip")) ));
        System.out.println("last_login:date="+
                Bytes.toString( result.getValue(Bytes.toBytes("lastlogin"), Bytes.toBytes("date")) ));
        System.out.println("last_login:time=" +
                Bytes.toString( result.getValue(Bytes.toBytes("lastlogin"), Bytes.toBytes("time")) ));
        System.out.println("last_login:success=" +
                Bytes.toString( result.getValue(Bytes.toBytes("lastlogin"), Bytes.toBytes("success")) ));
    }
}