package Database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Connect implements AutoCloseable {
    private Connection con;

    public Connect(){
        String driverName="org.postgresql.Driver";
        try{
            Class.forName(driverName);
        }catch (ClassNotFoundException e){
            e.printStackTrace();
        }
    }

    public Connection getCon() {
        String url="jdbc:postgresql://192.168.10.118:5432/ignite_demo";
        String user="ignite_master";
        String password="123456";
        con=null;
        try{
            con= DriverManager.getConnection(url,user,password);
            con.setAutoCommit(true);
            System.out.println("Connection success");
        }catch (SQLException e){
            e.printStackTrace();
        }
        return con;
    }

    @Override
    public void close() {
        try{
            if(con!=null)
                con.close();
            System.out.println("Close connection");
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
