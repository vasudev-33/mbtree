package edu.sunysb.dbManager;



import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;


public class DBManager {
	public static Connection connection;
	
	public Connection getConnection() {
		return connection;
	}

	static{
		openConnection();
	}
	public static void openConnection(){
		try {
			Class.forName("com.mysql.jdbc.Driver");
			connection=DriverManager.getConnection(Constants.MySQLServerURL+"/"+Constants.MySQLDB,Constants.MySQLUsername,Constants.MySQLPassword);
		} catch (SQLException e) {
			
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
		
			e.printStackTrace();
		}
	}
	
	public void closeConnection(){
		try {
			connection.close();
		} catch (SQLException e) {
			
			e.printStackTrace();
		}
	}

}
