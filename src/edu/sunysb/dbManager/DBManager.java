package edu.sunysb.dbManager;



import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;


public class DBManager {
	public Connection connection;
	
	public Connection getConnection() {
		return connection;
	}

	
	public Connection openConnection(){
		try {
			Class.forName("com.mysql.jdbc.Driver");
			connection=DriverManager.getConnection(Constants.MySQLServerURL+"/"+Constants.MySQLDB,Constants.MySQLUsername,Constants.MySQLPassword);
		} catch (SQLException e) {
			
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
		
			e.printStackTrace();
		}
		return connection;
		
	}
	
	public void closeConnection(){
		try {
			connection.close();
		} catch (SQLException e) {
			
			e.printStackTrace();
		}
	}

}
