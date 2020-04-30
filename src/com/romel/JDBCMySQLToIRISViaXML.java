package com.romel;

//Intersystems IRIS JDBC package.
import com.intersystems.jdbc.*;

//The JDBC MySQL driver is added into the classpath.

//Java SQL packages.
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;

//DOM, XML and File packages.
import org.w3c.dom.*;
import javax.xml.parsers.*;
import java.io.File;

/**
 * A Java program that retrieves the employee records from a MySQL schema and writes the results to an xml file. 
 * The records from the resulting xml file is then written to a remote  Intersystems IRIS database. 
 * Uses JDBC drivers for MySQL and IRIS.
 * @author Romel Lopez
 */

public class JDBCMySQLToIRISViaXML {

	public static void main(String[] args) {
		
		MySqlToIrisViaXml myIris = null;
		String sql;
		String strXmlFile = "";
		
		try {
			myIris = new MySqlToIrisViaXml();
			sql = "Select employeeNumber,Concat(lastName, \", \", firstName) As employee, extension, email, officeCode, reportsTo, jobTitle\n" + 
					"From employees";
		
			ResultSet resultSet = myIris.getResultMySql(sql);
		 
			/**If the employees table in MySQL is not empty, 
			 * then continue with the rest of the procedure, ie write to xml and IRIS database.
			 */
			if(resultSet != null || resultSet.isBeforeFirst()) {
				
				//Display employee records from MySQL.
				System.out.println("List of employees from MySQL - classicmodels schema:\n");
				System.out.printf("%-20s %-30s %-12s %-35s %-20s %-20s %-20s\n", "Employee Number", "Employee", "Extension",
						"Email", "Office Code", "Reports To", "Job Title");
				System.out.println("--------------------------------------------------------------------------"
						+"--------------------------------------------------------------------------");
				
				int iResultCount = 0;
				
				while(resultSet.next()) {
					System.out.printf("%-20s %-30s %-12s %-35s %-20s %-20s %-20s\n", resultSet.getString(1), resultSet.getString(2), 
							resultSet.getString(3), resultSet.getString(4), resultSet.getString(5),
							resultSet.getString(6), resultSet.getString(7));
					
					iResultCount ++;
				}//end while
				
				System.out.println("\nNumber of employees retrieved -> " + Integer.toString(iResultCount));
				
			}//end if
		}
		catch(SQLException sqlEx) {
			System.out.println("Exception in -> " +  sqlEx.getMessage());
		}
		catch(Exception ex) {
			System.out.println("Exception in -> " + ex.getMessage());
		}
		finally {
			if(myIris != null) {
				System.out.println("\nConnection released.");
				myIris.closeConnection();
			}
		}

	}
	
}//end class

class MySqlToIrisViaXml {
	
	private String strMySqlUrl = "jdbc:mysql://localhost:3306/classicmodels";//URL of MySql local instance and schema.
	private String strIrisUrl = "jdbc:iris://146.148.90.135:19517/USER";//URL of IRIS external instance and schema.
	
	private String strMySqlUsername = "root";//MySql username.
	private String strMySqlPassword = "mysqlcommunity2020";//MySql password.
	
	private String strIrisUsername = "tech";//Iris username.
	private String strIrisPassword = "demo";//Iris password.
	
	private Connection connection = null;
	
	/**
	 * Connects to MySql server to retrieve records.
	 * @param sql select statement.
	 * @return result of the query.
	 */
	public ResultSet getResultMySql(String sql) {
		
		PreparedStatement preparedStatement= null;
		ResultSet resultSet = null;
		
		try {
			connection = DriverManager.getConnection(strMySqlUrl, strMySqlUsername, strMySqlPassword);
			preparedStatement = connection.prepareStatement(sql);
			resultSet = preparedStatement.executeQuery();
		}
		catch(SQLTimeoutException timeEx) {
			
		}
		catch(SQLException sqlEx) {
			System.out.println("Exception in " + this.getClass().getSimpleName() + " -> " + sqlEx.getMessage());
		}
		catch(Exception ex) {
			System.out.println("Exception in " + this.getClass().getSimpleName() + " -> " + ex.getMessage());
		}
		
		return resultSet;
		
	}//getEmployeesMySql
	
	/**
	 * Close the Connection object.
	 */
	public void closeConnection() {
		try {
			if(connection != null) {
				connection.close();
			}
		}
		catch(Exception ex) {
			System.out.println("Exception in " + this.getClass().getSimpleName() + " - > " + ex.getMessage());
		}
	}//end closeConnection
	
}//end class