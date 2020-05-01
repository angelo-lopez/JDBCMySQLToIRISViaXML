package com.romel;

//Intersystems IRIS JDBC package.
import com.intersystems.jdbc.*;

//The JDBC MySQL and Intersystems IRIS drivers are added into the buildpath/classpath.

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
			myIris = new MySqlToIrisViaXml("jdbc:mysql://localhost:3306/classicmodels", "root", "mysqlcommunity2020",
					"jdbc:iris://146.148.90.135:19517/USER", "tech", "demo");
			
			sql = "Select employeeNumber,Concat(lastName, \", \", firstName) As employee, extension, email, officeCode, reportsTo, jobTitle\n" + 
					"From employees";
			
			int i = displayMySqlEmployees(myIris, sql);
			
			/**If the employees table in MySQL is not empty, 
			 * then continue with the rest of the procedure, ie write to xml and IRIS database.
			 */
			if(i > 0) {
				
			}
			else {
				System.out.println("\nThere are no employee records to display.");
			}
		}
		catch(Exception ex) {
			System.out.println("Exception in -> " + ex.getMessage());
		}
		finally {
			if(myIris != null) {
				System.out.println("\nConnection closed.");
				myIris.closeConnection();
			}
		}

	}//end main.
	
	private static int displayMySqlEmployees(MySqlToIrisViaXml myIris, String sql) {
		
		ResultSet resultSet = null;
		int iResultCount = 0;//Count the number of records retrieved from the query.
		
		try {
			resultSet = myIris.getMySqlEmployeeResultSet(sql);
		 
			if(resultSet != null || resultSet.isBeforeFirst()) {
				
				//Display employee records from MySQL.
				System.out.println("List of employees from MySQL - classicmodels schema:\n");
				System.out.printf("%-20s %-30s %-12s %-35s %-20s %-20s %-20s\n", "Employee Number", "Employee", "Extension",
						"Email", "Office Code", "Reports To", "Job Title");
				System.out.println("--------------------------------------------------------------------------"
						+"--------------------------------------------------------------------------");
				
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
			try {
				if(resultSet != null) {
					resultSet.close();
				}
			}
			catch(Exception ex) {
				System.out.println("Exception -> " + ex.getMessage());
			}
		}
		
		return iResultCount;
		
	}//end displayMySqlEmployees.
	
}//end class

class MySqlToIrisViaXml {
	
	private String strMySqlUrl;//URL of MySql local instance and schema.
	private String strIrisUrl;//URL of IRIS external instance and schema.
	
	private String strMySqlUsername;//MySql username.
	private String strMySqlPassword;//MySql password.
	
	private String strIrisUsername;//Iris username.
	private String strIrisPassword;//Iris password.
	
	private Connection mySqlConnection = null;
	
	public MySqlToIrisViaXml(String strMySqlUrl, String strMySqlUsername,String strMySqlPassword, 
			String strIrisUrl, String strIrisUsername, String strIrisPassword) {
		this.strMySqlUrl = strMySqlUrl;
		this.strMySqlUsername = strMySqlUsername;
		this.strMySqlPassword = strMySqlPassword;
		
		this.strIrisUrl = strIrisUrl;
		this.strIrisUsername = strIrisUsername;
		this.strIrisPassword = strIrisPassword;
	}
	
	public String getStrMySqlUrl() {
		return strMySqlUrl;
	}

	public void setStrMySqlUrl(String strMySqlUrl) {
		this.strMySqlUrl = strMySqlUrl;
	}

	public String getStrIrisUrl() {
		return strIrisUrl;
	}

	public void setStrIrisUrl(String strIrisUrl) {
		this.strIrisUrl = strIrisUrl;
	}

	public String getStrMySqlUsername() {
		return strMySqlUsername;
	}

	public void setStrMySqlUsername(String strMySqlUsername) {
		this.strMySqlUsername = strMySqlUsername;
	}

	public String getStrMySqlPassword() {
		return strMySqlPassword;
	}

	public void setStrMySqlPassword(String strMySqlPassword) {
		this.strMySqlPassword = strMySqlPassword;
	}

	public String getStrIrisUsername() {
		return strIrisUsername;
	}

	public void setStrIrisUsername(String strIrisUsername) {
		this.strIrisUsername = strIrisUsername;
	}

	public String getStrIrisPassword() {
		return strIrisPassword;
	}

	public void setStrIrisPassword(String strIrisPassword) {
		this.strIrisPassword = strIrisPassword;
	}

	/**
	 * Connects to MySql server to retrieve records.
	 * @param sql select statement.
	 * @return result of the query.
	 */
	public ResultSet getMySqlEmployeeResultSet(String sql) {
		
		PreparedStatement preparedStatement= null;
		ResultSet resultSet = null;
		
		try {
			mySqlConnection = DriverManager.getConnection(strMySqlUrl, strMySqlUsername, strMySqlPassword);
			preparedStatement = mySqlConnection.prepareStatement(sql);
			resultSet = preparedStatement.executeQuery();
		}
		catch(SQLTimeoutException timeEx) {
			System.out.println("Exception in " + this.getClass().getSimpleName() + " -> " + timeEx.getMessage());
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
			if(mySqlConnection != null) {
				mySqlConnection.close();
			}
		}
		catch(Exception ex) {
			System.out.println("Exception in " + this.getClass().getSimpleName() + " - > " + ex.getMessage());
		}
	}//end closeConnection
	
}//end class