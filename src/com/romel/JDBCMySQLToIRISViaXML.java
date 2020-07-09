package com.romel;

//Intersystems IRIS JDBC package.
import com.intersystems.jdbc.*;

//The JDBC MySQL and Intersystems IRIS drivers are added to the buildpath/classpath.

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
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

/**
 * A Java program that retrieves the employee records from a MySQL schema and writes the results to an xml file. 
 * The records from the resulting xml file is then written to a remote  Intersystems IRIS database. 
 * Uses JDBC drivers for MySQL and IRIS.
 * @author Romel Lopez
 */

public class JDBCMySQLToIRISViaXML {

	public static void main(String[] args) {	
		MySqlToIrisViaXml myIris = null;
		String strEmployeeSelect;
		String strPersonSelect;
		String strXmlFile = "/users/kubi/documents/employees2.xml";
		
		try {
			myIris = new MySqlToIrisViaXml("jdbc:mysql://localhost:3306/classicmodels", "root", "mysqlcommunity2020",
					"jdbc:IRIS://104.197.75.13:26180/USER", "tech", "demo");
			
			strEmployeeSelect = "Select employeeNumber,lastName, firstName, extension, email, officeCode, reportsTo, jobTitle\n" + 
					"From employees";
			
			//Display employee records from MySQL database. 
			int i = displayMySqlEmployees(myIris, strEmployeeSelect);
			
			/**If the employees table in MySQL is not empty, 
			 * then continue with the rest of the procedure, ie write to xml and IRIS database.
			 */
			if(i > 0) {
				System.out.println("\nNumber of records fetched -> " + i);
				//Display Person records from IRIS database.
				strPersonSelect = "Select ID, FirstName, LastName, Phonenumber From Demo.Person";
				
				i = displayIrisPerson(myIris, strPersonSelect);
				System.out.println("\nNumber of records fetched -> " + i);
				
				if(myIris.writeEmployeesToXml(myIris.getMySqlResultSet(strEmployeeSelect), strXmlFile)) {
					System.out.println("\nSuccessfully written employee records to file ('firstName', 'lastName', 'extension') -> " + strXmlFile);
					
					if(myIris.writeXmlToIris(strXmlFile) > 0) {
						System.out.println("\nSuccessfully written xml elements to the IRIS database.");
						
						i = displayIrisPerson(myIris, strPersonSelect);
						System.out.println("\nNumber of records fetched -> " + i);
					}
					else {
						System.out.println("\nNo records were added to the IRIS database.");
					}
				}
				else {
					System.out.println("\nUnable to write employee records to XML file -> " + strXmlFile);
				}
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
				System.out.println("\nConnections closed.");
				myIris.closeConnection();
			}
		}
	}
	
	/**
	 * Display the employee records from MySQL database.
	 * @param MySqlToIrisViaXml object
	 * @param SQL statement
	 * @return Number of records retrieved.
	 */
	private static int displayMySqlEmployees(MySqlToIrisViaXml myIris, String sql) {
		ResultSet resultSet = null;
		int iResultCount = 0;//Count the number of records retrieved from the query.
		
		try {
			resultSet = myIris.getMySqlResultSet(sql);
		 
			if(resultSet != null || resultSet.isBeforeFirst()) {
				
				//Display employee records from MySQL.
				System.out.println("\nList of employees from MySQL - classicmodels schema:\n");
				System.out.printf("%-20s %-30s %-12s %-35s %-20s %-20s %-20s\n", "Employee Number", "Employee", "Extension",
						"Email", "Office Code", "Reports To", "Job Title");
				System.out.println("--------------------------------------------------------------------------"
						+"--------------------------------------------------------------------------");
				
				while(resultSet.next()) {
					System.out.printf("%-20s %-30s %-12s %-35s %-20s %-20s %-20s\n", resultSet.getString(1), 
							resultSet.getString(2) + ", " + resultSet.getString(3), 
							resultSet.getString(4), resultSet.getString(5),
							resultSet.getString(6), resultSet.getString(7), 
							resultSet.getString(8));
					
					iResultCount ++;
				}
			}
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
	}
	
	/**
	 * Display the Person records from IRIS database.
	 * @param MySqlToIrisViaXml object
	 * @param SQL statement
	 * @return Number of records retrieved.
	 */
	private static int displayIrisPerson(MySqlToIrisViaXml myIris, String sql) {
		ResultSet resultSet = null;
		int iResultCount = 0;//Count the number of records retrieved from the query.
		
		try {
			resultSet = myIris.getIrisResultSet(sql);
		 
			if(resultSet != null || resultSet.isBeforeFirst()) {
				
				//Display employee records from MySQL.
				System.out.println("\nList of Persons from IRIS - Demo schema:\n");
				System.out.printf("%-20s %-20s %-20s %-20s\n", "ID", "FirstName", "LastName",
						"Phonenumber");
				System.out.println("--------------------------------------------------------------------------"
						+"--------------------------------------------------------------------------");
				
				while(resultSet.next()) {
					System.out.printf("%-20d %-20s %-20s %-20s\n", resultSet.getInt(1), 
							resultSet.getString(2), resultSet.getString(3), 
							resultSet.getString(4));
					
					iResultCount ++;
				}
			}
		}
		catch(SQLException sqlEx) {
			sqlEx.printStackTrace();
			System.out.println("Exception in -> " +  sqlEx.getMessage());
		}
		catch(Exception ex) {
			ex.printStackTrace();
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
	}
	
}//end class

class MySqlToIrisViaXml {
	
	private String strMySqlUrl;//URL of MySql local instance and schema.
	private String strIrisUrl;//URL of IRIS external instance and schema.
	
	private String strMySqlUsername;//MySql username.
	private String strMySqlPassword;//MySql password.
	
	private String strIrisUsername;//Iris username.
	private String strIrisPassword;//Iris password.
	
	private Connection mySqlConnection = null;
	
	private Connection irisConnection = null;
	private IRISDataSource irisDataSource = null;
	
	public MySqlToIrisViaXml(String strMySqlUrl, String strMySqlUsername,String strMySqlPassword, 
			String strIrisUrl, String strIrisUsername, String strIrisPassword) {
		this.strMySqlUrl = strMySqlUrl;
		this.strMySqlUsername = strMySqlUsername;
		this.strMySqlPassword = strMySqlPassword;
		
		this.strIrisUrl = strIrisUrl;
		this.strIrisUsername = strIrisUsername;
		this.strIrisPassword = strIrisPassword;
		
		//Establish connection to MySQL and IRIS databases.
		if(!connectToDatabase()) {
			try {
				throw new Exception();
			}
			catch(Exception ex) {}
		}
	}
	
	//Getters and Setters.
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
	 * Connects to MySQL and IRIS databases.
	 * @return True if connection to MySQL and IRIS databases are successful.
	 */
	public boolean connectToDatabase() {
		boolean boolIsConnectionSuccessful = true;
		
		try {
			//Establishes connection to MySQL database.
			mySqlConnection = DriverManager.getConnection(strMySqlUrl, strMySqlUsername, strMySqlPassword);
			
			//Establishes connection to IRIS database.
			irisDataSource = new IRISDataSource();
			irisDataSource.setURL(strIrisUrl);
			irisDataSource.setUser(strIrisUsername);
			irisDataSource.setPassword(strIrisPassword);
			irisConnection = irisDataSource.getConnection();
		}
		catch(Exception ex) {
			boolIsConnectionSuccessful = false;
			ex.printStackTrace();
		}
		
		return boolIsConnectionSuccessful;
	}

	/**
	 * Using a connection to MySQL, runs a query and returns a resultset.
	 * @param sql select statement.
	 * @return result of the query.
	 */
	public ResultSet getMySqlResultSet(String sql) {
		PreparedStatement preparedStatement= null;
		ResultSet resultSet = null;
		
		try {
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
	}
	
	/**
	 * Using a connection to IRIS, runs a query and returns a resultset.
	 * @param sql select statement.
	 * @return result of the query.
	 */
	public ResultSet getIrisResultSet(String sql) {
		PreparedStatement preparedStatement= null;
		ResultSet resultSet = null;
		
		try {
			preparedStatement = irisConnection.prepareStatement(sql);
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
	}
	
	/**
	 * Reads the employee records from MySQL database and write them to an xml file.
	 * @return True if records were successfully written to an xml file.
	 */
	public boolean writeEmployeesToXml(ResultSet resultSet, String strXmlFile) {
		boolean isWriteToXMLSuccessfull = true;
		
		try {
			DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
			Document document = documentBuilder.newDocument();
			
			//Create the root element.
			Element elePersons = document.createElement("Persons");
			
			//Traverse thru the recordset to create sub elements.
			while(resultSet.next()) {
				Element elePerson = document.createElement("person");
				
				Element eleFirstName = document.createElement("FirstName");
				eleFirstName.appendChild(document.createTextNode(resultSet.getString("firstName")));
				elePerson.appendChild(eleFirstName);
				
				Element eleLastName = document.createElement("LastName");
				eleLastName.appendChild(document.createTextNode(resultSet.getString("lastName")));
				elePerson.appendChild(eleLastName);
				
				Element elePhoneNumber = document.createElement("PhoneNumber");
				elePhoneNumber.appendChild(document.createTextNode(resultSet.getString("extension")));
				elePerson.appendChild(elePhoneNumber);
				
				elePersons.appendChild(elePerson);
			}
			
			document.appendChild(elePersons);
			
			//Persist xml dom to file.
			Transformer transformer = TransformerFactory.newInstance().newTransformer();
			DOMSource domSource = new DOMSource(document);
			transformer.transform(domSource, new StreamResult(new File(strXmlFile)));
		}
		catch(Exception ex) {
			isWriteToXMLSuccessfull = false;
			ex.printStackTrace();
		}
		
		return isWriteToXMLSuccessfull;
	}
	
	/**
	 * Reads employee elements from an xml file and writes to a remote Iris database.
	 * @param strXmlFile The xml file to read from.
	 * @return True if elements are successfully inserted to a remote Iris database.
	 */
	public int writeXmlToIris(String strXmlFile) {
		int iRecordsAffected = 0;
		String strPersonInsert = "Insert Into Demo.Person (FirstName, LastName, Phonenumber) " + 
				"Values (?, ?, ?)";
		
		try {
			PreparedStatement preparedStatement = irisConnection.prepareStatement(strPersonInsert);
			
			DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
			Document document = documentBuilder.parse(strXmlFile);
			document.normalize();
			
			NodeList nodeList = document.getElementsByTagName("person");
			
			for(int i = 0; i < nodeList.getLength(); i ++) {
				Node node = nodeList.item(i);
				
				if(node.getNodeType() == Node.ELEMENT_NODE) {
					Element element = (Element) node;
					
					preparedStatement.setString(1, element.getElementsByTagName("FirstName").item(0).getTextContent());
					preparedStatement.setString(2, element.getElementsByTagName("LastName").item(0).getTextContent());
					preparedStatement.setString(3, element.getElementsByTagName("PhoneNumber").item(0).getTextContent());
					preparedStatement.addBatch();
				}
			}
			
			iRecordsAffected = preparedStatement.executeBatch().length;
		}
		catch(Exception ex) {
			ex.printStackTrace();
		}
		
		return iRecordsAffected;
	}
	
	/**
	 * Close the Connection object.
	 */
	public void closeConnection() {
		try {
			if(mySqlConnection != null) {
				mySqlConnection.close();
			}
			if(irisConnection != null) {
				irisConnection.close();
			}
			if(irisDataSource != null) {
				irisDataSource.getConnection().close();
			}
		}
		catch(Exception ex) {
			System.out.println("Exception in " + this.getClass().getSimpleName() + " - > " + ex.getMessage());
		}
	}
	
}//end class