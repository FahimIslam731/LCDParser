import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.sql.PreparedStatement;


public class DataWriter {
	final String JDBC_DRIVER;
	final String DB_URL;
	final String USER;
	final String PASS;

	Connection connect = null;
	Connection connect2 = null;

	Statement state = null;
	Statement state2 = null;
	ArrayList <String> commandsCPT = new ArrayList<>();
	ArrayList <String> commandsBilling = new ArrayList<>();
	ArrayList <String> commandsContractors = new ArrayList<>();


	public DataWriter(String classpath, String url, String username, String password, String database)
			throws Exception {
		JDBC_DRIVER = classpath;
		DB_URL = url;
		USER = username;
		PASS = password;
		Class.forName(JDBC_DRIVER);
		connect = DriverManager.getConnection(url, USER, PASS);
		state = connect.createStatement();
		initTable(database);
	}
 
	public void initTable(String database) throws Exception {
		String dropCommand = "drop database if exists " + database;
		String createData = "create database if not exists " + database;
		String useData = "use " + database;
		String createTable1 = "create table cpt_to_icd(cpt_code varchar(1000), short_description varchar(1000), long_description varchar(1000), icd_code varchar(1000), icd_description varchar(1000), lcd_id varchar(1000))";
		String createTable2 = "create table billing_code(lcd_id varchar(1000), bill_code_id varchar(1000), description varchar(1000))";
		String createTable3 = "create table contractors(contractor_bus_name varchar(1000), description varchar(1000), contractor_number varchar(1000), sub_type_description varchar(1000), super_mac_description varchar(1000), states varchar(1000))";
		int ret1 = state.executeUpdate(dropCommand);
		int ret2 = state.executeUpdate(createData);
		int ret3 = state.executeUpdate(useData);
		int ret4 = state.executeUpdate(createTable1);
		int ret5 = state.executeUpdate(createTable2);
		int ret6 = state.executeUpdate(createTable3);
	}

	public void insertInto(Map<String, String> map, String table) throws Exception {
		String command;
		String column = "INSERT INTO " + table + " (";
		String values = " VALUES (";
		String columnString = "";
		String valuesString = "";
		for (String key : map.keySet()) {
			columnString += key + ", ";
			valuesString += "\"" + map.get(key) + "\", ";
		}
		columnString = columnString.substring(0, columnString.length() - 2);
		valuesString = valuesString.substring(0, valuesString.length() - 2);
		column += (columnString + ")");
		values += (valuesString + ")");
		command = column + values;
		state.addBatch(command);
		
	}
	
	public void execute() throws SQLException {
		state.executeBatch();
	}
	

	public void close() throws Exception {
		if (state != null) {
			state.close();
		}
		if (connect != null) {
			connect.close();
		}
	}

}
