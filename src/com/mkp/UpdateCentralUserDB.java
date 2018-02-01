package com.mkp;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.commons.cli.*;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import com.mchange.v2.c3p0.ComboPooledDataSource;

public class UpdateCentralUserDB {

	private static final String DB_DRIVER = "com.mysql.jdbc.Driver";
	private static String DB_NAME;
	private static String DB_USER;
	private static String DB_PASSWORD;
	private static String DB_CONNECTION;

	private static ComboPooledDataSource cpds;

	private static List<String> surveyFACTableNames;
	private static HashMap<Integer, String> renewalEstimatedDateRecords = new HashMap<Integer, String>();
	private static CommandLine cmd;
	private static boolean updateAll;
	private static int sidToUpdate=0;
	private static boolean procesado = false;
	
	
	public static void main(String[] argv) {

		long startTime = System.nanoTime();
		
		getCommandLineArguments(argv);
        
		System.out.println ("---- INICIO DEL PROCESO --- ");

		settingUpConnectionPool();

		surveyFACTableNames = getFACSurveyTableNames();

		registerSurveysData();

		if(updateAll) {	
		
			updateAllDatafromFAC();
			procesado = true;
		
		}else if(sidToUpdate != 0 ) {
			
			updateDatafromFACByTableName("survey_"+sidToUpdate );
			procesado = true;
		}
			
		
		if(!procesado) {
			System.out.println("Debe solicitar al menos una accion");
		}

		long endTime = System.nanoTime();
		long duration = (endTime - startTime);

		double seconds = (double) duration / 1000000000.0;

		System.out.println("---- FINALIZACION DEL PROCESO --- en " + seconds + "s");
	}

	
	private static void getCommandLineArguments(String[] argv) {
		
		Options options = new Options();

        Option allInput = new Option("a", "all", false, "Update all factory campaigns");
        allInput.setRequired(false);
        
        Option sidInput = new Option("id", "sid", true, "Update only factory campaign with this sid");
        sidInput.setRequired(false);
        
        options.addOption(allInput);
        options.addOption(sidInput);
        
        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        
        try {
            cmd = parser.parse(options, argv);
            
            if(cmd.hasOption("a")) 
            	updateAll=true;
            
            if(cmd.hasOption("sid"))
            	sidToUpdate = new Integer(cmd.getOptionValue("sid")).intValue();
            
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("UpdateCentralUserDB-usage", options);

            System.exit(1);
            return;
        }
	}
	
	private static void settingUpConnectionPool() {

		readingPropsFile();

		try {
			cpds = new ComboPooledDataSource();
			cpds.setDriverClass(DB_DRIVER);
			cpds.setJdbcUrl(DB_CONNECTION);
			cpds.setUser(DB_USER);
			cpds.setPassword(DB_PASSWORD);

			// the settings below are optional -- c3p0 can work with defaults
			cpds.setMinPoolSize(5);
			cpds.setAcquireIncrement(5);
			cpds.setMaxPoolSize(50);
		} catch (Exception e) {
			System.out.println("Error setting up connection pool definition!");
		}
	}

	private static void readingPropsFile() {

		InputStream input = null;

		try {

			Properties prop = new Properties();
			String propFileName = "config.properties";

			input = UpdateCentralUserDB.class.getClassLoader().getResourceAsStream(propFileName);

			if (input != null) {
				prop.load(input);
			} else {
				throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
			}

			DB_NAME = prop.getProperty("DB_NAME");
			DB_USER = prop.getProperty("DB_USER");
			DB_PASSWORD = prop.getProperty("DB_PASSWORD");
			DB_CONNECTION = "jdbc:mysql://localhost:3306/" + DB_NAME;

		} catch (IOException ex) {
			ex.printStackTrace();
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	private static void registerSurveysData() {

		for (String surveyTableName : surveyFACTableNames)
			registerSurveyData(surveyTableName);
	}

	private static List<String> getFACSurveyTableNames() {

		Connection dbConnection = null;
		Statement statement = null;

		String getFACTableNameSQL = "select surveyls_survey_id from surveys_languagesettings where LOWER(surveyls_title) like 'fac%'";

		List<String> facTableNames = new ArrayList<String>();

		try {
			dbConnection = cpds.getConnection();
			statement = dbConnection.createStatement();

			ResultSet rs = statement.executeQuery(getFACTableNameSQL);

			while (rs.next()) {

				String facTableName = "survey_" + rs.getString("surveyls_survey_id");
				facTableNames.add(facTableName);
			}

		} catch (SQLException e) {

			System.out.println(e.getMessage());

		} finally {

			if (statement != null) {
				try {
					statement.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}

			if (dbConnection != null) {
				try {
					dbConnection.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}

		}

		return facTableNames;
	}

	public static void updateAllDatafromFAC() {
		for (String surveyTableName : surveyFACTableNames) {

			updateDatafromFACByTableName(surveyTableName);
		}
	}

	public static void updateDatafromFACByTableName(String surveyTableName) {

		int idSurvey = getIdSurvey(surveyTableName);
		FACSurvey facSurvey = new FACSurvey(cpds, surveyTableName, renewalEstimatedDateRecords.get(idSurvey));

		facSurvey.updateExtactedData();
	}

	private static void registerSurveyData(String surveyTableName) {

		try {

			String surveyName = getSurveyTitle(surveyTableName);

			if (isEncuestaDeFactoria(surveyName)) {

//				String surveySectionHeader = "  [" + surveyTableName + "] Campaña: " + surveyName + " \t";
//				System.out.println(surveySectionHeader);

				List<String> tableColumns = getTableColumns(surveyTableName);

				for (String columnName : tableColumns)
					registerColumnData(columnName);
			}
		} catch (SQLException sqlException) {
			System.out.println("ERROR:" + sqlException.getMessage());
		}
	}
	
	
	private static boolean isEncuestaDeFactoria(String surveyName) {
		
		if (surveyName.startsWith("FAC") || surveyName.startsWith("Factoría")) 
				return true;
		else
				return false;
	}

	private static String getSurveyTitle(String tableName) {

		Connection dbConnection = null;
		Statement statement = null;

		String getSurveyNameSQL = "select surveyls_title from surveys_languagesettings where surveyls_survey_id="
				+ getIdSurvey(tableName);

		String surveyTitle = "";

		try {
			dbConnection = cpds.getConnection();
			statement = dbConnection.createStatement();

			ResultSet rs = statement.executeQuery(getSurveyNameSQL);

			rs.next();
			surveyTitle = rs.getString("surveyls_title");

		} catch (SQLException e) {
			System.out.println(e.getMessage());
		} finally {
			closeEverything(dbConnection, statement);
		}

		return surveyTitle;
	}

	private static void closeEverything(Connection dbConnection, Statement statement) {

		if (statement != null) {
			try {
				statement.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

		if (dbConnection != null) {
			try {
				dbConnection.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	private static void registerColumnData(String columnName) throws SQLException {

		String questionContent = getQuestionContent(columnName);

		if (questionContent.toLowerCase().contains("renovación")) {

			StringTokenizer st = new StringTokenizer(columnName, "X");
			Integer surveyId = new Integer(st.nextToken());

			if (surveyId != null) {
				renewalEstimatedDateRecords.put(surveyId, columnName);
			}
		}
	}

	private static List<String> getTableColumns(String surveyTableName) throws SQLException {

		Connection dbConnection = null;
		Statement statement = null;

		String getColumnNameSQL = "SELECT `COLUMN_NAME` FROM `INFORMATION_SCHEMA`.`COLUMNS`	" + "WHERE `TABLE_SCHEMA`='"
				+ DB_NAME + "' AND `TABLE_NAME`='" + surveyTableName + "'";

		List<String> tableColumns = new ArrayList<String>();

		try {
			dbConnection = cpds.getConnection();
			statement = dbConnection.createStatement();
			ResultSet rs = statement.executeQuery(getColumnNameSQL);

			while (rs.next()) {

				String columnName = rs.getString("COLUMN_NAME");
				tableColumns.add(columnName);
			}

			statement.close();
			dbConnection.close();

		} catch (SQLException e) {

			System.out.println(e.getMessage());

		} finally {

			if (statement != null) {
				statement.close();
			}

			if (dbConnection != null) {
				dbConnection.close();
			}
		}

		return tableColumns;
	}

	private static String getQuestionContent(String questionName) throws SQLException {

		Connection dbConnection = null;
		PreparedStatement preparedStatement = null;
		boolean isSubQuestion = false;

		String getQuestionContentSQL = "SELECT QUESTION FROM QUESTIONS WHERE qid= ?  AND sid = ? AND gid = ? ";
		String subQuestionSQL = "SELECT QUESTION FROM QUESTIONS WHERE sid=? AND gid=? AND parent_qid=? and title=? ";

		String questionContent = "";

		StringTokenizer st = new StringTokenizer(questionName, "X");

		String sid, gid, qid;
		String subquestionID = "";

		try {
			sid = st.nextToken();
			gid = st.nextToken();
			qid = st.nextToken();

			int indexOfSQ = qid.indexOf("SQ");

			if (indexOfSQ != -1) {
				isSubQuestion = true;
				subquestionID = qid.substring(indexOfSQ, qid.length());
				qid = qid.substring(0, indexOfSQ);
			}

			int indexOfComment = qid.indexOf("comment");

			if (indexOfComment != -1) {

				qid = qid.substring(0, indexOfComment);
			}

		} catch (NoSuchElementException e) {
			return "";
		}

		try {
			dbConnection = cpds.getConnection();

			if (!isSubQuestion) {

				preparedStatement = dbConnection.prepareStatement(getQuestionContentSQL);

				preparedStatement.setInt(1, new Integer(qid).intValue());
				preparedStatement.setInt(2, new Integer(sid).intValue());
				preparedStatement.setInt(3, new Integer(gid).intValue());

			} else {

				preparedStatement = dbConnection.prepareStatement(subQuestionSQL);

				preparedStatement.setInt(1, new Integer(sid).intValue());
				preparedStatement.setInt(2, new Integer(gid).intValue());
				preparedStatement.setInt(3, new Integer(qid).intValue());
				preparedStatement.setString(4, subquestionID);
			}

			ResultSet rs = preparedStatement.executeQuery();

			while (rs.next()) {

				questionContent = rs.getString("QUESTION");
			}

		} catch (SQLException e) {

			System.out.println(e.getMessage());

		} finally {

			if (preparedStatement != null) {
				preparedStatement.close();
			}

			if (dbConnection != null) {
				dbConnection.close();
			}
		}

		if (preparedStatement != null) {
			preparedStatement.close();
		}

		if (dbConnection != null) {
			dbConnection.close();
		}

		Document doc = Jsoup.parse(questionContent);

		return doc.text();
	}

	public static int getIdSurvey(String surveyTableName) {
		return new Integer(surveyTableName.toLowerCase().replace("survey_", "")).intValue();
	}

}
