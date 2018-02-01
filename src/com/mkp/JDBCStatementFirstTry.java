package com.mkp;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.beans.PropertyVetoException;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.mysql.jdbc.exceptions.jdbc4.MySQLIntegrityConstraintViolationException;


public class JDBCStatementFirstTry {

//	private static final String DB_DRIVER = "com.mysql.jdbc.Driver";
//	private static final String DB_NAME = "bbddmkp";
//	private static final String DB_CONNECTION = "jdbc:mysql://localhost:3306/" + DB_NAME;
//	private static final String DB_USER = "root";
//	private static final String DB_PASSWORD = "";

	private static final String DB_DRIVER = "com.mysql.jdbc.Driver";
	private static String DB_NAME;
	private static String DB_USER;
	private static String DB_PASSWORD;
	private static String DB_CONNECTION;
	
	
	private static ComboPooledDataSource cpds;
	
	private static List<String> surveyFACTableNames ;
	private static HashMap<Integer, String> renewalEstimatedDateRecords = new  HashMap<Integer, String> ();
	
	private static final String FILENAME = "resultado.txt";

	public static void main(String[] argv) throws IOException, PropertyVetoException  {

			readingPropsFile();
			
			settingUpConnectionPool();
			
			getFACSurveysTableName();
						
			writeDBSummarize();
			
			extractAllFACForDataExtraction();
			
			System.out.println("---- FINALIZACION DEL PROCESO ---");
	
	}
	
	
	private static void readingPropsFile() throws IOException {
		
		InputStream input = null;
		
		
		try {
			Properties prop = new Properties();
			String propFileName = "config.properties";
			
			
			
			input = JDBCStatementFirstTry.class.getClassLoader().getResourceAsStream(propFileName);
			 
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


	private static void settingUpConnectionPool() {
		try {
			cpds = new ComboPooledDataSource();
			cpds.setDriverClass( DB_DRIVER ); 
			cpds.setJdbcUrl( DB_CONNECTION);
			cpds.setUser(DB_USER);
			cpds.setPassword(DB_PASSWORD);

			// the settings below are optional -- c3p0 can work with defaults
			cpds.setMinPoolSize(5);
			cpds.setAcquireIncrement(5);
			cpds.setMaxPoolSize(50);
		}
		catch(Exception e) {
			System.out.println("Error setting up connection pool definition!");
			}
	}
	
	private static void  getFACSurveysTableName() {
		try {
			surveyFACTableNames = getFACSurveyTableNames();
		} catch (PropertyVetoException e) {

			e.printStackTrace();
		}
	}

	
	private static void writeDBSummarize() throws IOException, PropertyVetoException {
		
		FileWriter fw = new FileWriter(FILENAME);
		BufferedWriter bw = new BufferedWriter(fw);
					
		for (String surveyTableName : surveyFACTableNames) 
			writeSurveyData(surveyTableName,bw);
			
	}
	
	
	public static void extractAllFACForDataExtraction() {
		for (String surveyTableName : surveyFACTableNames) { 
				procesSurveyForDataExtraction(surveyTableName);
			}
		
		
	}

	private static List<String> getSurveyTableNames() throws PropertyVetoException {
		Connection dbConnection = null;
		Statement statement = null;

		String getTableNameSQL = "SELECT table_name FROM information_schema.tables " + "where table_schema='" + DB_NAME
				+ "' " + "and table_name like 'survey_%';";

		List<String> tableNames = new ArrayList<String>();

		try {
			dbConnection = cpds.getConnection();
			statement = dbConnection.createStatement();

			ResultSet rs = statement.executeQuery(getTableNameSQL);

			while (rs.next()) {

				String tableName = rs.getString("TABLE_NAME");
				if (validateTableName(tableName))
					tableNames.add(tableName);
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

		return tableNames;
	}
	
private static List<String> getFACSurveyTableNames() throws PropertyVetoException {
		
		Connection dbConnection = null;
		Statement statement = null;

		String getFACTableNameSQL = "select surveyls_survey_id from surveys_languagesettings where LOWER(surveyls_title) like 'fac%'";
		
		List<String> facTableNames = new ArrayList<String>();
		
		try {
			dbConnection = cpds.getConnection();
			statement = dbConnection.createStatement();

			ResultSet rs = statement.executeQuery(getFACTableNameSQL);

			while (rs.next()) {
				
				String facTableName = "survey_"+rs.getString("surveyls_survey_id");
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

	private static void writeSurveyData(String surveyTableName, BufferedWriter bw) {
		
		try {

			String surveyName = getSurveyTitle(surveyTableName);

			if (surveyName.startsWith("FAC") || surveyName.startsWith("Factoría")) {
				String surveySectionHeader = "\n\n\n"
						+ "========================================================================================\n"
						+ "\t\t Campaña: " + surveyName + " \t"+ surveyTableName +"\t\n"
						+ "========================================================================================\n";

				System.out.println(surveySectionHeader);
				bw.write(surveySectionHeader);

				List<String> tableColumns = getTableColumns(surveyTableName);

				for (String columnName : tableColumns)
					writeColumnData(columnName, bw);
			}
		} catch (IOException ioexception) {
			System.out.println(
					"Error escribiendo la informacion de la encuesta " + surveyTableName + "en el fichero de salida.");
		} catch (PropertyVetoException p) {
			System.out.println("ERROR:" + p.getMessage());
		} catch (SQLException sqlException) {
			System.out.println("ERROR:" + sqlException.getMessage());
		}
	}
	
	
	private static String getSurveyTitle(String surveyId) throws PropertyVetoException {
		 
			Connection dbConnection = null;
			Statement statement = null;

			surveyId=surveyId.replaceAll("survey_", "");
			
			String getSurveyNameSQL = "select surveyls_title from surveys_languagesettings where surveyls_survey_id=" + surveyId;
			String surveyName="";
			
			try {
				dbConnection = cpds.getConnection();
				statement = dbConnection.createStatement();
				
				
				ResultSet rs = statement.executeQuery(getSurveyNameSQL);

				rs.next();
				surveyName = rs.getString("surveyls_title");
					

			} catch (SQLException e) {

				System.out.println(e.getMessage());

			} finally {

				closeEverything(dbConnection, statement);
			}

			return surveyName;
	}

	private static void procesSurveyForDataExtraction(String surveyTableName) {
		
		Integer surveyId = new Integer(surveyTableName.replaceAll("survey_",""));
		
		if(estaEncuestaTieneDatosExtraibles(surveyId)) {
		
			List<String> surveyDistinctToken = getSurveyDistinctToken(surveyTableName);
			
			for(String token : surveyDistinctToken ) { 
				processAnswersFor_Token_Survey(token, surveyTableName);
			}
		}
		
	}
	
	private static boolean estaEncuestaTieneDatosExtraibles(Integer surveyId) {
		
		return renewalEstimatedDateRecords.get(surveyId) != null ? true : false; 
	}
	
	
	private static List<String> getSurveyDistinctToken (String surveyTableName ){
		
		Connection dbConnection = null;
		Statement statement = null;
		
		List<String> distinctTokens = new ArrayList<String> ();
		
		String getSurveyDistinctTokenSQL = "select distinct(token) from " + surveyTableName;
		
		try {
			
			dbConnection = cpds.getConnection();
			statement = dbConnection.createStatement();
			
			ResultSet rs = statement.executeQuery(getSurveyDistinctTokenSQL);
		
			while (rs.next()) {
				distinctTokens.add(rs.getString("token"));
			}
			
			
		}catch (SQLException e) {
			e.printStackTrace();
		} finally {

			closeEverything(dbConnection, statement);
		}
		
		return distinctTokens;
	}

	private static void processAnswersFor_Token_Survey(String token, String surveyTableName) {
		
		Connection dbConnection = null;
		Statement statement = null;
		
		Integer sid = new Integer(surveyTableName.replace("survey_",""));
		String registroConPreguntaRenovacion = renewalEstimatedDateRecords.get(sid);
		
		
		if(registroConPreguntaRenovacion != null) {
			
			String getAnswerForTokenSurveySQL = "select id as idRegistro,`"+registroConPreguntaRenovacion+"` as preguntaSobreRenovacion  from "+surveyTableName+" where token='"+token+"' order by startdate";
			
			try {
				
				dbConnection = cpds.getConnection();
				statement = dbConnection.createStatement();
				
				ResultSet rs = statement.executeQuery(getAnswerForTokenSurveySQL);
			
				int answerOrder = 1;
				
				while (rs.next()) {
					
					FacCentralAnswer facCentralAnswer = 
							new FacCentralAnswer(rs.getInt("idRegistro"), sid, token,new Integer(answerOrder),rs.getString("preguntaSobreRenovacion"));
				
					
					insertCentralAnswer(facCentralAnswer);
					
					answerOrder++;
				}
				
				
			}catch (SQLException e) {
				e.printStackTrace();
			} finally {

				closeEverything(dbConnection, statement);
			}
	}
			
	}
	
	public static void insertCentralAnswer(FacCentralAnswer facCentralAnswer){
			
		System.out.println(facCentralAnswer.getInsertSQL());
		
		String insertSQL = "INSERT INTO faccentralanswer ( sid, idRegistro, token, orderLlamada, conclusionLlamada, fechaNuevoContacto ) VALUES (?,?,?,?,?,?)";
		
		Connection dbConnection = null;
		
		
		try {
			
			dbConnection = cpds.getConnection();
			
			PreparedStatement preparedStatement = dbConnection.prepareStatement(insertSQL);
			preparedStatement.setInt(1, facCentralAnswer.getSid());
			preparedStatement.setInt(2, facCentralAnswer.getIdRegistro());
			preparedStatement.setString(3, facCentralAnswer.getToken());
			preparedStatement.setInt(4, facCentralAnswer.getOrdenLlamada());
			preparedStatement.setString(5, facCentralAnswer.getConclusionLlamada());
			
			if(facCentralAnswer.getFechaNuevoContacto() != null)
			{
				preparedStatement.setDate(6, new java.sql.Date(facCentralAnswer.getFechaNuevoContacto().getTime()));
			}else {
				
				preparedStatement.setDate(6,null);
			}
			
			preparedStatement .executeUpdate();
			
		} catch(MySQLIntegrityConstraintViolationException e){
			System.out.println("Registro procesado anteriormente");
			
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (dbConnection != null) {
				try {
					dbConnection.close();
				} catch (SQLException e) {
					e.printStackTrace();
				};
			}
		}
		
	}
	
	
	private static void closeEverything(Connection dbConnection,Statement statement) {

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
	
	private static void writeColumnData(String columnName, BufferedWriter bw) throws SQLException, PropertyVetoException, IOException {
	
		String questionContent = getQuestionContent(columnName);
		String rowContent = "\t\t" + columnName + ": " + questionContent +"\n";
	
		bw.write(rowContent);
		
		if(questionContent.toLowerCase().contains("renovación")) {
			
			StringTokenizer st = new StringTokenizer(columnName, "X");
			Integer surveyId = new Integer(st.nextToken());
			
			if (surveyId !=null) {
				System.out.println(columnName+"---> Registrando pregunta con fecha estimada");
				renewalEstimatedDateRecords.put(surveyId, columnName);
			}
		}
			
			
	} 
	
	private static boolean validateTableName(String tableName) {

		Pattern pattern;
		Matcher matcher;

		String NAME_PATTERN = "survey_[0-9]*";

		pattern = Pattern.compile(NAME_PATTERN);

		matcher = pattern.matcher(tableName);

		if (matcher.matches())
			return true;
		else
			return false;

	}

	private static List<String> getTableColumns(String tableName) throws SQLException, PropertyVetoException {

		Connection dbConnection = null;
		Statement statement = null;

		String getColumnNameSQL = "SELECT `COLUMN_NAME` FROM `INFORMATION_SCHEMA`.`COLUMNS`	" + "WHERE `TABLE_SCHEMA`='"
				+ DB_NAME + "' AND `TABLE_NAME`='" + tableName + "'";

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

	private static String getQuestionContent(String questionName) throws SQLException, PropertyVetoException {

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
	
	
	
}
