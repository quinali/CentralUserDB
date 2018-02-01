package com.mkp;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.mysql.jdbc.exceptions.jdbc4.MySQLIntegrityConstraintViolationException;


public class FACSurvey {

	private ComboPooledDataSource cpds;
	private String registroConPreguntaRenovacion;
	private String surveyTableName;
	private Integer surveyId;
	
	private int nTotalRegistrosProcesados = 0;
	private final int nRegistrosMostrarMensaje = 50;
	
	
	public FACSurvey(ComboPooledDataSource cpds, String surveyTableName, String registroConPreguntaRenovacion) {
	
		this.cpds = cpds;
		this.surveyTableName = surveyTableName;
		this.registroConPreguntaRenovacion = registroConPreguntaRenovacion;
		this.surveyId =  new Integer(surveyTableName.replaceAll("survey_", ""));
		
		System.out.println("Procesado la encuesta "+surveyTableName);
	}
	
	
	public void updateExtactedData() {
		
		int lastRegistroUpdated = lastRegistroUpdated();
		processAnswersFromId(lastRegistroUpdated);
	}
	

	
	private int lastRegistroUpdated() {
		
		Connection dbConnection = null;
		Statement statement = null;
		
		String consultaUltimoRegistro = "SELECT MAX(idRegistro) FROM faccentralanswer WHERE SID="+surveyId;
		int idUltimoRegistro = 1;
		
		try {
			
			dbConnection = cpds.getConnection();
			statement = dbConnection.createStatement();

			ResultSet rs = statement.executeQuery(consultaUltimoRegistro);
			
			if (rs.next()) {
				String value = rs.getString(1);
			
				if(value != null)
					idUltimoRegistro = new Integer(value).intValue();
			}	
			
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {

			closeEverything(dbConnection, statement);
		}

		return idUltimoRegistro;
	}
	

	private void processAnswersFromId(int idRegisterAlready) {
		
		Connection dbConnection = null;
		Statement statement = null;
		
		if (registroConPreguntaRenovacion != null) {

			String getAnswerFromSurvey = "select id as idRegistro, token, `" + registroConPreguntaRenovacion 
					+ "` as preguntaSobreRenovacion  from " + surveyTableName 
					+ " where id > " + idRegisterAlready
					+ " order by startdate";

			try {

				dbConnection = cpds.getConnection();
				statement = dbConnection.createStatement();

				ResultSet rs = statement.executeQuery(getAnswerFromSurvey);

				while (rs.next()) {
					
					Integer idRegistro = rs.getInt("idRegistro");
					String token = rs.getString("token");
					String preguntaSobreRenovacion = rs.getString("preguntaSobreRenovacion");
					
					int answerOrder = getMaxAnswerOrderByToken(token) + 1;

					FacCentralAnswer facCentralAnswer = new FacCentralAnswer(idRegistro, surveyId, token,
							answerOrder,preguntaSobreRenovacion );
					
					insertCentralAnswer(facCentralAnswer);
					
					nTotalRegistrosProcesados++;
					
					printRegistrosProcesados();
				}

			} catch (SQLException e) {
				e.printStackTrace();
			} finally {

				closeEverything(dbConnection, statement);
			}
		}
	}
	
	private void printRegistrosProcesados() {
		
		int resto = nTotalRegistrosProcesados%nRegistrosMostrarMensaje;
		
		if( resto ==0 ) 
			System.out.println(" ["+surveyId+"] Insertados	 "+nTotalRegistrosProcesados+" registros ...");

	}
	
	private int getMaxAnswerOrderByToken(String token) {
		
		Connection dbConnection = null;
		Statement statement = null;
		
		String consultaUltimoRegistro = "select Max(orderLlamada) FROM faccentralanswer  WHERE SID="+surveyId+" AND token='"+token+"'";
		int idMaxAnswerOrder = 0;
		
		try {
			
			dbConnection = cpds.getConnection();
			statement = dbConnection.createStatement();

			ResultSet rs = statement.executeQuery(consultaUltimoRegistro);
			
			if (rs.next()) {
				String value = rs.getString(1);
			
				if(value != null)
					idMaxAnswerOrder = new Integer(value).intValue();
			}	
			
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {

			closeEverything(dbConnection, statement);
		}

		return idMaxAnswerOrder;
			
	}
	
	

	public void insertCentralAnswer(FacCentralAnswer facCentralAnswer) {

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

			if (facCentralAnswer.getFechaNuevoContacto() != null) {
				preparedStatement.setDate(6, new java.sql.Date(facCentralAnswer.getFechaNuevoContacto().getTime()));
			} else {

				preparedStatement.setDate(6, null);
			}

			preparedStatement.executeUpdate();

		} catch (MySQLIntegrityConstraintViolationException e) {
			System.out.println("Registro procesado anteriormente");

		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (dbConnection != null) {
				try {
					dbConnection.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
				
			}
		}

	}

	private void closeEverything(Connection dbConnection, Statement statement) {

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

	
	public static int getIdSurvey(String surveyTableName) {
		
		return new Integer(surveyTableName.toLowerCase().replace("survey_","")).intValue();
	}

	
}
