-- MySQL dump 10.13  Distrib 5.6.21, for Win32 (x86)
--
-- Host: localhost    Database: bbddmkpcentraluser
-- ------------------------------------------------------
-- Server version	5.6.21
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Dumping routines for database 'bbddmkpcentraluser'
--
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = cp850 */ ;
/*!50003 SET character_set_results = cp850 */ ;
/*!50003 SET collation_connection  = cp850_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'NO_ENGINE_SUBSTITUTION' */ ;
/*!50003 DROP PROCEDURE IF EXISTS `consultaAll` */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`localhost` PROCEDURE `consultaAll`()
BEGIN
    DECLARE prepared_sql TEXT DEFAULT "";
    DECLARE v_finished INTEGER DEFAULT 0;
    DECLARE numero VARCHAR(100) DEFAULT "";
	  
    DECLARE sids CURSOR FOR
    SELECT DISTINCT(sid) FROM faccentralanswer;
	  
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_finished = 1;
    
	  OPEN sids;
	    
	  get_sids: LOOP
  		
		FETCH sids INTO numero;
      
		  IF v_finished = 1 THEN LEAVE get_sids;
		  END IF;
  		
			set prepared_sql=CONCAT(prepared_sql , ' SELECT fac.* , svlng.surveyls_title, tk.* FROM tokens_',numero,' tk',
  						' LEFT JOIN faccentralanswer fac ON (fac.sid=',numero,
  						' AND fac.token = tk.token AND fac.idRegistro = fac.idRegistro) ',
  						' LEFT JOIN surveys_languagesettings svlng ON fac.sid = svlng.surveyls_survey_id ',
  						' UNION ');
        
		END LOOP get_sids;
	  
		CLOSE sids;
      
		SET @t1 = SUBSTRING(prepared_sql, 1, LENGTH(prepared_sql)-6);
		
		PREPARE stmt3 FROM @t1;
		EXECUTE stmt3;
		DEALLOCATE PREPARE stmt3;
    
	 END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'NO_ENGINE_SUBSTITUTION' */ ;
/*!50003 DROP PROCEDURE IF EXISTS `consultaFiltrada` */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`localhost` PROCEDURE `consultaFiltrada`(
in p_start_date date,
in p_end_date date)
BEGIN
    DECLARE prepared_sql TEXT DEFAULT "";
    DECLARE v_finished INTEGER DEFAULT 0;
    DECLARE numero VARCHAR(100) DEFAULT "";
	  DECLARE sids CURSOR FOR SELECT DISTINCT(sid) FROM faccentralanswerTMP;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_finished = 1;
    
    DROP TABLE IF EXISTS faccentralanswerTMP;
    CREATE TABLE faccentralanswerTMP engine=memory
      SELECT sid,token,max(orderLlamada) as nLlamadas,fechaNuevoContacto
      FROM faccentralanswer 
      WHERE fechaNuevoContacto between p_start_date and p_end_date
      GROUP BY sid,token;
    
	  OPEN sids;
	    
	  get_sids: LOOP
  		
		FETCH sids INTO numero;
      
		  IF v_finished = 1 THEN LEAVE get_sids;
		  END IF;
  		
			set prepared_sql=CONCAT(prepared_sql , ' SELECT ',
              ' svlng.surveyls_title,',
              ' fac.sid,fac.fechaNuevoContacto, fac.nLlamadas,', 
              ' tk.* ',
              ' FROM faccentralanswerTMP fac ',
              ' LEFT JOIN tokens_',numero,' tk ON fac.token = tk.token ',
  						' LEFT JOIN surveys_languagesettings svlng ON fac.sid = svlng.surveyls_survey_id ',
              ' WHERE fac.sid=',numero,
  						' UNION ');
        
		END LOOP get_sids;
	  
		CLOSE sids;
      
		SET @t1 = SUBSTRING(prepared_sql, 1, LENGTH(prepared_sql)-6);
		
		PREPARE stmt3 FROM @t1;
		EXECUTE stmt3;
		DEALLOCATE PREPARE stmt3;
    
    DROP TABLE IF EXISTS faccentralanswerTMP;
    
	 END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = cp850 */ ;
/*!50003 SET character_set_results = cp850 */ ;
/*!50003 SET collation_connection  = cp850_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'NO_ENGINE_SUBSTITUTION' */ ;
/*!50003 DROP PROCEDURE IF EXISTS `consultaFiltradaUltimasLlamadas` */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`localhost` PROCEDURE `consultaFiltradaUltimasLlamadas`( in p_start_date date, in p_end_date date)
BEGIN
    DECLARE prepared_sql TEXT DEFAULT "";
    DECLARE v_finished INTEGER DEFAULT 0;
    DECLARE numero VARCHAR(100) DEFAULT "";
	  DECLARE sids CURSOR FOR SELECT DISTINCT(sid) FROM faccentralanswerTMP;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_finished = 1;
    
    DROP TABLE IF EXISTS faccentralanswerTMP;
    CREATE TABLE faccentralanswerTMP engine=memory
      SELECT sid,token,max(orderLlamada) as nLlamadas,fechaNuevoContacto
      FROM faccentralanswer 
      WHERE fechaNuevoContacto between p_start_date and p_end_date
      GROUP BY sid,token;
    
	  OPEN sids;
	    
	  get_sids: LOOP
  		
		FETCH sids INTO numero;
      
		  IF v_finished = 1 THEN LEAVE get_sids;
		  END IF;
  		
		SET columna = getColumnNameByQuestion( sid, 'Nombre persona de contacto');
		
		SET prepared_sql=CONCAT(prepared_sql , ' SELECT ',
              ' svlng.surveyls_title,',
              ' fac.sid,fac.fechaNuevoContacto, fac.nLlamadas,',
			  ' srv.',columna,' as Nombre_persona_de_contacto,'
              ' tk.* ',
              ' FROM faccentralanswerTMP fac ',
              ' LEFT JOIN tokens_',numero,' tk ON fac.token = tk.token ',
			  ' LEFT JOIN survey_',numero,' srv on fac.token = srv.token ',
              ' LEFT JOIN surveys_languagesettings svlng ON fac.sid = svlng.surveyls_survey_id ',
              ' WHERE fac.sid=',numero,
  						' UNION ');
        
		END LOOP get_sids;
	  
		CLOSE sids;
      
		SET @t1 = SUBSTRING(prepared_sql, 1, LENGTH(prepared_sql)-6);
		
		PREPARE stmt3 FROM @t1;
		EXECUTE stmt3;
		DEALLOCATE PREPARE stmt3;
    
    DROP TABLE IF EXISTS faccentralanswerTMP;
    
	 END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2018-01-25 13:08:39


DROP FUNCTION IF EXISTS getColumnNameByQuestion;
CREATE FUNCTION `getColumnNameByQuestion`(p_sid INTEGER, p_question_content VARCHAR(500)) RETURNS varchar(255) CHARSET latin1
BEGIN
	
  DECLARE return_var VARCHAR(255);
  SET return_var = (select CONCAT(sid,'X',gid,'X',parent_qid,title) from questions where sid=p_sid and trim(question)=p_question_content);
  
	RETURN return_var;
END;



DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = cp850 */ ;
/*!50003 SET character_set_results = cp850 */ ;
/*!50003 SET collation_connection  = cp850_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'NO_ENGINE_SUBSTITUTION' */ ;
/*!50003 DROP PROCEDURE IF EXISTS `consultaFiltradaUltimasLlamadasSurvData` */ ;
DELIMITER ;;

CREATE DEFINER=`root`@`localhost` PROCEDURE `consultaFiltradaUltimasLlamadasSurvData`( in p_start_date date, in p_end_date date)
BEGIN
    DECLARE prepared_sql TEXT DEFAULT "";
    
	DECLARE columna1 VARCHAR(255);
	DECLARE columna2 VARCHAR(255);
	DECLARE columna3 VARCHAR(255);
	DECLARE columna4 VARCHAR(255);
	DECLARE columna5 VARCHAR(255);
	DECLARE columna6 VARCHAR(255);
	DECLARE columna7 VARCHAR(255);
	DECLARE columna8 VARCHAR(255);
	DECLARE columna9 VARCHAR(255);
	DECLARE columna10 VARCHAR(255);
	DECLARE columna11 VARCHAR(255);
	DECLARE columna12 VARCHAR(255);
	DECLARE columna13 VARCHAR(255);
	DECLARE columna14 VARCHAR(255);
	DECLARE columna15 VARCHAR(255);
	DECLARE columna16 VARCHAR(255);
	DECLARE columna17 VARCHAR(255);
	DECLARE columna18 VARCHAR(255);
	DECLARE columna19 VARCHAR(255);
	DECLARE columna20 VARCHAR(255);
	DECLARE columna21 VARCHAR(255);
	DECLARE columna22 VARCHAR(255);
	DECLARE columna23 VARCHAR(255);
	DECLARE columna24 VARCHAR(255);
	DECLARE columna25 VARCHAR(255);
    
	DECLARE nombreColumn1 VARCHAR(255);
	DECLARE nombreColumn2 VARCHAR(255);
	DECLARE nombreColumn3 VARCHAR(255);
	DECLARE nombreColumn4 VARCHAR(255);
	DECLARE nombreColumn5 VARCHAR(255);
	DECLARE nombreColumn6 VARCHAR(255);
	DECLARE nombreColumn7 VARCHAR(255);
	DECLARE nombreColumn8 VARCHAR(255);
	DECLARE nombreColumn9 VARCHAR(255);
	DECLARE nombreColumn10 VARCHAR(255);
	DECLARE nombreColumn11 VARCHAR(255);
	DECLARE nombreColumn12 VARCHAR(255);
	DECLARE nombreColumn13 VARCHAR(255);
	DECLARE nombreColumn14 VARCHAR(255);
	DECLARE nombreColumn15 VARCHAR(255);
	DECLARE nombreColumn16 VARCHAR(255);
	DECLARE nombreColumn17 VARCHAR(255);
	DECLARE nombreColumn18 VARCHAR(255);
	DECLARE nombreColumn19 VARCHAR(255);
	DECLARE nombreColumn20 VARCHAR(255);
	DECLARE nombreColumn21 VARCHAR(255);
	DECLARE nombreColumn22 VARCHAR(255);
	DECLARE nombreColumn23 VARCHAR(255);
	DECLARE nombreColumn24 VARCHAR(255);
	DECLARE nombreColumn25 VARCHAR(255);
    
    DECLARE v_finished INTEGER DEFAULT 0;
    DECLARE numero VARCHAR(100) DEFAULT "";
	  DECLARE sids CURSOR FOR SELECT DISTINCT(sid) FROM faccentralanswerTMP;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_finished = 1;
    
    DROP TABLE IF EXISTS faccentralanswerTMP;
    CREATE TABLE faccentralanswerTMP engine=memory
      SELECT sid,token,max(orderLlamada) as nLlamadas,fechaNuevoContacto
      FROM faccentralanswer 
      WHERE fechaNuevoContacto between p_start_date and p_end_date
      GROUP BY sid,token;
    
	  OPEN sids;
	    
	  get_sids: LOOP
  		
		FETCH sids INTO numero;
      
		  IF v_finished = 1 THEN LEAVE get_sids;
		  END IF;
  		
		
		SET columna1=getColumnNameByQuestion( numero, 'Tipo de actividad');
		SET columna2=getColumnNameByQuestion( numero, 'Tamaño del parque');
		SET columna3= getColumnNameByQuestion(numero ,'Descripción talla del parque');
		SET columna4= getColumnNameByQuestion(numero ,'Marca principal del parque');
		SET columna5= getColumnNameByQuestion(numero ,'Nombre persona de contacto');
		SET columna6= getColumnNameByQuestion(numero ,'Cargo dentro de la empresa');
		SET columna7= getColumnNameByQuestion(numero ,'Teléfono');
		SET columna8= getColumnNameByQuestion(numero ,'Persona de contacto');
		SET columna9= getColumnNameByQuestion(numero ,'Teléfono de contacto');
		SET columna10= getColumnNameByQuestion(numero ,'Móvil de contacto');
		SET columna11= getColumnNameByQuestion(numero ,'Mail de contacto (en minúsculas)');
		SET columna12= getColumnNameByQuestion(numero ,'Apellidos persona de contacto');
		SET columna13= getColumnNameByQuestion(numero ,'Dirección');
		SET columna14= getColumnNameByQuestion(numero ,'CP');
		SET columna15= getColumnNameByQuestion(numero ,'Población');
		SET columna16= getColumnNameByQuestion(numero ,'Provincia');
		SET columna17= getColumnNameByQuestion(numero ,'Modelo principal del parque');
		SET columna18= getColumnNameByQuestion(numero ,'Producto (tipo/gama de vehículo)');
		SET columna19= getColumnNameByQuestion(numero ,'Descripción de la oferta');
		SET columna20= getColumnNameByQuestion(numero ,'Información adicional de la oferta (tipo de financiación,...)');
		SET columna21= getColumnNameByQuestion(numero ,'Teléfono 2 de contacto');
		SET columna22= getColumnNameByQuestion(numero ,'Tipo de financiación');
		SET columna23= getColumnNameByQuestion(numero ,'Fecha (respetar formato dd/mm/aaaa)');
		SET columna24= getColumnNameByQuestion(numero ,'Hora (respetar formato hh:mm)');
		SET columna25= getColumnNameByQuestion(numero ,'Fecha estimada renovación (respetar formato dd/mm/aaaa)');

		
		SET nombreColumn1= replace('Tipo de actividad', ' ', '_');
		SET nombreColumn2= replace('Tamaño del parque', ' ', '_');
		SET nombreColumn3= replace('Descripción talla del parque', ' ', '_');
		SET nombreColumn4= replace('Marca principal del parque', ' ', '_');
		SET nombreColumn5= replace('Nombre persona de contacto', ' ', '_');
		SET nombreColumn6= replace('Cargo dentro de la empresa', ' ', '_');
		SET nombreColumn7= replace('Teléfono', ' ', '_');
		SET nombreColumn8= replace('Persona de contacto', ' ', '_');
		SET nombreColumn9= replace('Teléfono de contacto', ' ', '_');
		SET nombreColumn10= replace('Móvil de contacto', ' ', '_');
		SET nombreColumn11= replace('Mail de contacto', ' ', '_');
		SET nombreColumn12= replace('Apellidos persona de contacto', ' ', '_');
		SET nombreColumn13= replace('Dirección', ' ', '_');
		SET nombreColumn14= replace('CP', ' ', '_');
		SET nombreColumn15= replace('Población', ' ', '_');
		SET nombreColumn16= replace('Provincia', ' ', '_');
		SET nombreColumn17= replace('Modelo principal del parque', ' ', '_');
		SET nombreColumn18= replace('Producto', ' ', '_');
		SET nombreColumn19= replace('Descripción de la oferta', ' ', '_');
		SET nombreColumn20= replace('Información adicional de la oferta', ' ', '_');
		SET nombreColumn21= replace('Teléfono 2 de contacto', ' ', '_');
		SET nombreColumn22= replace('Tipo de financiación', ' ', '_');
		SET nombreColumn23= replace('Fecha', ' ', '_');
		SET nombreColumn24= replace('Hora', ' ', '_');
		
		
		/*Columna problematica*/
		SET nombreColumn25= replace('Fecha estimada renovación', ' ', '_');
			
		
		
		SET prepared_sql=CONCAT(prepared_sql , ' SELECT ',
              ' svlng.surveyls_title,',
              ' fac.sid,fac.fechaNuevoContacto, fac.nLlamadas,',
              ' srv.',columna1,' as ',nombreColumn1,' ,',
              ' srv.',columna2,' as ',nombreColumn2,' ,',
              ' srv.',columna3,' as ',nombreColumn3,' ,',
              ' srv.',columna4,' as ',nombreColumn4,' ,',
              ' srv.',columna5,' as ',nombreColumn5,' ,',
              ' srv.',columna6,' as ',nombreColumn6,' ,',
              ' srv.',columna7,' as ',nombreColumn7,' ,',
              ' srv.',columna8,' as ',nombreColumn8,' ,',
              ' srv.',columna9,' as ',nombreColumn9,' ,',
              ' srv.',columna10,' as ',nombreColumn10,' ,',
              ' srv.',columna11,' as ',nombreColumn11,' ,',
              ' srv.',columna12,' as ',nombreColumn12,' ,',
              ' srv.',columna13,' as ',nombreColumn13,' ,',
              ' srv.',columna14,' as ',nombreColumn14,' ,',
              ' srv.',columna15,' as ',nombreColumn15,' ,',
              ' srv.',columna16,' as ',nombreColumn16,' ,',
              ' srv.',columna17,' as ',nombreColumn17,' ,',
              ' srv.',columna18,' as ',nombreColumn18,' ,',
              ' srv.',columna19,' as ',nombreColumn19,' ,',
              ' srv.',columna20,' as ',nombreColumn20,' ,',
              ' srv.',columna21,' as ',nombreColumn21,' ,', 
              ' srv.',columna22,' as ',nombreColumn22,' ,',
              ' srv.',columna23,' as ',nombreColumn23,' ,',
              ' srv.',columna24,' as ',nombreColumn24,' ,',
            /*' srv.',columna25,' as ',nombreColumn25,' ,',*/
              ' tk.* ',
              ' FROM faccentralanswerTMP fac ',
              ' LEFT JOIN tokens_',numero,' tk ON fac.token = tk.token ',
              ' LEFT JOIN survey_',numero,' srv on fac.token = srv.token ',
              ' LEFT JOIN surveys_languagesettings svlng ON fac.sid = svlng.surveyls_survey_id ',
              ' WHERE fac.sid=',numero,
              ' UNION ');
        
		END LOOP get_sids;
	  
		CLOSE sids;
      
		SET @t1 = SUBSTRING(prepared_sql, 1, LENGTH(prepared_sql)-6);
		
		PREPARE stmt3 FROM @t1;
		EXECUTE stmt3;
		DEALLOCATE PREPARE stmt3;
    
    DROP TABLE IF EXISTS faccentralanswerTMP;
    
	 END ;;

DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;