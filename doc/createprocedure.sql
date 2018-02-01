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
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2018-01-25 13:08:39
