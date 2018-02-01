/*!50003 DROP PROCEDURE IF EXISTS `consultaFiltradaUltimasLlamadas`*/;

DELIMITER ;;
/*!50003 CREATE PROCEDURE `consultaFiltradaUltimasLlamadas`(
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
    
	 END;
 */;;
DELIMITER ;
