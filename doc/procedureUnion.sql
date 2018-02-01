/*!50003 DROP PROCEDURE IF EXISTS `consultaAll`*/;

DELIMITER ;;
/*!50003 CREATE PROCEDURE `consultaAll`()
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
    
	 END */;;
DELIMITER ;
