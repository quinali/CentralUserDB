CREATE TABLE `faccentralanswer` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `sid` int(11) NOT NULL,
  `token` varchar(20) NOT NULL,
  `idRegistro` int(11) NOT NULL,
  `fechaNuevoContacto` date DEFAULT NULL,
  `orderLlamada` int(11) NOT NULL,
  `conclusionLlamada` text CHARACTER SET utf8 COLLATE utf8_unicode_ci,
  PRIMARY KEY (`id`),
  UNIQUE KEY `sid` (`sid`,`token`,`idRegistro`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
