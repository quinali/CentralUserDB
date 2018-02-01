package com.mkp;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;

public class FacCentralAnswer {
	
	
	private Integer sid;
	private Integer idRegistro;
	private String token;
	
	private Date fechaNuevoContacto;
	private Integer ordenLlamada;
	private String conclusionLlamada;
	
	
	public FacCentralAnswer( Integer idRegistro, Integer sid, String token, Integer ordenLlamada,String conclusionLlamada){
		this.idRegistro = idRegistro;
		this.sid = sid;
		this.token = token;
		this.ordenLlamada = ordenLlamada;
		this.conclusionLlamada = conclusionLlamada;
		
		this.fechaNuevoContacto = extraeFechaNuevoContacto(this.conclusionLlamada);
		
	}
	
	private Date extraeFechaNuevoContacto(String cadena) {
		
		Date fecha = null;
		SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
		
		try {
			if (cadena.matches("^(0?[1-9]|[12][0-9]|3‌​[01])/(0?[1-9]|1[012])/(\\d{4})$")) {
				
				fecha = sdf.parse(cadena.trim());
	
			}else if(cadena.matches("^(0?[1-9]|1[012])/(\\d{4})$")){
	
				fecha = sdf.parse("01/"+cadena.trim());
			}
		
		} catch (ParseException e) {

			e.printStackTrace();
		}	
		
		System.out.println(" Localizada la fecha "+fecha);
		
		return fecha;
	}

	Integer id;
	public Integer getId() {
		return id;
	}
	public void setId(Integer id) {
		this.id = id;
	}
	public Integer getSid() {
		return sid;
	}
	public void setSid(Integer sid) {
		this.sid = sid;
	}
	public Integer getIdRegistro() {
		return idRegistro;
	}
	public void setIdRegistro(Integer idRegistro) {
		this.idRegistro = idRegistro;
	}
	public Date getFechaNuevoContacto() {
		return fechaNuevoContacto;
	}
	public void setFechaNuevoContacto(Date fechaNuevoContacto) {
		this.fechaNuevoContacto = fechaNuevoContacto;
	}
	public Integer getOrdenLlamada() {
		return ordenLlamada;
	}
	public void setOrdenLlamada(Integer ordenLlamada) {
		this.ordenLlamada = ordenLlamada;
	}
	public String getConclusionLlamada() {
		return conclusionLlamada;
	}
	public void setConclusionLlamada(String conclusionLlamada) {
		this.conclusionLlamada = conclusionLlamada;
	}
	public String getToken() {
		return token;
	}
	public void setToken(String token) {
		this.token = token;
	}
	

	public String toString(){
		
		return "order="+ordenLlamada+" "
				+ " id="+idRegistro 
				+ " sid="+sid  
				+ " pregunta="+ conclusionLlamada;
	}
	
	public String getInsertSQL(){
		
		return "INSERT INTO faccentralanswer (sid,idRegistro, token, orderLlamada, conclusionLlamada) VALUES"
				+ " ("+sid+","+idRegistro+","+token+","+ordenLlamada+",'"+conclusionLlamada+"')";
		
	}
}
