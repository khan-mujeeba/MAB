package com.mas.idm.util;

import java.util.ResourceBundle;

public class IDMConstants {
	static ResourceBundle bundle = ResourceBundle
			.getBundle("ApplicationResources"); 
	public static String returnSystemAdmin=bundle.getString("login.success.systemadmin");
	public static String returnSystemOwner=bundle.getString("login.success.systemowner");
	public static String returnVendorSystemOwner=bundle.getString("login.success.vendorsystemowner");
	public static String returnLoginError=bundle.getString("login.error");
	public static String returnLoginMsg=bundle.getString("login.error.message");
	public static String returnInvalidUser=bundle.getString("login.regerror.message");
	public static String returnReportsPath=bundle.getString("reports.path");
	public static String returnTempReportsPath=bundle.getString("tempreports.path");
	public static String returnAccessLevelDocPath=bundle.getString("accessleveldoc.path");
	public static String returnPulseraAppName=bundle.getString("pulsera.name");
	
	//ldap connection configuration for existing AD
	public static String returnBaseDN=bundle.getString("ldap.basedn");
	public static String returnLDAPUserName=bundle.getString("ldap.username");
	public static String returnLDAPPassword=bundle.getString("ldap.password");
	public static String returnLDAPServer=bundle.getString("ldap.server");
	
	//ldap connection configuration for FY AD
	public static String returnFYBaseDN=bundle.getString("ldap.authentication.searchBase");
	public static String returnFYLDAPUserName=bundle.getString("ldap.authentication.managerDn");
	public static String returnFYLDAPPassword=bundle.getString("ldap.authentication.managerPassword");
	public static String returnFYLDAPServer=bundle.getString("ldap.authentication.server");

	public static String returnVPNUserName=bundle.getString("vpn.username");
	public static String returnVPNPassword=bundle.getString("vpn.password");
	public static String returnVPNServer=bundle.getString("vpn.server");
	public static String returnVPNBaseDN=bundle.getString("vpn.basedn");
	public static String returnAlertMail=bundle.getString("alert.mailid");
	public static String returnSnowAlertMail=bundle.getString("security.mailid");
	//public static String returnSnowBccAlertMail=bundle.getString("security.mailidbcc");
	//ADDED for pulsera record not found
	//public static String returnPulseraRecord=bundle.getString("alert.pulserarecord");
	
	public static String returnSystemRun=bundle.getString("system.run");
	public static String DISABLE_AD = bundle.getString("AD");
	public static String DISABLE_VPN = bundle.getString("VPN");
	
	public static String MAIL_TEST = bundle.getString("mail.test");
	public static String ITCB_REPORTS_PATH = bundle.getString("itcb.reports.path");
	//added
	//public static String returnLDAPADServer=bundle.getString("ldap.adserver");
	public static final int APPID=5;
	public static final int STATUS_ACTIVE=1;
	public static final int STATUS_INACTIVE=2;
	
	public static String idadmMail = bundle.getString("alert.IdadmMailId");
}
