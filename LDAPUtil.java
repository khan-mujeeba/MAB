package com.mas.idm.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.DirContext;
import javax.naming.directory.ModificationItem;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.Control;
import javax.naming.ldap.InitialLdapContext;
import javax.naming.ldap.LdapContext;
import javax.naming.ldap.PagedResultsControl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.mas.idm.bean.StaffVo;
import com.mas.idm.ldap.ActiveDirectoryLdapService;




public class LDAPUtil {

	/* private static final String AD_ATTR_NAME_TOKEN_GROUPS = "tokenGroups";
	    private static final String AD_ATTR_NAME_OBJECT_CLASS = "objectClass";
	    private static final String AD_ATTR_NAME_OBJECT_CATEGORY = "objectCategory";
	    private static final String AD_ATTR_NAME_MEMBER = "member";
	    private static final String AD_ATTR_NAME_MEMBER_OF = "memberOf";
	    private static final String AD_ATTR_NAME_DESCRIPTION = "description";
	    private static final String AD_ATTR_NAME_OBJECT_GUID = "objectGUID";
	    private static final String AD_ATTR_NAME_OBJECT_SID = "objectSid";*/
	private static final String AD_ATTR_NAME_DISTINGUISHED_NAME = "distinguishedName";
	/* private static final String AD_ATTR_NAME_CN = "cn";*/
	private static final String AD_ATTR_NAME_DISPLAY_NAME = "displayName";
	private static final String AD_ATTR_NAME_USER_ACCOUNT_CONTROL = "userAccountControl";
	private static final String AD_ATTR_NAME_USER_EMAIL = "mail";
	/*private static final String AD_ATTR_NAME_GIVEN_NAME = "givenName";
	    private static final String AD_ATTR_NAME_USER_PRINCIPAL_NAME = "userPrincipalName";
	    
	    private static final String AD_ATTR_NAME_GROUP_TYPE = "groupType";
	    private static final String AD_ATTR_NAME_SAM_ACCOUNT_TYPE = "sAMAccountType";
	    private static final String AD_ATTR_NAME_USER_ACCOUNT_CONTROL = "userAccountControl";
	    private static final int FLAG_TO_DISABLE_USER = 0x2;*/
	static Map<String, Object> userInfo=new HashMap<String, Object>();
	private static final Logger log = LogManager.getLogger(LDAPUtil.class);


	public static void main(String[] args) throws NamingException, IOException {
		
		/*isDisabled(ctx, "mh\\testuser2", baseDN);
		disableUser(ctx,userInfo.get("userDn").toString());*/
	/*	enableStaff("2110735");*/	
		log.info("Start of enable user");
		enableStaffRunAppln("20010idvs");
		//enableStaffRunAppln("20004idvs");	
		// disableStaff("20010idvs");
		
		log.info("End of enable user");
	}

	public static void enableStaffRunAppln(String staffid) throws NamingException
	{
		try {
			Map<String, Object> userInfo=new HashMap<String, Object>();
			String baseDN="OU=RO,DC=CH,DC=RO,DC=NET"; 
		//	String baseDN=IDMConstants.returnBaseDN;
			ActiveDirectoryLdapService adLdapService;
				adLdapService=new ActiveDirectoryLdapService();	
			LdapContext ctx;
			ctx=getLdapContext("10.250.30.86", "_svctstidvs", "Holiday@1");
		//	ctx=getLdapContext(IDMConstants.returnLDAPServer, IDMConstants.returnLDAPUserName, IDMConstants.returnLDAPPassword);
			userInfo = getUserInfoByDomainWithUser(ctx, baseDN, "mh\\"+staffid);
			//isDisabled(ctx, "mh\\testuser2", baseDN);
			if(userInfo.get("userDn")!=null)
			{
				log.info("User access for staff id:"+staffid+" is "+userInfo.get("userStatus"));
					enableUser(ctx,userInfo.get("userDn").toString());
					log.info("User enabled: "+staffid);
				
			}
			else
				log.error("User Not Found");

		} catch (NamingException e) {
			//	log.error(e.getMessage());
			Calendar cal = Calendar.getInstance();
			IDMAlert.sendMail(e.getMessage(),"ID Deactivation on AD",cal.getTime());
			/*e.printStackTrace();*/
			throw e;
		}

	}
	public static void disableStaff(String staffid) throws NamingException
	{
		try {
			Map<String, Object> userInfo=new HashMap<String, Object>();
			/*String baseDN="DC=TMH,DC=TSTCO,DC=NET";*/ 
			String baseDN=IDMConstants.returnBaseDN;
			/*ActiveDirectoryLdapService adLdapService;*/
			/*	adLdapService=new ActiveDirectoryLdapService();	*/
			LdapContext ctx;
			//ctx=getLdapContext("10.251.21.91", "tmh\\testuser1", "G0odd@y1");
			ctx=getLdapContext(IDMConstants.returnLDAPServer, IDMConstants.returnLDAPUserName, IDMConstants.returnLDAPPassword);
			userInfo = getUserInfoByDomainWithUser(ctx, baseDN, "mh\\"+staffid);
			//isDisabled(ctx, "mh\\testuser2", baseDN);
			if(userInfo.get("userDn")!=null)
			{
				if(userInfo.get("userStatus").equals("514"))
				{
					log.info("User already disabled");
				}
				else
				{
					disableUser(ctx,userInfo.get("userDn").toString());
					log.info("User disabled: "+staffid);
				}
			}
			else
				log.error("User Not Found");

		} catch (NamingException e) {
			//	log.error(e.getMessage());
			Calendar cal = Calendar.getInstance();
			IDMAlert.sendMail(e.getMessage(),"ID Deactivation on AD",cal.getTime());
			/*e.printStackTrace();*/
			throw e;
		}

	}
	
	
	// disable FY staff
	public static void disableFYStaff(String staffid) throws NamingException
	{
		try {
			Map<String, Object> userInfo=new HashMap<String, Object>();
			/*String baseDN="DC=TMH,DC=TSTCO,DC=NET";*/ 
			String baseDN=IDMConstants.returnFYBaseDN;
			/*ActiveDirectoryLdapService adLdapService;*/
			/*	adLdapService=new ActiveDirectoryLdapService();	*/
			LdapContext ctx;
			//ctx=getLdapContext("10.251.21.91", "tmh\\testuser1", "G0odd@y1");
			ctx=getLdapContext(IDMConstants.returnFYLDAPServer, IDMConstants.returnFYLDAPUserName, IDMConstants.returnFYLDAPPassword);
			userInfo = getUserInfoByDomainWithUser(ctx, baseDN, "mh\\"+staffid);
			//isDisabled(ctx, "mh\\testuser2", baseDN);
			if(userInfo.get("userDn")!=null)
			{
				if(userInfo.get("userStatus").equals("514"))
				{
					log.info("User already disabled");
				}
				else
				{
					disableUser(ctx,userInfo.get("userDn").toString());
					log.info("User disabled: "+staffid);
				}
			}
			else
				log.error("User Not Found");

		} catch (NamingException e) {
			//	log.error(e.getMessage());
			Calendar cal = Calendar.getInstance();
			IDMAlert.sendMail(e.getMessage(),"ID Deactivation on AD",cal.getTime());
			/*e.printStackTrace();*/
			throw e;
		}

	}
	
	public static void enableStaff(String staffid) throws NamingException
	{
		try {
			Map<String, Object> userInfo=new HashMap<String, Object>();
			/*String baseDN="DC=TMH,DC=TSTCO,DC=NET";*/ 
			String baseDN=IDMConstants.returnBaseDN;
			/*ActiveDirectoryLdapService adLdapService;*/
			/*	adLdapService=new ActiveDirectoryLdapService();	*/
			LdapContext ctx;
			//ctx=getLdapContext("10.251.21.91", "tmh\\testuser1", "G0odd@y1");
			ctx=getLdapContext(IDMConstants.returnLDAPServer, IDMConstants.returnLDAPUserName, IDMConstants.returnLDAPPassword);
			userInfo = getUserInfoByDomainWithUser(ctx, baseDN, "mh\\"+staffid);
			//isDisabled(ctx, "mh\\testuser2", baseDN);
			if(userInfo.get("userDn")!=null)
			{
				log.info("User access for staff id:"+staffid+" is "+userInfo.get("userStatus").toString());
					enableUser(ctx,userInfo.get("userDn").toString());
					log.info("User enabled: "+staffid);
				
			}
			else
				log.error("User Not Found");

		} catch (NamingException e) {
			//	log.error(e.getMessage());
			Calendar cal = Calendar.getInstance();
			IDMAlert.sendMail(e.getMessage(),"ID Deactivation on AD",cal.getTime());
			/*e.printStackTrace();*/
			throw e;
		}

	}

	// enable FY staff
	public static void enableFYStaff(String staffid) throws NamingException
	{
		try {
			Map<String, Object> userInfo=new HashMap<String, Object>();
			/*String baseDN="DC=TMH,DC=TSTCO,DC=NET";*/ 
			String baseDN=IDMConstants.returnFYBaseDN;
			/*ActiveDirectoryLdapService adLdapService;*/
			/*	adLdapService=new ActiveDirectoryLdapService();	*/
			LdapContext ctx;
			//ctx=getLdapContext("10.251.21.91", "tmh\\testuser1", "G0odd@y1");
			ctx=getLdapContext(IDMConstants.returnFYLDAPServer, IDMConstants.returnFYLDAPUserName, IDMConstants.returnFYLDAPPassword);
			userInfo = getUserInfoByDomainWithUser(ctx, baseDN, "mh\\"+staffid);
			//isDisabled(ctx, "mh\\testuser2", baseDN);
			if(userInfo.get("userDn")!=null)
			{
				log.info("User access for staff id:"+staffid+" is "+userInfo.get("userStatus").toString());
					enableUser(ctx,userInfo.get("userDn").toString());
					log.info("User enabled: "+staffid);
				
			}
			else
				log.error("User Not Found");

		} catch (NamingException e) {
			//	log.error(e.getMessage());
			Calendar cal = Calendar.getInstance();
			IDMAlert.sendMail(e.getMessage(),"ID Deactivation on AD",cal.getTime());
			/*e.printStackTrace();*/
			throw e;
		}

	}

	/**
	 * 
	 * @param attrs
	 * @param propertyName
	 * @return the value of the property.
	 */
	public static String getString(Attributes attrs, String propertyName) {
		String value = "";

		if (null != attrs) {
			Attribute attr = attrs.get(propertyName);
			if (null != attr) {
				value = String.valueOf(attr);
				value = value.substring(value.indexOf(": ") + 2).trim();
			}
		}

		return value;
	}

	/**
	 * 
	 * @param host
	 * @param port
	 * @param username
	 * @param password
	 * @return 
	 * @return true if passed the authenticate, or else false.
	 * @throws NamingException 
	 */
	public static void authenticate(String host, int port, String username, String password) 
			throws NamingException {

		LdapContext ctx = getLdapContext(host,  username, password);
		if(null != ctx){
			ctx.close();
		}

	}

	/**
	 * 
	 * @param host
	 *            host name or IP address
	 * @param port
	 *            port for LDAP protocol
	 * @param username
	 * @param password
	 * @return the LDAP context
	 * @throws NamingException
	 */
	public static LdapContext getLdapContext(String host,  String username, String password)
			throws NamingException {

		Hashtable<String, String> env = new Hashtable<String, String>();

		env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
		env.put(Context.PROVIDER_URL, "ldap://" + host );
		env.put(Context.SECURITY_AUTHENTICATION, "simple");
		env.put(Context.SECURITY_PRINCIPAL, username);
		env.put(Context.SECURITY_CREDENTIALS, password);
		env.put("java.naming.ldap.attributes.binary", "tokenGroups objectSid objectGUID");

		LdapContext ctx = null;
		try {
			ctx = new InitialLdapContext(env, null);
		} catch (Exception e) {

			log.info(e);
		}
		return ctx;
	}
	public static boolean isDisabled(LdapContext ctx, String username, String baseDn) throws NamingException, IOException  {

		boolean disabled = false;

		String filter = "sAMAccountName=" + username;
		SearchControls searchCtls = new SearchControls();
		searchCtls.setSearchScope(SearchControls.SUBTREE_SCOPE);

		searchCtls.setCountLimit(1);

		searchCtls.setTimeLimit(0);

		// We want 500 results per request.
		ctx.setRequestControls(new Control[] { new PagedResultsControl(1,
				Control.CRITICAL) });

		// We only want to retrieve the "distinguishedName" attribute.
		// You can specify other attributes/properties if you want here.
		String returnedAtts[] = { "userAccountControl" };
		searchCtls.setReturningAttributes(returnedAtts);

		NamingEnumeration<SearchResult> answer = ctx.search(baseDn, filter,
				searchCtls);

		// Loop through the search results.
		if (answer.hasMoreElements()) {
			/*  SearchResult sr = answer.next();*/
			/* Attributes attr = sr.getAttributes();*/
			/*  long userAccountControl = Long.parseLong(getString(attr, returnedAtts[0]));*/
			/*  if(isDisabled(userAccountControl)){
	            disabled = true;
	        }*/
		}
		return disabled;

	}

	/**
	 * Remove the user from group.
	 * 
	 * @param ctx
	 * @param userDn
	 * @param groupDn
	 * @return
	 * @throws NamingException 
	 * @throws Exception
	 */
	public static void removeFromGroup(LdapContext ctx, String userDn, String groupDn) 
			throws NamingException {

		ModificationItem[] mods = new ModificationItem[1];
		mods[0] = new ModificationItem(DirContext.REMOVE_ATTRIBUTE, new BasicAttribute("member", userDn));
		ctx.modifyAttributes(groupDn, mods);

	}

	/**
	 * Disable the account
	 * 
	 * @param ctx
	 * @param dn
	 * @throws NamingException
	 */
	public static void disableUser(LdapContext ctx, String dn)
			throws NamingException {
		//  int newUserAccountControl = /*Integer.parseInt(AD_ATTR_NAME_USER_ACCOUNT_CONTROL) |*/ FLAG_TO_DISABLE_USER;
		ModificationItem[] mods = new ModificationItem[1];
		mods[0] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE,
				new BasicAttribute("userAccountControl",
						"514"));// To disable 514. To enable 512
		ctx.modifyAttributes(dn, mods);

	}
	
	public static void enableUser(LdapContext ctx, String dn)
			throws NamingException {
		//  int newUserAccountControl = /*Integer.parseInt(AD_ATTR_NAME_USER_ACCOUNT_CONTROL) |*/ FLAG_TO_DISABLE_USER;
		ModificationItem[] mods = new ModificationItem[1];
		mods[0] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE,
				new BasicAttribute("userAccountControl",
						"512"));// To disable 514. To enable 512
		ctx.modifyAttributes(dn, mods);

	}

	public static Map<String, Object> getUserInfoByDomainWithUser(LdapContext ctx, String searchBase, String domainWithUser) throws NamingException 
	{
		//ctx=getLdapContext(IDMConstants.returnLDAPServer, IDMConstants.returnLDAPUserName, IDMConstants.returnLDAPPassword);
		//ctx=getLdapContext("10.250.25.214", IDMConstants.returnLDAPUserName, IDMConstants.returnLDAPPassword);
		
		String userName = domainWithUser.substring(domainWithUser.indexOf('\\') +1 );
		try
		{
			NamingEnumeration<SearchResult> userDataBysAMAccountName = getUserDataBysAMAccountName(ctx, searchBase, userName);

			return getUserInfoFromSearchResults( userDataBysAMAccountName );
		}
		catch(Exception e)
		{
			/*e.printStackTrace();*/
			throw new RuntimeException(e);
		}
	}



	private static NamingEnumeration<SearchResult> getUserDataBysAMAccountName(LdapContext ctx, String searchBase, String username)throws Exception 
	{
		String filter = "(&(&(objectClass=person)	(objectCategory=user))(sAMAccountName=" + username + "))";
		SearchControls searchCtls = new SearchControls();

		// ctx.reconnect(ctx.getResponseControls());

		
		//To search by distinguish attribute. Only these attributes can be fetched while retriving the attributes in getUserMailFromSearchResults
		 String[] resultAttributes = {"cn", "distinguishedName", "displayName", "lastLogon", "description", "mail","userAccountControl"};
    searchCtls.setReturningAttributes(resultAttributes);
		searchCtls.setSearchScope(SearchControls.SUBTREE_SCOPE);
		NamingEnumeration<SearchResult> answer = null;
		try
		{
			answer = ctx.search(searchBase, filter, searchCtls);
		}
		catch (Exception e)
		{
			log.error(e.getMessage());
			throw e;
		}

		return answer;
	}        

	private static Map<String, Object> getUserInfoFromSearchResults( NamingEnumeration<SearchResult> userData ) 
			throws Exception 
			{
		try
		{
			String mail = new String() ;
			String displayname = new String();
			String userDn=new String();
			String userStatus=new String();
			// getting only the first result if we have more than one
			if (userData.hasMoreElements())
			{
				SearchResult sr = userData.nextElement();
				Attributes attributes = sr.getAttributes();

				//   mail = attributes.get(AD_ATTR_NAME_USER_EMAIL).get().toString();
				//   if(attributes.get(AD_ATTR_NAME_DISPLAY_NAME).get()!=null)
				if(attributes.get(AD_ATTR_NAME_USER_EMAIL)!=null)
					mail=attributes.get(AD_ATTR_NAME_USER_EMAIL).get().toString();
				else
					mail=null;
				if(attributes.get(AD_ATTR_NAME_DISPLAY_NAME)!=null)
					displayname=attributes.get(AD_ATTR_NAME_DISPLAY_NAME).get().toString();
				else
					displayname=null;
				if(attributes.get(AD_ATTR_NAME_DISTINGUISHED_NAME)!=null)
					userDn=attributes.get(AD_ATTR_NAME_DISTINGUISHED_NAME).get().toString();
				else
					userDn=null;
				if(attributes.get(AD_ATTR_NAME_USER_ACCOUNT_CONTROL)!=null)
					userStatus=attributes.get(AD_ATTR_NAME_USER_ACCOUNT_CONTROL).get().toString(); 
				else
					userStatus=null;
				/* log.info("Attributes::"+attributes.toString());
		            log.info("NAME::"+displayname+"\t EMAIL ::"+mail);*/
				//logger.debug("found email " + mail);

			}
			else
			{
				userDn=null;
				displayname=null;
				userStatus=null;
				mail=null;
			}

			userInfo.put("mail", mail);
			userInfo.put("StaffName", displayname);
			userInfo.put("userDn", userDn);
			userInfo.put("userStatus", userStatus);

			return userInfo;
		}
		catch (Exception e)
		{
			log.error(e.getMessage());
			//logger.error("Error fetching attribute from object");
			throw e;
		}        
			}
	
	
	/**
	 * Validating Pulsera Previous months Inactive StaffID in LDAP System
	 * @param staffid
	 * @throws NamingException
	 */
	public static ArrayList<StaffVo> validateStaffInLDAP(ArrayList<StaffVo> inActiveStaffData) throws NamingException
	{
		try {
			Map<String, Object> userInfo=new HashMap<String, Object>();
			String baseDN=IDMConstants.returnBaseDN;
			LdapContext ctx;
			ctx=getLdapContext(IDMConstants.returnLDAPServer, IDMConstants.returnLDAPUserName, IDMConstants.returnLDAPPassword);
			if(inActiveStaffData!=null && !inActiveStaffData.isEmpty()){
				for (StaffVo staffVo : inActiveStaffData) {
					userInfo = getUserInfoByDomainWithUser(ctx, baseDN, "mh\\" + staffVo.getStaffId());
					if (userInfo.get("userDn") != null) {
						if (userInfo.get("userStatus").equals("514")) {
							log.info("Staff disabled in LDAP ");
							staffVo.setADStatus("Inactive");
						} else {							
							log.info("User NOT disabled in LDAP: " +"mh\\" + staffVo.getStaffId());
							staffVo.setADStatus("Active");
						}
					} else{
						log.error("User Not Found");
						staffVo.setADStatus("Not Present");
					}
				}
			}
		} catch (NamingException e) {
			Calendar cal = Calendar.getInstance();
			IDMAlert.sendMail(e.getMessage(),"ID Deactivation on AD",cal.getTime());
			throw e;
		}
		return inActiveStaffData;
	}

}
