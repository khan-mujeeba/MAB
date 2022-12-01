package com.mas.idm.batch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.naming.AuthenticationException;
import javax.naming.CommunicationException;
import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.ModificationItem;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.InitialLdapContext;
import javax.naming.ldap.LdapContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.mas.idm.bean.ADUpdateVO;
import com.mas.idm.dao.CreateUserADDAO;
import com.mas.idm.dao.SFFileReaderDAO;
import com.mas.idm.util.IDMAlert;
import com.mas.idm.util.IDMConstants;
import com.mas.idm.util.SFIntegrationConstants;

/**
 * The Class LDAPUtil.
 * 
 * @author 1030502
 * @version 1.0
 */
public class DataInsertionAD {

	static ResourceBundle bundle = ResourceBundle.getBundle("ApplicationResources");
	/** The Constant log. */
	private static final Logger log = LogManager.getLogger(DataInsertionAD.class);

	/** The Constant AD_ATTR_NAME_DISPLAY_NAME. */
	private static final String AD_ATTR_NAME_DISPLAY_NAME = "displayName";

	/** The user info. */
	static Map<String, Object> userInfo = new HashMap<String, Object>();

	static Map<String, Object> managerInfo = new HashMap<String, Object>();

	static Map<String, Object> userMailId = new HashMap<String, Object>();
	/** The Constant AD_ATTR_NAME_USER_EMAIL. */
	private static final String AD_ATTR_NAME_USER_EMAIL = "mail";

	private static final String AD_ATTR_NAME_USER_MOBILE = "mobile";

	private static final String AD_ATTR_NAME_USER_PSWDSET = "pwdLastSet";

	private static final String AD_ATTR_NAME_USER_CN = "distinguishedName";

	private static final String AD_ATTR_NAME_USER_ACCOUNTCONTROL = "userAccountControl";
	private static final String baseDN = "DC=MH,DC=CORP,DC=NET";

	private CreateUserADDAO userADDAO;

	public CreateUserADDAO getUserADDAO() {
		return userADDAO;
	}

	public void setUserADDAO(CreateUserADDAO userADDAO) {
		this.userADDAO = userADDAO;
	}

	private SFFileReaderDAO fileReaderDAO;

	public SFFileReaderDAO getFileReaderDAO() {
		return fileReaderDAO;
	}

	public void setFileReaderDAO(SFFileReaderDAO fileReaderDAO) {
		this.fileReaderDAO = fileReaderDAO;
	}

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 * @throws NamingException the naming exception
	 */
	public String insertNewHireToAD() throws NamingException {
		log.info("START: insertNewHireToAD***");

	//	createUser(IDMConstants.returnLDAPServer);
	//	modifyUser(IDMConstants.returnLDAPServer);
		createFYUser(IDMConstants.returnFYLDAPServer);
		modifyFYUser(IDMConstants.returnFYLDAPServer);
		
		log.info("END: insertNewHireToAD***");
		return "success";

	}

	public void createUser(String host) {

		log.info("START: Inside createUser***");

		String username = "";
		String completeUserName = "";
		String LocationCode = "";
		String AdOU = "";
		try {
			List<ADUpdateVO> newRehireAdDataList = new ArrayList<ADUpdateVO>();
			newRehireAdDataList = userADDAO.getNewHireData();

			List<String[]> paramsOut = userADDAO.getParamValues(
					Arrays.asList(SFIntegrationConstants.JAPANESE_CODES, SFIntegrationConstants.EXCLUDED));

			String japaneseLocation__NotFound_in_ad = "";
			String excludedNames_NotFound_in_ad = "";

			for (String[] param : paramsOut) {
				if (SFIntegrationConstants.JAPANESE_CODES.equals(param[0])) {
					japaneseLocation__NotFound_in_ad = param[1];
				} else if (SFIntegrationConstants.EXCLUDED.equals(param[0])) {
					excludedNames_NotFound_in_ad = param[1];
				}
			}

			// String japaneseLocation__NotFound_in_ad =
			// userADDAO.getParamValues(SFIntegrationConstants.JAPANESE_CODES);
			String[] locationCode_NotFound_in_ad = japaneseLocation__NotFound_in_ad.split(",");
			List<String> locationCodes__NotFound_in_ad = Arrays.asList(locationCode_NotFound_in_ad);

			// String excludedNames_NotFound_in_ad =
			// userADDAO.getParamValues(SFIntegrationConstants.EXCLUDED);
			String[] excludeNames_NotFound_in_ad = excludedNames_NotFound_in_ad.split(",");
			List<String> excluded_NotFound_in_ad = Arrays.asList(excludeNames_NotFound_in_ad);

			// AD INSERTION
			ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
			String certPath = classLoader.getResource(bundle.getString("idvs.certPath")).getPath();
			log.info("CertPath ---- " + certPath);
			System.setProperty("javax.net.ssl.trustStore", certPath);
			System.setProperty("javax.net.ssl.trustStorePassword", "changeit");

			Hashtable<String, String> env = new Hashtable<String, String>();
			env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
			env.put(Context.SECURITY_PROTOCOL, "ssl");
			env.put(Context.PROVIDER_URL, "ldap://" + host + ":636");
			env.put(Context.SECURITY_AUTHENTICATION, "simple");
			env.put(Context.SECURITY_PRINCIPAL, IDMConstants.returnLDAPUserName);
			env.put(Context.SECURITY_CREDENTIALS, IDMConstants.returnLDAPPassword);

			LdapContext ctx = null;

			Attributes container = new BasicAttributes();

			// Create the objectclass to add
			Attribute objClasses = new BasicAttribute("objectClass");
			objClasses.add("top");
			objClasses.add("person");
			objClasses.add("organizationalPerson");
			objClasses.add("user");

			// passing the values in AD
			// passing the new rehire values to AD
			if (newRehireAdDataList.size() > 0) {
				// Updating the new changes 18/01/18
				for (ADUpdateVO list : newRehireAdDataList) {
					log.info("newRehireAdDataList STaff id" + list.getStaffId());

					String staffId = list.getStaffId();
					String Manager = list.getSuperior();

					try {

						log.info("Inside try method for getting ldap details of the user" + staffId);
						Map<String, Object> userInfo = new HashMap<String, Object>();
						Map<String, Object> managerInfo = new HashMap<String, Object>();

						String baseDN = IDMConstants.returnBaseDN;
						ctx = getLdapContext(IDMConstants.returnLDAPServer, IDMConstants.returnLDAPUserName,
								IDMConstants.returnLDAPPassword);
						ctx.close();
						ctx = new InitialLdapContext(env, null);
						userInfo = getUserInfoByDomainWithUser(ctx, baseDN, "mh\\" + staffId);

						managerInfo = getUserInfoByDomainWithManager(ctx, baseDN, "mh\\" + Manager);

						String EmployeeCategory = list.getEmployeeCategory();
						// Check whether Id is present in AD or not

						if (userInfo.get("cn") != null) {

							if (userInfo.get("userAccountControl").toString().equalsIgnoreCase("514")) {

								log.info("User status is not active => " + list.getStaffId());
								username = list.getStaffId();

								LocationCode = list.getLocationCode();
								AdOU = list.getAdOU();

								if (EmployeeCategory.equalsIgnoreCase("Top Management")) {
									completeUserName = "CN=" + username + "," + "OU=" + LocationCode + ","
											+ "OU=VVIP,OU=MH" + "," + IDMConstants.returnBaseDN;
								} else if (AdOU.equalsIgnoreCase("MALAYSIA")) {
									completeUserName = "CN=" + username + "," + "OU=" + LocationCode + ","
											+ "OU=Malaysia,OU=MH" + "," + IDMConstants.returnBaseDN;
								} else if (AdOU.equalsIgnoreCase("INTLW")) {
									completeUserName = "CN=" + username + "," + "OU=" + LocationCode + ","
											+ "OU=INTLW,OU=MH" + "," + IDMConstants.returnBaseDN;
								} else {
									completeUserName = "CN=" + username + "," + "OU=" + LocationCode + ","
											+ "OU=INTLE,OU=MH" + "," + IDMConstants.returnBaseDN;
								}
								// Changing the OU details
								ctx.rename(userInfo.get("cn").toString(), completeUserName);

								ctx.close();
								ctx = new InitialLdapContext(env, null);
								userInfo = getUserInfoByDomainWithUser(ctx, baseDN, "mh\\" + staffId);

								int UF_NORMAL_ACCOUNT = 0x0200;
								ModificationItem[] mods1 = new ModificationItem[1];
								ModificationItem[] mods2 = new ModificationItem[2];

								// added
								final int mid = username.length() / 2; // get the middle of the String
								log.info(mid);
								String[] parts = { username.substring(0, mid), username.substring(mid) };
								log.info(parts[0]); // first part
								log.info(parts[1]);
								String password = "Mab@" + parts[0] + "#" + parts[1];
								String newQuotedPassword = "\"" + password + "\"";

								char unicodePwd[] = newQuotedPassword.toCharArray();
								byte pwdArray[] = new byte[unicodePwd.length * 2];
								for (int i = 0; i < unicodePwd.length; i++) {
									pwdArray[i * 2 + 1] = (byte) (unicodePwd[i] >>> 8);
									pwdArray[i * 2 + 0] = (byte) (unicodePwd[i] & 0xff);
								}
								mods1[0] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE,
										new BasicAttribute("userAccountControl", Integer.toString(UF_NORMAL_ACCOUNT)));
								ctx.modifyAttributes(userInfo.get("cn").toString(), mods1);

								ctx.close();

								ctx = new InitialLdapContext(env, null);
								mods2[0] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE,
										new BasicAttribute("unicodePwd", pwdArray));
								mods2[1] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE,
										new BasicAttribute("pwdLastSet", Integer.toString(0)));
								ctx.modifyAttributes(userInfo.get("cn").toString(), mods2);
								// modifying the other details

								String email = userInfo.get("email").toString();
								log.info("Final mail id=====" + email);

								modifyUserDetails(ctx, userInfo.get("cn").toString(), list, email, managerInfo);
								// Updating the mail to IDVS DB
								fileReaderDAO.updateEmailToDB(email, staffId);
								log.info(" if block User details are modified in AD: " + staffId);

							} else {
								log.info("User is active => " + list.getStaffId());

								// Existing mail id will be used even user is active
								String email = userInfo.get("email").toString();
								username = list.getStaffId();
								LocationCode = list.getLocationCode();
								AdOU = list.getAdOU();

								if (EmployeeCategory.equalsIgnoreCase("Top Management")) {
									completeUserName = "CN=" + username + "," + "OU=" + LocationCode + ","
											+ "OU=VVIP,OU=MH" + "," + IDMConstants.returnBaseDN;
								} else if (AdOU.equalsIgnoreCase("MALAYSIA")) {
									completeUserName = "CN=" + username + "," + "OU=" + LocationCode + ","
											+ "OU=Malaysia,OU=MH" + "," + IDMConstants.returnBaseDN;
								} else if (AdOU.equalsIgnoreCase("INTLW")) {
									completeUserName = "CN=" + username + "," + "OU=" + LocationCode + ","
											+ "OU=INTLW,OU=MH" + "," + IDMConstants.returnBaseDN;
								} else {
									completeUserName = "CN=" + username + "," + "OU=" + LocationCode + ","
											+ "OU=INTLE,OU=MH" + "," + IDMConstants.returnBaseDN;
								}
								// Changing the OU details
								ctx.rename(userInfo.get("cn").toString(), completeUserName);
								ctx.close();
								ctx = new InitialLdapContext(env, null);
								userInfo = getUserInfoByDomainWithUser(ctx, baseDN, "mh\\" + staffId);
								log.info(userInfo.get("cn").toString());

								modifyUserDetails(ctx, userInfo.get("cn").toString(), list, email, managerInfo);
								fileReaderDAO.updateEmailToDB(email, staffId);

								log.info("else block - update mail ID in DB staffId " + staffId + " email " + email);
							}
						}

						else {

							log.info(" else block " + staffId);

							String email = null;
							String firstName = list.getFirstName();
							String lastName = list.getLastName();
							StringBuilder str = new StringBuilder(firstName.toLowerCase());
							StringBuilder str1 = new StringBuilder(lastName.toLowerCase());
							for (String names : excluded_NotFound_in_ad) {
								if (firstName.toLowerCase().contains(names.toLowerCase())) {

									int i = str.indexOf(names.toLowerCase());

									str.delete(i, i + names.length() + 1);
									String finalFirstName = camelCase(str);
									firstName = finalFirstName;
								}
								if (lastName.toLowerCase().contains(names.toLowerCase())) {
									int i = str1.indexOf(names.toLowerCase());

									str1.delete(i, i + names.length() + 1);
									String finalLastName = camelCase(str1);
									lastName = finalLastName;
									// System.out.println(finalLastName);
								} else {
									firstName = camelCase(str);
									lastName = camelCase(str1);
								}

							}

							// ADDED TO INTERCHANGE JAPANESE FIRST AND LAST NAMES
							if (locationCodes__NotFound_in_ad.contains(list.getLocationCode())) {

								String email1 = firstName.trim();
								email1 = email1.replaceAll("[\\s ']", "");

								// email1=email1.replace
								email1 = email1.toLowerCase();
								String email2 = lastName.trim();
								email2 = email2.replaceAll("[\\s ']", "");
								email2 = email2.toLowerCase();
								email = email2 + "." + email1 + "@malaysiaairlines.com";
								log.info("Mail id=====" + email);

							} else {

								String email1 = firstName.trim();
								email1 = email1.replaceAll("[\\s ']", "");
								email1 = email1.toLowerCase();
								String email2 = lastName.trim();
								email2 = email2.replaceAll("[\\s ']", "");
								email2 = email2.toLowerCase();
								email = email1 + "." + email2 + "@malaysiaairlines.com";
								log.info("Mail id=====" + email);

							}

							String finalMail = "";
							// Check whether email is already exists in AD
							// Added 27/09/18
							while (true) {
								ctx = new InitialLdapContext(env, null);
								Map<String, Object> userMail = new HashMap<String, Object>();
								userMail = checkEmailExistInAD(ctx, baseDN, "mh\\" + email);

								if (userMail.get("email") == null) {
									break;
								} else {
									if (userMail.get("email").toString().equalsIgnoreCase(email)) {
										String result = "";
										String regex = "(.)*(\\d)(.)*";
										Pattern pattern = Pattern.compile(regex);
										String fine = "";

										String[] parts = email.split("@");
										String part1 = parts[0]; // 004
										part1 = part1.replaceAll("\\s", "");
										log.info(part1);
										String part2 = parts[1]; // 034556
										log.info(part2);

										Matcher matcher = pattern.matcher(part1);
										boolean isMatched = matcher.matches();
										if (isMatched) {

											String[] num = part1.split("(?<=\\D)(?=\\d)");
											String num1 = num[0];

											String num2 = num[1];
											int foo = Integer.parseInt(num2);
											int fin = foo + 1;
											if (fin < 10) {
												fine = "0" + Integer.toString(fin);
											} else {
												fine = Integer.toString(fin);
											}
											result = num1 + fine;
											// log.info("num==" + result);
											finalMail = result + "@" + part2;
										} else {
											finalMail = part1 + "01" + "@" + part2;
										}
									}

									log.info("Final mail id=====" + finalMail);

								}
								email = finalMail;
							}
							// end
							username = list.getStaffId();
							String fullName = list.getFullName();
							StringBuilder str2 = new StringBuilder(fullName.toLowerCase());
							String userFullName = camelCase(str2);
							LocationCode = list.getLocationCode();
							AdOU = list.getAdOU();
							String CostCentre = list.getCostCenter();

							Attribute division;
							Attribute department;
							Attribute mobile;
							Attribute description;
							Attribute postalCode;
							Attribute manager;
							Attribute co;
							Attribute st;
							Attribute company;
							Attribute streetAddress;
							Attribute title;
							Attribute city;
							Attribute extensionAttribute4;
							Attribute extensionAttribute3;
							String cnValue = username;
							Attribute cn = new BasicAttribute("cn", cnValue);
							Attribute displayName = new BasicAttribute("displayName", userFullName);
							Attribute sAMAccountName = new BasicAttribute("sAMAccountName", username);
							Attribute principalName = new BasicAttribute("userPrincipalName", email);
							Attribute mail = new BasicAttribute("mail", email);
							Attribute givenName = new BasicAttribute("givenName", firstName);
							Attribute sn = new BasicAttribute("sn", lastName);
							// added
							// Attribute lastpswdSet = new BasicAttribute("pwdLastSet ", "0");
							if (list.getDivisionName().equalsIgnoreCase("")) {
								division = new BasicAttribute("division", "null");
							} else {
								division = new BasicAttribute("division", list.getDivisionName());
							}

							// log.info("manager info=-===="+result);
							if (managerInfo.get("cn") == null) {
								manager = new BasicAttribute("manager", "");
							} else {
								// manager =new BasicAttribute("manager",list.getSuperior());
								// doubt
								manager = new BasicAttribute("manager", managerInfo.get("cn").toString());
								container.put(manager);
							}
							// ENd

							if (list.getDepartmentName().equalsIgnoreCase("")) {
								department = new BasicAttribute("department", "null");
							} else {
								department = new BasicAttribute("department", list.getDepartmentName());
							}

							if (list.getLegalEntity().equalsIgnoreCase("")) {
								company = new BasicAttribute("company", "null");
							} else {
								company = new BasicAttribute("company", list.getLegalEntity());
							}

							if (list.getPhoneNO().equalsIgnoreCase("")) {
								mobile = new BasicAttribute("mobile", "null");
							} else {
								mobile = new BasicAttribute("mobile", list.getPhoneNO());
							}

							if (list.getStreetAddress().equalsIgnoreCase("")) {
								streetAddress = new BasicAttribute("streetAddress", "null");
							} else {
								streetAddress = new BasicAttribute("streetAddress", list.getStreetAddress());
							}
							if (list.getState().equalsIgnoreCase("")) {
								st = new BasicAttribute("st", "null");
							} else {
								st = new BasicAttribute("st", list.getState());
							}
							if (list.getPostalCode().equalsIgnoreCase("")) {
								postalCode = new BasicAttribute("postalCode", "null");
							} else {
								postalCode = new BasicAttribute("postalCode", list.getPostalCode());
							}

							if (list.getCountry().equalsIgnoreCase("")) {
								co = new BasicAttribute("co", "null");
							} else {
								co = new BasicAttribute("co", list.getCountry());
							}

							if (list.getDesignation().equalsIgnoreCase("")) {
								title = new BasicAttribute("title", "null");
							} else {
								title = new BasicAttribute("title", list.getDesignation());
							}
							if (list.getCity().equalsIgnoreCase("")) {
								city = new BasicAttribute("l", "null");
							} else {
								city = new BasicAttribute("l", list.getCity());
							}

							if (CostCentre.equalsIgnoreCase("")) {
								extensionAttribute4 = new BasicAttribute("extensionAttribute4", "null");
							} else {
								extensionAttribute4 = new BasicAttribute("extensionAttribute4", CostCentre);
							}

							// end
							if (list.getJobGrade().equalsIgnoreCase("")) {
								extensionAttribute3 = new BasicAttribute("extensionAttribute3", "null");
							} else {
								extensionAttribute3 = new BasicAttribute("extensionAttribute3", list.getJobGrade());
							}

							Attribute uid = new BasicAttribute("uid", username);
							Attribute physicalDeliveryOfficeName = new BasicAttribute("physicalDeliveryOfficeName",
									list.getLocationCode());

							if (EmployeeCategory.equalsIgnoreCase("Top Management")) {
								completeUserName = "CN=" + username + "," + "OU=" + LocationCode + "," + "OU=VVIP,OU=MH"
										+ "," + IDMConstants.returnBaseDN;
							} else if (AdOU.equalsIgnoreCase("MALAYSIA")) {
								completeUserName = "CN=" + username + "," + "OU=" + LocationCode + ","
										+ "OU=Malaysia,OU=MH" + "," + IDMConstants.returnBaseDN;
							} else if (AdOU.equalsIgnoreCase("INTLW")) {
								completeUserName = "CN=" + username + "," + "OU=" + LocationCode + ","
										+ "OU=INTLW,OU=MH" + "," + IDMConstants.returnBaseDN;
							} else {
								completeUserName = "CN=" + username + "," + "OU=" + LocationCode + ","
										+ "OU=INTLE,OU=MH" + "," + IDMConstants.returnBaseDN;
							}

							int UF_NORMAL_ACCOUNT = 0x0200;

							container.put(objClasses);
							container.put(sAMAccountName);
							container.put(principalName);
							container.put(cn);
							container.put(sn);
							container.put(givenName);
							container.put(uid);
							container.put(displayName);
							container.put(division);
							container.put(department);
							container.put(company);
							container.put(streetAddress);
							container.put(st);
							container.put(postalCode);
							container.put(city);
							container.put(co);
							container.put(mail);

							container.put(physicalDeliveryOfficeName);
							container.put(extensionAttribute4);
							container.put(extensionAttribute3);

							container.put(title);
							container.put(mobile);
							// added

							try {
								ctx = new InitialLdapContext(env, null);
								
								log.info(" completeUserName => "+completeUserName);
								log.info(" container => "+container);
								
								ctx.createSubcontext(completeUserName, container);

								ModificationItem[] mods = new ModificationItem[3];

								final int mid = username.length() / 2; // get the middle of the String

								String[] parts = { username.substring(0, mid), username.substring(mid) };

								String password = "Mab@" + parts[0] + "#" + parts[1];
								String newQuotedPassword = "\"" + password + "\"";
								byte[] newUnicodePassword = newQuotedPassword.getBytes("UTF-16LE");

								mods[0] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE,
										new BasicAttribute("unicodePwd", newUnicodePassword));

								mods[2] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE,
										new BasicAttribute("userAccountControl", Integer.toString(UF_NORMAL_ACCOUNT)));

								mods[1] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE,
										new BasicAttribute("pwdLastSet", Integer.toString(0)));

								ctx.modifyAttributes(completeUserName, mods);

								// RAJI END

								fileReaderDAO.updateEmailToDB(email, staffId);

								log.info("update mail ID in DB staffId " + staffId + " email " + email);

								log.info("Set password & updated userccountControl");
							} catch (Exception e)

							{
								log.error(e);
								Calendar cal = Calendar.getInstance();
								IDMAlert.sendMailToSupport(e.getMessage(),
										"Failed : AD Creation for on boarding staff " + staffId + " <br><br>"+container+"  <br><br>"+ list, cal.getTime(),
										staffId);
							}
						}
					}
					catch (NamingException e) {

						log.error(e);

						Calendar cal = Calendar.getInstance();
						IDMAlert.sendMailToSupport(e.getMessage(),
								"Failed : AD Creation for on boarding staff " + staffId+ " <br><br>"+container +"  <br><br>"+ list, cal.getTime(), staffId);
					} catch (Exception e) {
						log.error(e.getMessage());
						Calendar cal = Calendar.getInstance();
						IDMAlert.sendMailToSupport(e.getMessage(),
								"Failed : AD Creation for on boarding staff " + staffId+ " <br><br>"+container+"  <br><br>"+ list, cal.getTime(), staffId);
						log.error("Error occured while adding user in Active Directory for:" + staffId);

					} finally {
						try {
							ctx.close();
						} catch (NamingException e) {
							// TODO Auto-generated catch block
							log.error(e);
						}
					}
				}

			} else {

				log.info("No New hire data available in DB for AD Updation");
			}
		} catch (Exception e) {
			log.error(e.getMessage());
			Calendar cal = Calendar.getInstance();
			log.error("Error occured while adding user in Active Directory" + e);
		}
		
		log.info("END: Inside createUser***");
	}

	// Creating user in FY-AD 
	// @mujeeba.khan
	
	public void createFYUser(String host) {

		log.info("START: Inside createUser***");

		String username = "";
		String completeUserName = "";
		String LocationCode = "";
		String AdOU = "";
		try {
			List<ADUpdateVO> newRehireAdDataList = new ArrayList<ADUpdateVO>();
			newRehireAdDataList = userADDAO.getFYNewHireData();

			List<String[]> paramsOut = userADDAO.getParamValues(
					Arrays.asList(SFIntegrationConstants.JAPANESE_CODES, SFIntegrationConstants.EXCLUDED));

			String japaneseLocation__NotFound_in_ad = "";
			String excludedNames_NotFound_in_ad = "";

			for (String[] param : paramsOut) {
				if (SFIntegrationConstants.JAPANESE_CODES.equals(param[0])) {
					japaneseLocation__NotFound_in_ad = param[1];
				} else if (SFIntegrationConstants.EXCLUDED.equals(param[0])) {
					excludedNames_NotFound_in_ad = param[1];
				}
			}

			// String japaneseLocation__NotFound_in_ad =
			// userADDAO.getParamValues(SFIntegrationConstants.JAPANESE_CODES);
			String[] locationCode_NotFound_in_ad = japaneseLocation__NotFound_in_ad.split(",");
			List<String> locationCodes__NotFound_in_ad = Arrays.asList(locationCode_NotFound_in_ad);

			// String excludedNames_NotFound_in_ad =
			// userADDAO.getParamValues(SFIntegrationConstants.EXCLUDED);
			String[] excludeNames_NotFound_in_ad = excludedNames_NotFound_in_ad.split(",");
			List<String> excluded_NotFound_in_ad = Arrays.asList(excludeNames_NotFound_in_ad);

			// AD INSERTION
			
			Hashtable<String, String> env = new Hashtable<String, String>();
			env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
			//env.put(Context.SECURITY_PROTOCOL, "ssl");
			env.put(Context.PROVIDER_URL, "ldap://" + host + ":389");
			env.put(Context.SECURITY_AUTHENTICATION, "simple");
			env.put(Context.SECURITY_PRINCIPAL, IDMConstants.returnFYLDAPUserName);
			env.put(Context.SECURITY_CREDENTIALS, IDMConstants.returnFYLDAPPassword);


			LdapContext ctx = null;

			Attributes container = new BasicAttributes();

			// Create the objectclass to add
			Attribute objClasses = new BasicAttribute("objectClass");
			objClasses.add("top");
			objClasses.add("person");
			objClasses.add("organizationalPerson");
			objClasses.add("user");

			// passing the values in AD
			// passing the new rehire values to AD
			if (newRehireAdDataList.size() > 0) {
				// Updating the new changes 18/01/18
				for (ADUpdateVO list : newRehireAdDataList) {
					log.info("newRehireAdDataList STaff id" + list.getStaffId());

					String staffId = list.getStaffId();
					String Manager = list.getSuperior();

					try {

						log.info("Inside try method for getting ldap details of the user" + staffId);
						Map<String, Object> userInfo = new HashMap<String, Object>();
						Map<String, Object> managerInfo = new HashMap<String, Object>();

						String baseDN = IDMConstants.returnFYBaseDN;
						ctx = getLdapContext(IDMConstants.returnFYLDAPServer, IDMConstants.returnFYLDAPUserName,
								IDMConstants.returnFYLDAPPassword);
						ctx.close();
						ctx = new InitialLdapContext(env, null);
						userInfo = getUserInfoByDomainWithUser(ctx, baseDN, "mh\\" + staffId);

						managerInfo = getUserInfoByDomainWithManager(ctx, baseDN, "mh\\" + Manager);

						String EmployeeCategory = list.getEmployeeCategory();
						// Check whether Id is present in AD or not

						if (userInfo.get("cn") != null) {

							if (userInfo.get("userAccountControl").toString().equalsIgnoreCase("514")) {

								log.info("User status is not active => " + list.getStaffId());
								username = list.getStaffId();

								LocationCode = list.getLocationCode();
								AdOU = list.getAdOU();

								if (EmployeeCategory.equalsIgnoreCase("Top Management")) {
									completeUserName = "CN=" + username + "," + "OU=" + LocationCode + ","
											+ "OU=VVIP,OU=MH" + "," + IDMConstants.returnFYBaseDN;
								} else if (AdOU.equalsIgnoreCase("MALAYSIA")) {
									completeUserName = "CN=" + username + "," + "OU=" + LocationCode + ","
											+ "OU=Malaysia,OU=MH" + "," + IDMConstants.returnFYBaseDN;
								} else if (AdOU.equalsIgnoreCase("INTLW")) {
									completeUserName = "CN=" + username + "," + "OU=" + LocationCode + ","
											+ "OU=INTLW,OU=MH" + "," + IDMConstants.returnFYBaseDN;
								} else {
									completeUserName = "CN=" + username + "," + "OU=" + LocationCode + ","
											+ "OU=INTLE,OU=MH" + "," + IDMConstants.returnFYBaseDN;
								}
								// Changing the OU details
								ctx.rename(userInfo.get("cn").toString(), completeUserName);

								ctx.close();
								ctx = new InitialLdapContext(env, null);
								userInfo = getUserInfoByDomainWithUser(ctx, baseDN, "mh\\" + staffId);

								int UF_NORMAL_ACCOUNT = 0x0200;
								ModificationItem[] mods1 = new ModificationItem[1];
								ModificationItem[] mods2 = new ModificationItem[2];

								// added
								final int mid = username.length() / 2; // get the middle of the String
								log.info(mid);
								String[] parts = { username.substring(0, mid), username.substring(mid) };
								log.info(parts[0]); // first part
								log.info(parts[1]);
								String password = "Mab@" + parts[0] + "#" + parts[1];
								String newQuotedPassword = "\"" + password + "\"";

								char unicodePwd[] = newQuotedPassword.toCharArray();
								byte pwdArray[] = new byte[unicodePwd.length * 2];
								for (int i = 0; i < unicodePwd.length; i++) {
									pwdArray[i * 2 + 1] = (byte) (unicodePwd[i] >>> 8);
									pwdArray[i * 2 + 0] = (byte) (unicodePwd[i] & 0xff);
								}
								mods1[0] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE,
										new BasicAttribute("userAccountControl", Integer.toString(UF_NORMAL_ACCOUNT)));
								ctx.modifyAttributes(userInfo.get("cn").toString(), mods1);

								ctx.close();

								ctx = new InitialLdapContext(env, null);
								mods2[0] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE,
										new BasicAttribute("unicodePwd", pwdArray));
								mods2[1] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE,
										new BasicAttribute("pwdLastSet", Integer.toString(0)));
								ctx.modifyAttributes(userInfo.get("cn").toString(), mods2);
								// modifying the other details

								String email = userInfo.get("email").toString();
								log.info("Final mail id=====" + email);

								modifyUserDetails(ctx, userInfo.get("cn").toString(), list, email, managerInfo);
								// Updating the mail to IDVS DB
								fileReaderDAO.updateEmailToDB(email, staffId);
								log.info(" if block User details are modified in AD: " + staffId);

							} else {
								log.info("User is active => " + list.getStaffId());

								// Existing mail id will be used even user is active
								String email = userInfo.get("email").toString();
								username = list.getStaffId();
								LocationCode = list.getLocationCode();
								AdOU = list.getAdOU();

								if (EmployeeCategory.equalsIgnoreCase("Top Management")) {
									completeUserName = "CN=" + username + "," + "OU=" + LocationCode + ","
											+ "OU=VVIP,OU=MH" + "," + IDMConstants.returnFYBaseDN;
								} else if (AdOU.equalsIgnoreCase("MALAYSIA")) {
//									completeUserName = "CN=" + username + "," + "OU=" + LocationCode + ","
//											+ "OU=Malaysia,OU=MH" + "," + IDMConstants.returnBaseDN;
									completeUserName = "CN=" + username + "," + IDMConstants.returnFYBaseDN;
								} else if (AdOU.equalsIgnoreCase("INTLW")) {
									completeUserName = "CN=" + username + "," + "OU=" + LocationCode + ","
											+ "OU=INTLW,OU=MH" + "," + IDMConstants.returnFYBaseDN;
								} else {
									completeUserName = "CN=" + username + "," + "OU=" + LocationCode + ","
											+ "OU=INTLE,OU=MH" + "," + IDMConstants.returnFYBaseDN;
								}
								// Changing the OU details
								ctx.rename(userInfo.get("cn").toString(), completeUserName);
								ctx.close();
								ctx = new InitialLdapContext(env, null);
								userInfo = getUserInfoByDomainWithUser(ctx, baseDN, "mh\\" + staffId);
								log.info(userInfo.get("cn").toString());

								modifyUserDetails(ctx, userInfo.get("cn").toString(), list, email, managerInfo);
								fileReaderDAO.updateEmailToDB(email, staffId);

								log.info("else block - update mail ID in DB staffId " + staffId + " email " + email);
							}
						}

						else {

							log.info(" else block " + staffId);

							String email = null;
							String firstName = list.getFirstName();
							String lastName = list.getLastName();
							StringBuilder str = new StringBuilder(firstName.toLowerCase());
							StringBuilder str1 = new StringBuilder(lastName.toLowerCase());
							for (String names : excluded_NotFound_in_ad) {
								if (firstName.toLowerCase().contains(names.toLowerCase())) {

									int i = str.indexOf(names.toLowerCase());

									str.delete(i, i + names.length() + 1);
									String finalFirstName = camelCase(str);
									firstName = finalFirstName;
								}
								if (lastName.toLowerCase().contains(names.toLowerCase())) {
									int i = str1.indexOf(names.toLowerCase());

									str1.delete(i, i + names.length() + 1);
									String finalLastName = camelCase(str1);
									lastName = finalLastName;
									// System.out.println(finalLastName);
								} else {
									firstName = camelCase(str);
									lastName = camelCase(str1);
								}

							}

							// ADDED TO INTERCHANGE JAPANESE FIRST AND LAST NAMES
							if (locationCodes__NotFound_in_ad.contains(list.getLocationCode())) {

								String email1 = firstName.trim();
								email1 = email1.replaceAll("[\\s ']", "");

								// email1=email1.replace
								email1 = email1.toLowerCase();
								String email2 = lastName.trim();
								email2 = email2.replaceAll("[\\s ']", "");
								email2 = email2.toLowerCase();
							//	email = email2 + "." + email1 + "@malaysiaairlines.com";
								email = email2 + "." + email1 + "@fireflyz.com.my";
								log.info("Mail id=====" + email);

							} else {

								String email1 = firstName.trim();
								email1 = email1.replaceAll("[\\s ']", "");
								email1 = email1.toLowerCase();
								String email2 = lastName.trim();
								email2 = email2.replaceAll("[\\s ']", "");
								email2 = email2.toLowerCase();
							//	email = email1 + "." + email2 + "@malaysiaairlines.com";
								email = email2 + "." + email1 + "@fireflyz.com.my";
								log.info("Mail id=====" + email);

							}

							String finalMail = "";
							// Check whether email is already exists in AD
							// Added 27/09/18
							while (true) {
								ctx = new InitialLdapContext(env, null);
								Map<String, Object> userMail = new HashMap<String, Object>();
								userMail = checkEmailExistInAD(ctx, baseDN, "mh\\" + email);

								if (userMail.get("email") == null) {
									break;
								} else {
									if (userMail.get("email").toString().equalsIgnoreCase(email)) {
										String result = "";
										String regex = "(.)*(\\d)(.)*";
										Pattern pattern = Pattern.compile(regex);
										String fine = "";

										String[] parts = email.split("@");
										String part1 = parts[0]; // 004
										part1 = part1.replaceAll("\\s", "");
										log.info(part1);
										String part2 = parts[1]; // 034556
										log.info(part2);

										Matcher matcher = pattern.matcher(part1);
										boolean isMatched = matcher.matches();
										if (isMatched) {

											String[] num = part1.split("(?<=\\D)(?=\\d)");
											String num1 = num[0];

											String num2 = num[1];
											int foo = Integer.parseInt(num2);
											int fin = foo + 1;
											if (fin < 10) {
												fine = "0" + Integer.toString(fin);
											} else {
												fine = Integer.toString(fin);
											}
											result = num1 + fine;
											// log.info("num==" + result);
											finalMail = result + "@" + part2;
										} else {
											finalMail = part1 + "01" + "@" + part2;
										}
									}

									log.info("Final mail id=====" + finalMail);

								}
								email = finalMail;
							}
							// end
							username = list.getStaffId();
							String fullName = list.getFullName();
							StringBuilder str2 = new StringBuilder(fullName.toLowerCase());
							String userFullName = camelCase(str2);
							LocationCode = list.getLocationCode();
							AdOU = list.getAdOU();
							String CostCentre = list.getCostCenter();

							Attribute division;
							Attribute department;
							Attribute mobile;
							Attribute description;
							Attribute postalCode;
							Attribute manager;
							Attribute co;
							Attribute st;
							Attribute company;
							Attribute streetAddress;
							Attribute title;
							Attribute city;
							Attribute extensionAttribute4;
							Attribute extensionAttribute3;
							String cnValue = username;
							Attribute cn = new BasicAttribute("cn", cnValue);
							Attribute displayName = new BasicAttribute("displayName", userFullName);
							Attribute sAMAccountName = new BasicAttribute("sAMAccountName", username);
							Attribute principalName = new BasicAttribute("userPrincipalName", email);
							Attribute mail = new BasicAttribute("mail", email);
							Attribute givenName = new BasicAttribute("givenName", firstName);
							Attribute sn = new BasicAttribute("sn", lastName);
							// added
							// Attribute lastpswdSet = new BasicAttribute("pwdLastSet ", "0");
							if (list.getDivisionName().equalsIgnoreCase("")) {
								division = new BasicAttribute("division", "null");
							} else {
								division = new BasicAttribute("division", list.getDivisionName());
							}

							// log.info("manager info=-===="+result);
							if (managerInfo.get("cn") == null) {
								manager = new BasicAttribute("manager", "");
							} else {
								// manager =new BasicAttribute("manager",list.getSuperior());
								// doubt
								manager = new BasicAttribute("manager", managerInfo.get("cn").toString());
								container.put(manager);
							}
							// ENd

							if (list.getDepartmentName().equalsIgnoreCase("")) {
								department = new BasicAttribute("department", "null");
							} else {
								department = new BasicAttribute("department", list.getDepartmentName());
							}

							if (list.getLegalEntity().equalsIgnoreCase("")) {
								company = new BasicAttribute("company", "null");
							} else {
								company = new BasicAttribute("company", list.getLegalEntity());
							}

							if (list.getPhoneNO().equalsIgnoreCase("")) {
								mobile = new BasicAttribute("mobile", "null");
							} else {
								mobile = new BasicAttribute("mobile", list.getPhoneNO());
							}

							if (list.getStreetAddress().equalsIgnoreCase("")) {
								streetAddress = new BasicAttribute("streetAddress", "null");
							} else {
								streetAddress = new BasicAttribute("streetAddress", list.getStreetAddress());
							}
							if (list.getState().equalsIgnoreCase("")) {
								st = new BasicAttribute("st", "null");
							} else {
								st = new BasicAttribute("st", list.getState());
							}
							if (list.getPostalCode().equalsIgnoreCase("")) {
								postalCode = new BasicAttribute("postalCode", "null");
							} else {
								postalCode = new BasicAttribute("postalCode", list.getPostalCode());
							}

							if (list.getCountry().equalsIgnoreCase("")) {
								co = new BasicAttribute("co", "null");
							} else {
								co = new BasicAttribute("co", list.getCountry());
							}

							if (list.getDesignation().equalsIgnoreCase("")) {
								title = new BasicAttribute("title", "null");
							} else {
								title = new BasicAttribute("title", list.getDesignation());
							}
							if (list.getCity().equalsIgnoreCase("")) {
								city = new BasicAttribute("l", "null");
							} else {
								city = new BasicAttribute("l", list.getCity());
							}

							if (CostCentre.equalsIgnoreCase("")) {
								extensionAttribute4 = new BasicAttribute("extensionAttribute4", "null");
							} else {
								extensionAttribute4 = new BasicAttribute("extensionAttribute4", CostCentre);
							}

							// end
							if (list.getJobGrade().equalsIgnoreCase("")) {
								extensionAttribute3 = new BasicAttribute("extensionAttribute3", "null");
							} else {
								extensionAttribute3 = new BasicAttribute("extensionAttribute3", list.getJobGrade());
							}

							Attribute uid = new BasicAttribute("uid", username);
							Attribute physicalDeliveryOfficeName = new BasicAttribute("physicalDeliveryOfficeName",
									list.getLocationCode());

							if (EmployeeCategory.equalsIgnoreCase("Top Management")) {
								completeUserName = "CN=" + username + "," + "OU=" + LocationCode + "," + "OU=VVIP,OU=MH"
										+ "," + IDMConstants.returnFYBaseDN;
							} else if (AdOU.equalsIgnoreCase("MALAYSIA")) {
								completeUserName = "CN=" + username + "," + "OU=" + LocationCode + ","
										+ "OU=Malaysia,OU=MH" + "," + IDMConstants.returnFYBaseDN;
							} else if (AdOU.equalsIgnoreCase("INTLW")) {
								completeUserName = "CN=" + username + "," + "OU=" + LocationCode + ","
										+ "OU=INTLW,OU=MH" + "," + IDMConstants.returnFYBaseDN;
							} else {
								completeUserName = "CN=" + username + "," + "OU=" + LocationCode + ","
										+ "OU=INTLE,OU=MH" + "," + IDMConstants.returnFYBaseDN;
							}

							int UF_NORMAL_ACCOUNT = 0x0200;

							container.put(objClasses);
							container.put(sAMAccountName);
							container.put(principalName);
							container.put(cn);
							container.put(sn);
							container.put(givenName);
							container.put(uid);
							container.put(displayName);
							container.put(division);
							container.put(department);
							container.put(company);
							container.put(streetAddress);
							container.put(st);
							container.put(postalCode);
							container.put(city);
							container.put(co);
							container.put(mail);

							container.put(physicalDeliveryOfficeName);
							container.put(extensionAttribute4);
							container.put(extensionAttribute3);

							container.put(title);
							container.put(mobile);
							// added

							try {
								ctx = new InitialLdapContext(env, null);
								
								log.info(" completeUserName => "+completeUserName);
								log.info(" container => "+container);
								
								ctx.createSubcontext(completeUserName, container);

								ModificationItem[] mods = new ModificationItem[3];

								final int mid = username.length() / 2; // get the middle of the String

								String[] parts = { username.substring(0, mid), username.substring(mid) };

								String password = "Mab@" + parts[0] + "#" + parts[1];
								String newQuotedPassword = "\"" + password + "\"";
								byte[] newUnicodePassword = newQuotedPassword.getBytes("UTF-16LE");

								mods[0] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE,
										new BasicAttribute("unicodePwd", newUnicodePassword));

								mods[2] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE,
										new BasicAttribute("userAccountControl", Integer.toString(UF_NORMAL_ACCOUNT)));

								mods[1] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE,
										new BasicAttribute("pwdLastSet", Integer.toString(0)));

								ctx.modifyAttributes(completeUserName, mods);

								// RAJI END

								fileReaderDAO.updateEmailToDB(email, staffId);

								log.info("update mail ID in DB staffId " + staffId + " email " + email);

								log.info("Set password & updated userccountControl");
							} catch (Exception e)

							{
								log.error(e);
								Calendar cal = Calendar.getInstance();
								IDMAlert.sendMailToSupport(e.getMessage(),
										"Failed : AD Creation for on boarding staff " + staffId + " <br><br>"+container+"  <br><br>"+ list, cal.getTime(),
										staffId);
							}
						}
					}
					catch (NamingException e) {

						log.error(e);

						Calendar cal = Calendar.getInstance();
						IDMAlert.sendMailToSupport(e.getMessage(),
								"Failed : AD Creation for on boarding staff " + staffId+ " <br><br>"+container +"  <br><br>"+ list, cal.getTime(), staffId);
					} catch (Exception e) {
						log.error(e.getMessage());
						Calendar cal = Calendar.getInstance();
						IDMAlert.sendMailToSupport(e.getMessage(),
								"Failed : AD Creation for on boarding staff " + staffId+ " <br><br>"+container+"  <br><br>"+ list, cal.getTime(), staffId);
						log.error("Error occured while adding user in Active Directory for:" + staffId);

					} finally {
						try {
							ctx.close();
						} catch (NamingException e) {
							// TODO Auto-generated catch block
							log.error(e);
						}
					}
				}

			} else {

				log.info("No New hire data available in DB for AD Updation");
			}
		} catch (Exception e) {
			log.error(e.getMessage());
			Calendar cal = Calendar.getInstance();
			log.error("Error occured while adding user in Active Directory" + e);
		}
		
		log.info("END: Inside createUser***");
	}

	
	
	
//Modifying user details for movement update in AD
	public void modifyUser(String host) {

		log.info("START: Inside Movement Update to AD");

		List<ADUpdateVO> movementAdDataList = new ArrayList<ADUpdateVO>();
		List<ADUpdateVO> errorList = new ArrayList<>();
		List<ADUpdateVO> successList = new ArrayList<>();

		String username = "";
		String completeUserName = "";
		String LocationCode = "";
		String AdOU = "";
//Getting movement data 
		try {
			movementAdDataList = userADDAO.getMovementData();
			ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
			String certPath = classLoader.getResource(bundle.getString("idvs.certPath")).getPath();
			log.info("CertPath" + certPath);
			System.setProperty("javax.net.ssl.trustStore", certPath);
			System.setProperty("javax.net.ssl.trustStorePassword", "changeit");
			System.setProperty("javax.net.debug", "all");
			Hashtable<String, String> env = new Hashtable<String, String>();
			env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
			env.put(Context.PROVIDER_URL, "ldap://" + host + ":636");
			env.put(Context.SECURITY_AUTHENTICATION, "simple");
			env.put(Context.SECURITY_PRINCIPAL, IDMConstants.returnLDAPUserName);
			env.put(Context.SECURITY_CREDENTIALS, IDMConstants.returnLDAPPassword);

			env.put(Context.SECURITY_PROTOCOL, "ssl");
			
			LdapContext ctx = null;

			// Create the objectclass to add
			Attribute objClasses = new BasicAttribute("objectClass");
			objClasses.add("top");
			objClasses.add("person");
			objClasses.add("organizationalPerson");
			objClasses.add("user");

			// passing the movement values in AD
			if (movementAdDataList.size() > 0) {
				for (ADUpdateVO list : movementAdDataList) {

					String staffId = list.getStaffId();

					try {

						// added
						username = list.getStaffId();
						LocationCode = list.getLocationCode();
						AdOU = list.getAdOU();
						String EmployeeCategory = list.getEmployeeCategory();
						Map<String, Object> userInfo = new HashMap<String, Object>();
						String baseDN = IDMConstants.returnBaseDN;
						ctx = new InitialLdapContext(env, null);

						userInfo = getUserInfoByDomainWithUser(ctx, baseDN, "mh\\" + staffId);

						if (EmployeeCategory.equalsIgnoreCase("Top Management")) {
							completeUserName = "CN=" + username + "," + "OU=" + LocationCode + "," + "OU=VVIP,OU=MH"
									+ "," + IDMConstants.returnBaseDN;
						} else if (AdOU.equalsIgnoreCase("MALAYSIA")) {
							completeUserName = "CN=" + username + "," + "OU=" + LocationCode + "," + "OU=Malaysia,OU=MH"
									+ "," + IDMConstants.returnBaseDN;
						} else if (AdOU.equalsIgnoreCase("INTLW")) {
							completeUserName = "CN=" + username + "," + "OU=" + LocationCode + "," + "OU=INTLW,OU=MH"
									+ "," + IDMConstants.returnBaseDN;
						} else {
							completeUserName = "CN=" + username + "," + "OU=" + LocationCode + "," + "OU=INTLE,OU=MH"
									+ "," + IDMConstants.returnBaseDN;
						}
						// Changing the OU details

						if (userInfo.get("cn") != null) {
							try {

								try {
								ctx.rename(userInfo.get("cn").toString(), completeUserName);
								ctx.close();
								}catch(Exception ex) {
									try {
										ctx.rename(userInfo.get("cn").toString(), userInfo.get("cn").toString());
										ctx.close();	
									}catch(Exception e1) {
										throw e1;
									}
								}
							} catch (Exception e) {
								log.error(userInfo.get("cn").toString() + "  <=> Latest: => " + completeUserName);
								log.error(e);
								userInfo = new HashMap<String, Object>();
								ctx.close();
								errorList.add(list);
								continue;
							}
						} else {
							log.info(userInfo.get("cn") + "  <=> Latest: => " + completeUserName);
							userInfo = new HashMap<String, Object>();
							errorList.add(list);
							ctx.close();
							continue;
						}

						// end

						Map<String, Object> managerInfo = new HashMap<String, Object>();

						ctx = getLdapContext(IDMConstants.returnLDAPServer, IDMConstants.returnLDAPUserName,
								IDMConstants.returnLDAPPassword);
						userInfo = getUserInfoByDomainWithUser(ctx, baseDN, "mh\\" + staffId);

						String manager = list.getSuperior();
						managerInfo = getUserInfoByDomainWithManager(ctx, baseDN, "mh\\" + manager);

						if (userInfo.get("cn") != null) {
							modifyUserDetails(ctx, userInfo.get("cn").toString(), list,
									userInfo.get("email").toString(), managerInfo);

							log.info("User enabled: " + staffId);

							fileReaderDAO.updateStatusToDB(staffId);

							log.info("Updated in DB " + staffId);
							successList.add(list);
						} else {
							log.error("User Not Found " + staffId);
							errorList.add(list);
						}
					} catch (NamingException e) {

						log.error(e);
						errorList.add(list);
						log.error(e.getMessage());
						Calendar cal = Calendar.getInstance();

						log.error("Error occured while modifying user in Active Directory for:" + staffId);
					} catch (Exception e) {
						errorList.add(list);
						log.error(e.getMessage());
						Calendar cal = Calendar.getInstance();

						log.error("Error occured while modifying user in Active Directory for:" + staffId);
						continue;
						// throw e;
					} finally {
						try {
							ctx.close();
						} catch (NamingException e) {
							// TODO Auto-generated catch block
							errorList.add(list);
							log.error("ERROR staff =>" + list + " ERROR " + e);
						}
					}
				}

			} else {
				log.info("No Movement data available in DB for AD Updation");
			}

		} catch (Exception e) {
			log.error(e.getMessage());
			Calendar cal = Calendar.getInstance();
			log.error("Error occured while modifying user in Active Directory" + e);
		}

		log.info("modifyUser - successList=>  " + successList);
		log.info("modifyUser - errorList=>  " + errorList);
		
		log.info("END: Inside Movement Update to AD");
	}

	// Modifying user details for movement update in FY AD
	// @mujeeba.khan
	public void modifyFYUser(String host) {

		log.info("START: Inside Movement Update to AD");

		List<ADUpdateVO> movementAdDataList = new ArrayList<ADUpdateVO>();
		List<ADUpdateVO> errorList = new ArrayList<>();
		List<ADUpdateVO> successList = new ArrayList<>();

		String username = "";
		String completeUserName = "";
		String LocationCode = "";
		String AdOU = "";
//Getting movement data 
		try {
			movementAdDataList = userADDAO.getFYMovementData();
			
			Hashtable<String, String> env = new Hashtable<String, String>();
			env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
			env.put(Context.PROVIDER_URL, "ldap://" + host + ":389");
			env.put(Context.SECURITY_AUTHENTICATION, "simple");
			env.put(Context.SECURITY_PRINCIPAL, IDMConstants.returnFYLDAPUserName);
			env.put(Context.SECURITY_CREDENTIALS, IDMConstants.returnFYLDAPPassword);
			
			LdapContext ctx = null;

			// Create the objectclass to add
			Attribute objClasses = new BasicAttribute("objectClass");
			objClasses.add("top");
			objClasses.add("person");
			objClasses.add("organizationalPerson");
			objClasses.add("user");

			// passing the movement values in AD
			if (movementAdDataList.size() > 0) {
				for (ADUpdateVO list : movementAdDataList) {

					String staffId = list.getStaffId();

					try {

						// added
						username = list.getStaffId();
						LocationCode = list.getLocationCode();
						AdOU = list.getAdOU();
						String EmployeeCategory = list.getEmployeeCategory();
						Map<String, Object> userInfo = new HashMap<String, Object>();
						String baseDN = IDMConstants.returnFYBaseDN;
						ctx = new InitialLdapContext(env, null);

						userInfo = getUserInfoByDomainWithUser(ctx, baseDN, "mh\\" + staffId);

						if (EmployeeCategory.equalsIgnoreCase("Top Management")) {
							completeUserName = "CN=" + username + "," + "OU=" + LocationCode + "," + "OU=VVIP,OU=MH"
									+ "," + IDMConstants.returnFYBaseDN;
						} else if (AdOU.equalsIgnoreCase("MALAYSIA")) {
							completeUserName = "CN=" + username + "," + "OU=" + LocationCode + "," + "OU=Malaysia,OU=MH"
									+ "," + IDMConstants.returnFYBaseDN;
						} else if (AdOU.equalsIgnoreCase("INTLW")) {
							completeUserName = "CN=" + username + "," + "OU=" + LocationCode + "," + "OU=INTLW,OU=MH"
									+ "," + IDMConstants.returnFYBaseDN;
						} else {
							completeUserName = "CN=" + username + "," + "OU=" + LocationCode + "," + "OU=INTLE,OU=MH"
									+ "," + IDMConstants.returnFYBaseDN;
						}
						// Changing the OU details

						if (userInfo.get("cn") != null) {
							try {

								try {
								ctx.rename(userInfo.get("cn").toString(), completeUserName);
								ctx.close();
								}catch(Exception ex) {
									try {
										ctx.rename(userInfo.get("cn").toString(), userInfo.get("cn").toString());
										ctx.close();	
									}catch(Exception e1) {
										throw e1;
									}
								}
							} catch (Exception e) {
								log.error(userInfo.get("cn").toString() + "  <=> Latest: => " + completeUserName);
								log.error(e);
								userInfo = new HashMap<String, Object>();
								ctx.close();
								errorList.add(list);
								continue;
							}
						} else {
							log.info(userInfo.get("cn") + "  <=> Latest: => " + completeUserName);
							userInfo = new HashMap<String, Object>();
							errorList.add(list);
							ctx.close();
							continue;
						}

						// end

						Map<String, Object> managerInfo = new HashMap<String, Object>();

						ctx = getLdapContext(IDMConstants.returnFYLDAPServer, IDMConstants.returnFYLDAPUserName,
								IDMConstants.returnFYLDAPPassword);
						userInfo = getUserInfoByDomainWithUser(ctx, baseDN, "mh\\" + staffId);

						String manager = list.getSuperior();
						managerInfo = getUserInfoByDomainWithManager(ctx, baseDN, "mh\\" + manager);

						if (userInfo.get("cn") != null) {
							modifyUserDetails(ctx, userInfo.get("cn").toString(), list,
									userInfo.get("email").toString(), managerInfo);

							log.info("User enabled: " + staffId);

							fileReaderDAO.updateStatusToDB(staffId);

							log.info("Updated in DB " + staffId);
							successList.add(list);
						} else {
							log.error("User Not Found " + staffId);
							errorList.add(list);
						}
					} catch (NamingException e) {

						log.error(e);
						errorList.add(list);
						log.error(e.getMessage());
						Calendar cal = Calendar.getInstance();

						log.error("Error occured while modifying user in Active Directory for:" + staffId);
					} catch (Exception e) {
						errorList.add(list);
						log.error(e.getMessage());
						Calendar cal = Calendar.getInstance();

						log.error("Error occured while modifying user in Active Directory for:" + staffId);
						continue;
						// throw e;
					} finally {
						try {
							ctx.close();
						} catch (NamingException e) {
							// TODO Auto-generated catch block
							errorList.add(list);
							log.error("ERROR staff =>" + list + " ERROR " + e);
						}
					}
				}

			} else {
				log.info("No Movement data available in DB for AD Updation");
			}

		} catch (Exception e) {
			log.error(e.getMessage());
			Calendar cal = Calendar.getInstance();
			log.error("Error occured while modifying user in Active Directory" + e);
		}

		log.info("modifyUser - successList=>  " + successList);
		log.info("modifyUser - errorList=>  " + errorList);
		
		log.info("END: Inside Movement Update to AD");
	}

	
	
	public static void modifyUserDetails(LdapContext ctx, String id, ADUpdateVO list, String email,
			Map<String, Object> managerInfo2) throws NamingException {
		String baseDN = IDMConstants.returnBaseDN;

		// Checking whether superior is null or not
		log.info(managerInfo2.get("cn"));
		if (managerInfo2.get("cn") == null) {

			ModificationItem[] mods = new ModificationItem[15];

			log.info("Inside setting the values for Movement data update");

			if (nullCheckEmptyCheck(list.getDepartmentName())) {
				list.setDepartmentName("null");

				mods[0] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE,
						new BasicAttribute("department", list.getDepartmentName()));
			} else {
				mods[0] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE,
						new BasicAttribute("department", list.getDepartmentName()));
			}

			if (nullCheckEmptyCheck(list.getDivisionName())) {
				list.setDivisionName("null");

				mods[1] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE,
						new BasicAttribute("division", list.getDivisionName()));
			} else {
				mods[1] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE,
						new BasicAttribute("division", list.getDivisionName()));
			}

			if (nullCheckEmptyCheck(list.getCostCenter())) {
				list.setCostCenter("null");

				mods[2] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE,
						new BasicAttribute("extensionAttribute4", list.getCostCenter()));
			} else {
				mods[2] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE,
						new BasicAttribute("extensionAttribute4", list.getCostCenter()));
			}

			// ADDED to sync in profile updates

			if (nullCheckEmptyCheck(list.getPostalCode())) {
				list.setPostalCode("null");

				mods[3] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE,
						new BasicAttribute("postalCode", list.getPostalCode()));
			} else {
				mods[3] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE,
						new BasicAttribute("postalCode", list.getPostalCode()));
			}
			mods[4] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE,
					new BasicAttribute("userPrincipalName", email));

			if (nullCheckEmptyCheck(list.getLegalEntity())) {
				list.setLegalEntity("null");
				mods[5] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE,
						new BasicAttribute("company", list.getLegalEntity()));
			} else {
				mods[5] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE,
						new BasicAttribute("company", list.getLegalEntity()));
			}

			if (nullCheckEmptyCheck(list.getStreetAddress())) {
				list.setStreetAddress("null");
				mods[6] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE,
						new BasicAttribute("streetAddress", list.getStreetAddress()));
			} else {
				mods[6] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE,
						new BasicAttribute("streetAddress", list.getStreetAddress()));
			}

			if (nullCheckEmptyCheck(list.getState())) {
				list.setState("null");
				mods[7] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE, new BasicAttribute("st", list.getState()));
			} else {
				mods[7] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE, new BasicAttribute("st", list.getState()));
			}

			if (nullCheckEmptyCheck(list.getCountry())) {
				list.setCountry("null");
				mods[8] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE,
						new BasicAttribute("co", list.getCountry()));
			} else {
				mods[8] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE,
						new BasicAttribute("co", list.getCountry()));
			}
			mods[9] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE, new BasicAttribute("mail", email));

			if (nullCheckEmptyCheck(list.getDesignation())) {
				list.setDesignation("null");
				mods[10] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE,
						new BasicAttribute("title", list.getDesignation()));
			} else {
				mods[10] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE,
						new BasicAttribute("title", list.getDesignation()));
			}
			if (nullCheckEmptyCheck(list.getCity())) {
				list.setCity("null");
				mods[11] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE, new BasicAttribute("l", list.getCity()));
			} else {
				mods[11] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE, new BasicAttribute("l", list.getCity()));
			}

			if (nullCheckEmptyCheck(list.getPhoneNO())) {
				list.setPhoneNO("null");
				mods[12] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE,
						new BasicAttribute("mobile", list.getPhoneNO()));
			} else {
				mods[12] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE,
						new BasicAttribute("mobile", list.getPhoneNO()));
			}

			mods[13] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE,
					new BasicAttribute("physicalDeliveryOfficeName", list.getLocationCode()));

			if (nullCheckEmptyCheck(list.getJobGrade())) {
				list.setJobGrade("null");
				mods[14] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE,
						new BasicAttribute("extensionAttribute3", list.getJobGrade()));
			} else {
				mods[14] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE,
						new BasicAttribute("extensionAttribute3", list.getJobGrade()));
			}

			ctx.modifyAttributes(id, mods);

		} else {
			ModificationItem[] mods = new ModificationItem[16];

			log.info("Inside setting the values for Movement data update");

			if (nullCheckEmptyCheck(list.getDepartmentName())) {

				list.setDepartmentName("null");

				mods[0] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE,
						new BasicAttribute("department", list.getDepartmentName()));
			} else {
				mods[0] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE,
						new BasicAttribute("department", list.getDepartmentName()));
			}

			if (nullCheckEmptyCheck(list.getDivisionName())) {
				list.setDivisionName("null");

				mods[1] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE,
						new BasicAttribute("division", list.getDivisionName()));
			} else {
				mods[1] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE,
						new BasicAttribute("division", list.getDivisionName()));
			}
			// Added for sync btwn pulsera and idvs ad Raji
			if (nullCheckEmptyCheck(list.getCostCenter())) {
				list.setCostCenter("null");

				mods[2] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE,
						new BasicAttribute("extensionAttribute4", list.getCostCenter()));
			} else {
				mods[2] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE,
						new BasicAttribute("extensionAttribute4", list.getCostCenter()));
			}

			Map<String, Object> managerInfo = new HashMap<String, Object>();
			managerInfo = getUserInfoByDomainWithManager(ctx, baseDN, "mh\\" + list.getSuperior());

			mods[3] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE,
					new BasicAttribute("manager", managerInfo.get("cn").toString()));
			// ADDED to sync in profile updates

			if (nullCheckEmptyCheck(list.getPostalCode())) {
				list.setPostalCode("null");

				mods[4] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE,
						new BasicAttribute("postalCode", list.getPostalCode()));
			} else {
				mods[4] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE,
						new BasicAttribute("postalCode", list.getPostalCode()));
			}
			mods[5] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE,
					new BasicAttribute("userPrincipalName", email));

			if (nullCheckEmptyCheck(list.getLegalEntity())) {
				list.setLegalEntity("null");
				mods[6] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE,
						new BasicAttribute("company", list.getLegalEntity()));
			} else {
				mods[6] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE,
						new BasicAttribute("company", list.getLegalEntity()));
			}

			if (nullCheckEmptyCheck(list.getStreetAddress())) {
				list.setStreetAddress("null");
				mods[7] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE,
						new BasicAttribute("streetAddress", list.getStreetAddress()));
			} else {
				mods[7] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE,
						new BasicAttribute("streetAddress", list.getStreetAddress()));
			}

			if (nullCheckEmptyCheck(list.getState())) {
				list.setState("null");
				mods[8] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE, new BasicAttribute("st", list.getState()));
			} else {
				mods[8] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE, new BasicAttribute("st", list.getState()));
			}

			if (nullCheckEmptyCheck(list.getCountry())) {
				list.setCountry("null");
				mods[9] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE,
						new BasicAttribute("co", list.getCountry()));
			} else {
				mods[9] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE,
						new BasicAttribute("co", list.getCountry()));
			}

			mods[10] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE, new BasicAttribute("mail", email));

			if (nullCheckEmptyCheck(list.getDesignation())) {
				list.setDesignation("null");
				mods[11] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE,
						new BasicAttribute("title", list.getDesignation()));
			} else {
				mods[11] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE,
						new BasicAttribute("title", list.getDesignation()));
			}

			if (nullCheckEmptyCheck(list.getCity())) {
				list.setCity("null");
				mods[12] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE, new BasicAttribute("l", list.getCity()));
			} else {
				mods[12] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE, new BasicAttribute("l", list.getCity()));
			}
			if (nullCheckEmptyCheck(list.getPhoneNO())) {
				list.setPhoneNO("null");
				mods[13] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE,
						new BasicAttribute("mobile", list.getPhoneNO()));
			} else {
				mods[13] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE,
						new BasicAttribute("mobile", list.getPhoneNO()));
			}

			mods[14] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE,
					new BasicAttribute("physicalDeliveryOfficeName", list.getLocationCode()));
			if (nullCheckEmptyCheck(list.getJobGrade())) {
				list.setJobGrade("null");
				mods[15] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE,
						new BasicAttribute("extensionAttribute3", list.getJobGrade()));
			} else {
				mods[15] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE,
						new BasicAttribute("extensionAttribute3", list.getJobGrade()));
			}
			ctx.modifyAttributes(id, mods);
		}

	}

//Nikhil
	public static Map<String, Object> getUserInfoByDomainWithUser(LdapContext ctx, String searchBase,
			String domainWithUser) throws NamingException {
		String userName = domainWithUser.substring(domainWithUser.indexOf('\\') + 1);
		try {
			NamingEnumeration<SearchResult> userDataBysAMAccountName = getUserDataBysAMAccountName(ctx, searchBase,
					userName);

			return getUserInfoFromSearchResults(userDataBysAMAccountName);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

//end

//Raji

	public static Map<String, Object> getUserInfoByDomainWithManager(LdapContext ctx, String searchBase,
			String domainWithManager) throws NamingException {
		String userName = domainWithManager.substring(domainWithManager.indexOf('\\') + 1);
		try {
			NamingEnumeration<SearchResult> userDataBysAMAccountName = getUserDataBysAMAccountName(ctx, searchBase,
					userName);

			return getManagerInfoFromSearchResults(userDataBysAMAccountName);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
//Check mail id exists in AD

	public static Map<String, Object> checkEmailExistInAD(LdapContext ctx, String searchBase, String mail)
			throws NamingException {
		String mailId = mail.substring(mail.indexOf('\\') + 1);
		try {
			NamingEnumeration<SearchResult> userDataByMail = getUserDataByMail(ctx, searchBase, mailId);

			return getUserInfoFromSearchResults(userDataByMail);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private static NamingEnumeration<SearchResult> getUserDataByMail(LdapContext ctx, String searchBase, String mail)
			throws Exception {

		String filter = "(&(mail=" + mail + "))";
		SearchControls searchCtls = new SearchControls();

		String[] resultAttributes = { "cn", "mail", "distinguishedName", "userAccountControl", "displayName",
				"lastLogon", "description", "otherMobile", "mobile", "pwdLastSet" };
		searchCtls.setReturningAttributes(resultAttributes);

		searchCtls.setSearchScope(SearchControls.SUBTREE_SCOPE);
		NamingEnumeration<SearchResult> answer = null;
		try {
			answer = ctx.search(searchBase, filter, searchCtls);

		} catch (Exception e) {
			log.info(e);
			throw e;
		}

		return answer;
	}

	/**
	 * Authenticate.
	 *
	 * @param host     the host
	 * @param username the username
	 * @param password the password
	 * @return true if passed the authenticate, or else false.
	 * @throws NamingException the naming exception
	 */
	public static LdapContext authenticate(String host, String username, String password) throws NamingException {
		LdapContext ctx = getLdapContext(host, username, password);
		return ctx;
	}

	/**
	 * Gets the ldap context.
	 *
	 * @param host     the host
	 * @param username the username
	 * @param password the password
	 * @return the ldap context
	 * @throws NamingException the naming exception
	 */
	public static LdapContext getLdapContext(String host, String username, String password) throws NamingException {

		Hashtable<String, String> env = new Hashtable<String, String>();
		env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
		env.put(Context.PROVIDER_URL, "ldap://" + host);
		env.put(Context.SECURITY_AUTHENTICATION, "simple");
		env.put(Context.SECURITY_PRINCIPAL, username);

		env.put(Context.SECURITY_CREDENTIALS, password);
		env.put(Context.REFERRAL, "follow");
		env.put("com.sun.jndi.ldap.connect.timeout", "1800000");
		env.put("java.naming.ldap.attributes.binary", "tokenGroups objectSid objectGUID");
		LdapContext ctx = null;
		try {
			ctx = new InitialLdapContext(env, null);
		} catch (AuthenticationException e) {
			throw e;

		} catch (CommunicationException e) {
			log.info(e);
			throw e;

		} catch (Exception e) {
			log.info(e);
			throw e;
		}
		return ctx;
	}

	/**
	 * Gets the user info by domain with user.
	 *
	 * @param ctx            the ctx
	 * @param domainWithUser the domain with user
	 * @return the user info by domain with user
	 * @throws NamingException the naming exception
	 */
	public static Map<String, Object> getUserInfoByDomainWithUser(LdapContext ctx, String domainWithUser)
			throws NamingException {

		String searchBase = "DC=MH,DC=CORP,DC=NET";

		String userName = domainWithUser.substring(domainWithUser.indexOf('\\') + 1);
		try {
			NamingEnumeration<SearchResult> userDataBysAMAccountName = getUserDataBysAMAccountName(ctx, searchBase,
					userName);
			return getUserInfoFromSearchResults(userDataBysAMAccountName);
		} catch (Exception e) {
			log.info(e);
			throw new RuntimeException(e);
		}
	}

	/**
	 * Gets the user data bys am account name.
	 *
	 * @param ctx        the ctx
	 * @param searchBase the search base
	 * @param username   the username
	 * @return the user data bys am account name
	 * @throws Exception the exception
	 */
	private static NamingEnumeration<SearchResult> getUserDataBysAMAccountName(LdapContext ctx, String searchBase,
			String username) throws Exception {

		String filter = "(&(&(objectClass=person)	(objectCategory=user))(sAMAccountName=" + username + "))";
		SearchControls searchCtls = new SearchControls();
		if (username.contains("ext")) {
			String[] resultAttributes = { "cn", "mail", "distinguishedName", "userAccountControl", "displayName",
					"lastLogon", "description", "otherMobile", "mobile", "pwdLastSet" };
			searchCtls.setReturningAttributes(resultAttributes);

		} else {
			String[] resultAttributes = { "cn", "mail", "distinguishedName", "userAccountControl", "displayName",
					"lastLogon", "description", "pwdLastSet" };
			searchCtls.setReturningAttributes(resultAttributes);

		}
		searchCtls.setSearchScope(SearchControls.SUBTREE_SCOPE);
		NamingEnumeration<SearchResult> answer = null;
		try {
			answer = ctx.search(searchBase, filter, searchCtls);
		} catch (Exception e) {
			log.info(e);
			throw e;
		}

		return answer;
	}

	/**
	 * Gets the user info from search results.
	 *
	 * @param userData the user data
	 * @return the user info from search results
	 * @throws Exception the exception
	 */
	private static Map<String, Object> getUserInfoFromSearchResults(NamingEnumeration<SearchResult> userData)
			throws Exception {
		try {
			String displayname = null;
			String email = null;
			String mobile = null;
			String pwdLastSet = null;
			String cn = null;
			String userAccountControl = null;
			// getting only the first result if we have more than one
			if (userData.hasMoreElements()) {
				SearchResult sr = userData.nextElement();
				Attributes attributes = sr.getAttributes();
				if (attributes.get(AD_ATTR_NAME_DISPLAY_NAME) != null) {
					displayname = attributes.get(AD_ATTR_NAME_DISPLAY_NAME).get().toString();
				}
				if (attributes.get(AD_ATTR_NAME_USER_EMAIL) != null) {
					email = attributes.get(AD_ATTR_NAME_USER_EMAIL).get().toString();
				}
				if (attributes.get(AD_ATTR_NAME_USER_MOBILE) != null) {
					mobile = attributes.get(AD_ATTR_NAME_USER_MOBILE).get().toString();
				}
				if (attributes.get(AD_ATTR_NAME_USER_PSWDSET) != null) {
					pwdLastSet = attributes.get(AD_ATTR_NAME_USER_PSWDSET).get().toString();
				}
				if (attributes.get(AD_ATTR_NAME_USER_CN) != null) {
					cn = attributes.get(AD_ATTR_NAME_USER_CN).get().toString();
				}
				if (attributes.get(AD_ATTR_NAME_USER_ACCOUNTCONTROL) != null) {
					userAccountControl = attributes.get(AD_ATTR_NAME_USER_ACCOUNTCONTROL).get().toString();
				}
				log.info("Attributes::" + attributes.toString());
				log.info("NAME::" + displayname + "\t EMAIL ::" + email);
			}
			userInfo.put("StaffName", displayname);
			userInfo.put("email", email);
			userInfo.put("mobile", mobile);
			userInfo.put("pwdLastSet", pwdLastSet);
			userInfo.put("cn", cn);
			log.info("CN Value---" + userInfo.get("cn"));
			log.info("CN Value---> " + userInfo.get("cn"));
			userInfo.put("userAccountControl", userAccountControl);
			return userInfo;
		} catch (Exception e) {
			log.info(e);
			throw e;
		}
	}

	// RAji

	private static Map<String, Object> getManagerInfoFromSearchResults(NamingEnumeration<SearchResult> userData)
			throws Exception {
		try {
			String displayname = null;
			String email = null;
			String mobile = null;
			String pwdLastSet = null;
			String cn = null;
			String userAccountControl = null;
			// getting only the first result if we have more than one
			if (userData.hasMoreElements()) {
				SearchResult sr = userData.nextElement();
				Attributes attributes = sr.getAttributes();
				if (attributes.get(AD_ATTR_NAME_DISPLAY_NAME) != null) {
					displayname = attributes.get(AD_ATTR_NAME_DISPLAY_NAME).get().toString();
				}
				if (attributes.get(AD_ATTR_NAME_USER_EMAIL) != null) {
					email = attributes.get(AD_ATTR_NAME_USER_EMAIL).get().toString();
				}
				if (attributes.get(AD_ATTR_NAME_USER_MOBILE) != null) {
					mobile = attributes.get(AD_ATTR_NAME_USER_MOBILE).get().toString();
				}
				if (attributes.get(AD_ATTR_NAME_USER_PSWDSET) != null) {
					pwdLastSet = attributes.get(AD_ATTR_NAME_USER_PSWDSET).get().toString();
				}
				if (attributes.get(AD_ATTR_NAME_USER_CN) != null) {
					cn = attributes.get(AD_ATTR_NAME_USER_CN).get().toString();
				}
				if (attributes.get(AD_ATTR_NAME_USER_ACCOUNTCONTROL) != null) {
					userAccountControl = attributes.get(AD_ATTR_NAME_USER_ACCOUNTCONTROL).get().toString();
				}
				log.info("Attributes::" + attributes.toString());
				log.info("NAME::" + displayname + "\t EMAIL ::" + email);
			}
			managerInfo.put("StaffName", displayname);
			managerInfo.put("email", email);
			managerInfo.put("mobile", mobile);
			managerInfo.put("pwdLastSet", pwdLastSet);
			managerInfo.put("cn", cn);
			log.info("CN Value" + managerInfo.get("cn"));
			managerInfo.put("userAccountControl", userAccountControl);
			return managerInfo;
		} catch (Exception e) {
			log.info(e);
			throw e;
		}
	}

	private static String camelCase(StringBuilder str) {
		// TODO Auto-generated method stub

		StringBuilder builder = new StringBuilder(str);
		// Flag to keep track if last visited character is a
		// white space or not
		boolean isLastSpace = true;

		// Iterate String from beginning to end.
		for (int i = 0; i < builder.length(); i++) {
			char ch = builder.charAt(i);

			if (isLastSpace && ch >= 'a' && ch <= 'z') {
				// Character need to be converted to uppercase
				builder.setCharAt(i, (char) (ch + ('A' - 'a')));
				isLastSpace = false;
			} else if (ch != ' ')
				isLastSpace = false;
			else
				isLastSpace = true;
		}

		return builder.toString();
	}

	private static boolean nullCheckEmptyCheck(String param) {
		if (param == null || "".equals(param)) {
			return true;
		} else {
			return false;
		}
	}
}
