package com.mas.idm.batch;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.mail.Address;
import javax.mail.Authenticator;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMultipart;

import org.apache.log4j.MDC;
import org.springframework.dao.DataAccessException;

import com.mas.idm.bean.ComplianceVO;
import com.mas.idm.bean.NonComplianceVo;
import com.mas.idm.bean.ReportsVo;
import com.mas.idm.bean.StaffVo;
import com.mas.idm.bean.SystemOwnerVO;
import com.mas.idm.dao.PulseraTaskDAO;
import com.mas.idm.dao.SrasTasksDAO;
import com.mas.idm.exception.IDMDBException;
import com.mas.idm.exception.IDMException;
import com.mas.idm.exception.IDMPulseraException;
import com.mas.idm.util.IDMAlert;
import com.mas.idm.util.IDMConstantEnhanced;
import com.mas.idm.util.IDMConstants;
import com.mas.idm.util.IDMUtil;
import com.mas.idm.util.LDAPUtil;
import org.apache.logging.log4j.LogManager;import org.apache.logging.log4j.Logger;
import com.mas.idm.util.QuarterlyReportGenerator;
import com.mas.idm.util.VPNUtil;
import com.sun.mail.smtp.SMTPMessage;

import freemarker.template.TemplateException;

public class SrasTask {

	private static final Logger log = LogManager.getLogger(SrasTask.class);
	private SrasTasksDAO srasdao;
	private PulseraTaskDAO pulseradao;
	private StaffVo staffobj;
	private ArrayList<StaffVo> separationProcessingList;
	private ArrayList<StaffVo> separationList;
	private ArrayList<StaffVo> movementList;
	private HashMap<String, ArrayList<StaffVo>> separationHashMap;
	private HashMap<String, ArrayList<StaffVo>> globalSeparationMapper;
	private HashMap<String, ArrayList<StaffVo>> movementMailMapper;
	private HashMap<String, SystemOwnerVO> appSOMap;
	private IDMUtil util;
	private ArrayList<StaffVo> staffsList;
	private ArrayList<StaffVo> updationProcessingList;
	private HashMap<String, ArrayList<StaffVo>> reminderHM;
	private int rptIndicator;

	public int getRptIndicator() {
		return rptIndicator;
	}

	public void setRptIndicator(int rptIndicator) {
		this.rptIndicator = rptIndicator;
	}

	public IDMUtil getUtil() {
		return util;
	}

	public void setUtil(IDMUtil util) {
		this.util = util;
	}

	public StaffVo getStaffobj() {
		return staffobj;
	}

	public void setStaffobj(StaffVo staffobj) {
		this.staffobj = staffobj;
	}

	public PulseraTaskDAO getPulseradao() {
		return pulseradao;
	}

	public void setPulseradao(PulseraTaskDAO pulseradao) {
		this.pulseradao = pulseradao;
	}

	public SrasTasksDAO getSrasdao() {
		return srasdao;
	}

	public void setSrasdao(SrasTasksDAO srasdao) {
		this.srasdao = srasdao;
	}

	/**
	 * This method is used to trigger the separation process of the staffs whose
	 * separation date is <= current system date
	 */

	public void startSeparationProcess() {

		try {
			boolean status = srasdao.getJobConfigStatus(IDMConstantEnhanced.JOB_CONFIG_SEPARATION_PROCESS);
			if (status) {
				MDC.put("LOGID", "IDVS System");
				/**
				 * Update the separation table for those staffs who have been deactivated from
				 * their respective application by system owners
				 */
				log.debug("Fetching processing list for closed items");

				updationProcessingList = pulseradao.getUpdationList();

				log.debug("After fetching processing list for closed items");
				separationList = new ArrayList<>();
				movementList = new ArrayList<>();

				for (StaffVo updationProcessing : updationProcessingList) {

					if (updationProcessing.getCurrDiv() == null && updationProcessing.getPrevDiv() == null) {
						separationList.add(updationProcessing);
					} else {
						movementList.add(updationProcessing);
					}
				}
				// Commented for SIT - Enahancement
				if (updationProcessingList.size() != 0) {

					log.debug("Updating closed items");
					srasdao.checkForClosedItems(separationList, movementList);
					log.debug("After updating closed items");
				}

				log.debug("Updating closed item -  over");

				// Commented for SIT - Enahancement : Ends

				/**
				 * Start of separation process
				 */

				log.debug("Fetching separation data from IDM_separation table started");

				separationProcessingList = new ArrayList<>();
				separationProcessingList = pulseradao.getSeparationData();

				log.debug("End of Fetching separation data from IDM_separation table");

				if (separationProcessingList != null) {
					separationList = new ArrayList<>();
					movementList = new ArrayList<>();

					for (StaffVo separationProcess : separationProcessingList) {

						if (separationProcess.getCurrDiv() == null && separationProcess.getPrevDiv() == null) {
							separationList.add(separationProcess);
						} else {
							movementList.add(separationProcess);
						}
					}

					/**
					 * Separation staff deactivation in AD & VPN
					 */
					if (IDMConstants.DISABLE_AD.equalsIgnoreCase("true")
							&& IDMConstants.DISABLE_VPN.equalsIgnoreCase("true")) {

						for (StaffVo seperationStaff : separationList) {
							try {
								if (seperationStaff.getCurrDiv() == null && seperationStaff.getPrevDiv() == null && seperationStaff.getLegalEntity() != "FY") {

									LDAPUtil.disableStaff(seperationStaff.getStaffId());

									log.info("AD disabled for terminated staff:" + seperationStaff.getStaffId());

								} 
								
								//Separation staff deactivation in FY AD
								//@mujeeba.khan
								
								else if (seperationStaff.getCurrDiv() == null && seperationStaff.getPrevDiv() == null && seperationStaff.getLegalEntity() == "FY") {

									LDAPUtil.disableFYStaff(seperationStaff.getStaffId());

									log.info("AD disabled for terminated staff:" + seperationStaff.getStaffId());

								} else {
								//	log.info("AD not disabled for movement staff:" + seperationStaff.getStaffId());

								}
							} catch (Exception e) {
								log.error(e.getMessage());
								Calendar cal = Calendar.getInstance();
								IDMAlert.sendMail(e.getMessage(), "AD Deactivation for " + seperationStaff.getStaffId(),
										cal.getTime());

								log.error("Error occured while deactivating user in Active Directory for:"
										+ seperationStaff.getStaffId());

								pulseradao.updateStatusInMaster("Pending Deactivation", seperationStaff.getStaffId(),
										"NO");
								throw e;
							}
							try {
								if (seperationStaff.getCurrDiv() == null && seperationStaff.getPrevDiv() == null) {

									VPNUtil.disableStaff(seperationStaff.getStaffId());

									log.info("VPN disabled for terminated staff:" + seperationStaff.getStaffId());
								} else {
								//	log.info("VPN not disabled for movement staff:" + seperationStaff.getStaffId());
								}
							}

							catch (Exception e) {
								log.error(e.getMessage());
								Calendar cal = Calendar.getInstance();

								IDMAlert.sendMail(e.getMessage(),
										"VPN Deactivation for " + seperationStaff.getStaffId(), cal.getTime());

								log.error("Error occured while deactivating user VPN for:"
										+ seperationStaff.getStaffId());

							}
							// Updating status in MASTER Table as TERMINATED after disabling the ADVPN

							pulseradao.updateStatusInMaster("TERMINATED", seperationStaff.getStaffId(), "YES");
						}
					}

					HashMap<String, HashMap<String, ArrayList<StaffVo>>> separationListReturn = new HashMap<String, HashMap<String, ArrayList<StaffVo>>>();
					separationHashMap = new HashMap<>();
					movementMailMapper = new HashMap<>();
					globalSeparationMapper = new HashMap<>();
					log.debug("Fetching application list for separated staffs " + separationList.size());
					log.debug("separationList => "+ separationList);
					
					if (separationList.size() > 0) {

						separationListReturn = srasdao.getSeperationAppList(separationList);
						separationHashMap = separationListReturn.get("Separation");
						globalSeparationMapper = separationListReturn.get("Global");
					}

					log.debug("Fetching application list for Movement staffs " + movementList.size());
					log.debug("movementList => "+ movementList);
					
					if (movementList.size() > 0) {
						movementMailMapper = srasdao.getMovementAppList(movementList);
					}
					log.debug("End of fetching application list for separated/movement staffs");

					if (separationHashMap != null && !separationHashMap.isEmpty()) {

						log.info("Started sending separation mail Task...");
						sendMail(separationHashMap, "NormalMail", IDMConstantEnhanced.SEPARATION_MAIL_NOTIFICATION);

					} else {
						log.debug(
								"No separation mail send. No Application found for users fetched from Pulsera System");
					}
					if (movementMailMapper != null && !movementMailMapper.isEmpty()) {

						log.info("Started sending movement mail Task...");
						sendMail(movementMailMapper, "NormalMail", IDMConstantEnhanced.MOVEMENT_MAIL_NOTIFICATION);

					} else {
						log.debug("No movement mail send. No Application found for users fetched from Pulsera System");
					}

					if (globalSeparationMapper != null && !globalSeparationMapper.isEmpty()) {

						log.info("Started sending movement mail Task...");
						sendMail(globalSeparationMapper, "NormalMail",
								IDMConstantEnhanced.SEPARATION_MAIL_GLOBAL_NOTIFICATION);

					} else {
						log.debug(
								"No Global mail send. No global application found for users fetched from Pulsera System");
					}
				}
			
				log.info("Completed StartSeparationProcess*******");
			} else {
				log.debug("Job Inactive");
			}
			
		} catch (DataAccessException dataAccessException) {
			log.error(dataAccessException.getMessage());
			log.info(dataAccessException);
			log.debug("Error occured at startSeparationProcess method");
		} catch (Exception exception) {
			log.error(exception.getMessage());
			log.info(exception);
			log.debug("Error occured at startSeparationProcess method");
		}
	}

	/**
	 * This method is used to fetch the email address of system owners and trigger
	 * mail to them
	 * 
	 * @param separationStaffsMaps
	 * @throws Exception
	 */
	private void sendMail(HashMap<String, ArrayList<StaffVo>> separationStaffsMaps, String reminderType,
			String mailType) throws Exception {
		
		log.debug("STARt: sendMail ->  reminderType "+  reminderType+" mailType "+mailType);

		MDC.put("LOGID", "IDVS System");
		String customMailId = null;
		if (separationStaffsMaps != null) {
			try {
				appSOMap = new HashMap<>();
				log.debug("Start of fetching system owners emails for applications fetched");
				appSOMap = srasdao.getSystemOwners(separationStaffsMaps);
				log.debug("End of fetching system owners emails for applications fetched");
				if (appSOMap != null) {
					customMailId = srasdao.fetchCustomEmails();
					triggerMail(separationStaffsMaps, appSOMap, reminderType, mailType, customMailId);
				}
			} catch (Exception e) {
				log.error(e.getMessage());
				log.info(e);
				/* e.printStackTrace(); */
				log.debug("Error occured at sendMail method");
				throw e;
			}

		}
		log.debug("END: sendMail ->  reminderType "+  reminderType+" mailType "+mailType);
	}

	/**
	 * This method is used to set the properties of java mail and send the email to
	 * the application system owners
	 * 
	 * @param separationStaffsMaps
	 * @param SOInfoMap
	 * @param reminderType
	 * @throws MessagingException
	 * @throws UnsupportedEncodingException
	 */
	private void triggerMail(HashMap<String, ArrayList<StaffVo>> separationStaffsMaps,
			HashMap<String, SystemOwnerVO> SOInfoMap, String reminderType, String mailType, String customMailId)
			throws MessagingException, UnsupportedEncodingException {
		boolean globalType = false;
		try {
			MDC.put("LOGID", "IDVS System");
			log.debug("Start of mail trigger");

			Properties props = new Properties();
			props.setProperty("mail.smtp.host", "mhsmtp.malaysiaairlines.com");
			props.put("mail.smtp.socketFactory.port", "25");
			props.put("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFkactory");
			props.put("mail.smtp.auth", "false");
			props.put("mail.smtp.port", "25");
			Session session = Session.getDefaultInstance(props, new Authenticator() {
				protected PasswordAuthentication getPasswordAuthentication() {
					return new PasswordAuthentication("ext736673", "xxx");
				}
			});

			SystemOwnerVO sysOwnerObj;
			Set<String> keys = SOInfoMap.keySet();
			for (String idAdminStaffID : keys) {
				log.debug("START: IdAdmin Staff Id : " + idAdminStaffID);
				staffsList = new ArrayList<>();
				SMTPMessage message = new SMTPMessage(session);
				MimeMultipart content = new MimeMultipart("related");
				MimeBodyPart textPart = new MimeBodyPart();

				message.setFrom(new InternetAddress("IDVS@malaysiaairlines.com", "IDVS Notification"));

				// Need to check MailType
				if (IDMConstants.MAIL_TEST.equalsIgnoreCase("yes")) {
					// TEST MAILS
					if (mailType.equalsIgnoreCase(IDMConstantEnhanced.SEPARATION_MAIL_NOTIFICATION)) {
						if (reminderType.equalsIgnoreCase("Reminder1") || reminderType.equalsIgnoreCase("Reminder2")
								|| reminderType.equalsIgnoreCase("Warning")) {
							message.setSubject(
									"SF-TEST || [REMINDER] Action Required – Staff Separation Notification to ID Admin ");
						} else {
							message.setSubject("SF-TEST || [URGENT] Staff Separation Notification to ID Admin");
						}
					} else if (mailType.equalsIgnoreCase(IDMConstantEnhanced.MOVEMENT_MAIL_NOTIFICATION)) {
						if (reminderType.equalsIgnoreCase("Reminder1") || reminderType.equalsIgnoreCase("Reminder2")
								|| reminderType.equalsIgnoreCase("Warning")) {
							message.setSubject(
									"SF-TEST || [REMINDER] Action Required – Staff Transfer Notification to ID Admin ");
						} else {
							message.setSubject("SF-TEST || [URGENT] Staff Transfer Notification to ID Admin ");
						}
					} else if (mailType.equalsIgnoreCase(IDMConstantEnhanced.SEPARATION_MAIL_GLOBAL_NOTIFICATION)) {
						message.setSubject("SF-TEST || [ACKNOWLEDGEMENT] Staff Separation Notification to ID Admin ");
						globalType = true;
					}
				} else {
					// PROD MAILS
					if (mailType.equalsIgnoreCase(IDMConstantEnhanced.SEPARATION_MAIL_NOTIFICATION)) {
						if (reminderType.equalsIgnoreCase("Reminder1") || reminderType.equalsIgnoreCase("Reminder2")
								|| reminderType.equalsIgnoreCase("Warning")) {
							message.setSubject(
									" [REMINDER] Action Required – Staff Separation Notification to ID Admin ");
						} else {
							message.setSubject("[URGENT] Staff Separation Notification to ID Admin");
						}
					} else if (mailType.equalsIgnoreCase(IDMConstantEnhanced.MOVEMENT_MAIL_NOTIFICATION)) {
						if (reminderType.equalsIgnoreCase("Reminder1") || reminderType.equalsIgnoreCase("Reminder2")
								|| reminderType.equalsIgnoreCase("Warning")) {
							message.setSubject(
									" [REMINDER] Action Required – Staff Transfer Notification to ID Admin ");
						} else {
							message.setSubject("[URGENT] Staff Transfer Notification to ID Admin ");
						}
					} else if (mailType.equalsIgnoreCase(IDMConstantEnhanced.SEPARATION_MAIL_GLOBAL_NOTIFICATION)) {
						message.setSubject("[ACKNOWLEDGEMENT] Staff Separation Notification to ID Admin ");
						globalType = true;
					}
				}
				sysOwnerObj = new SystemOwnerVO();
				sysOwnerObj = SOInfoMap.get(idAdminStaffID);
				for (int i = 0; i < sysOwnerObj.getApplicationList().size(); i++) {
					if (separationStaffsMaps.containsKey(sysOwnerObj.getApplicationList().get(i).trim())) {

						staffsList.addAll(separationStaffsMaps.get(sysOwnerObj.getApplicationList().get(i)));
					}
				}
				log.debug("Email Address:" + sysOwnerObj.getIDA_email());
				if (!IDMConstants.returnSystemRun.trim().equalsIgnoreCase("UAT")) {
					log.debug("System.run - PROD ");
					if (sysOwnerObj.getIDA_email() != null && !sysOwnerObj.getIDA_email().isEmpty()) {
						if (sysOwnerObj.getIDA_email().contains(",")) {
							for (String idemail : sysOwnerObj.getIDA_email().split(",")) {
								if (idemail != null && !idemail.isEmpty()) {
									// message.addRecipient(Message.RecipientType.TO,new InternetAddress(idemail));
									message.setRecipients(Message.RecipientType.TO,
											(Address[]) InternetAddress.parse(idemail));
								}
							}
						} else {
							message.addRecipient(Message.RecipientType.TO,
									new InternetAddress(sysOwnerObj.getIDA_email()));
						}

//							message.addRecipient(Message.RecipientType.BCC,
//								new InternetAddress(IDMConstants.returnAlertMail));
						message.setRecipients(Message.RecipientType.BCC,
								(Address[]) InternetAddress.parse(IDMConstants.returnAlertMail));
					} else if (sysOwnerObj.getSO_email() != null && !sysOwnerObj.getSO_email().isEmpty()) {
						message.addRecipient(Message.RecipientType.TO, new InternetAddress(sysOwnerObj.getSO_email()));
//						message.addRecipient(Message.RecipientType.BCC,
//								new InternetAddress(IDMConstants.returnAlertMail));
//						
						message.setRecipients(Message.RecipientType.BCC,
								(Address[]) InternetAddress.parse(IDMConstants.returnAlertMail));

					}
					if (customMailId != null && !customMailId.isEmpty()) {
						// message.addRecipient(Message.RecipientType.CC,new
						// InternetAddress(customMailId));
						message.setRecipients(Message.RecipientType.CC,
								(Address[]) InternetAddress.parse(customMailId));
					}
				} else {
					log.debug("Only ALERT ");

					message.setRecipients(Message.RecipientType.TO,
							(Address[]) InternetAddress.parse(IDMConstants.returnAlertMail));
				}
				// freemarker stuff.
				String htmlText = null;
				try {
					htmlText = util.getEmailContent(staffsList, sysOwnerObj, reminderType, mailType);
				} catch (IOException | TemplateException e) {
					log.error(e.getMessage());
					log.info(e);
					/* e.printStackTrace(); */
				}

				textPart.setText(htmlText, "US-ASCII", "html");
				content.addBodyPart(textPart);
				// Now set the actual message
				message.setContent(content);

				log.debug("Before Sending email");
				log.debug("Email Address of ID Admin:" + sysOwnerObj.getIDA_email());
				try {
					try {
						Transport.send(message);
					} catch (Exception e) {

						log.error(e.getMessage());
						Calendar cal = Calendar.getInstance();
						IDMAlert.sendMail(e.getMessage(), "Email Notification", cal.getTime());
						throw e;
					}
					log.debug("After Sending email");
					pulseradao.updateEmailSentItems(staffsList, reminderType);
					
					if (reminderType.equalsIgnoreCase("NormalMail")) {
						srasdao.updateStatusInSras(staffsList, globalType);
					}
					
					log.debug("UPDATED IN DB : " + idAdminStaffID +" reminderType "+reminderType);
				} catch (Exception e) {
					log.error(e.getMessage());
					log.info(e);
					log.error("Sending email failed for email id:" + sysOwnerObj.getIDA_email() + "or"
							+ sysOwnerObj.getSO_email());
				}
				
				log.debug("END: IdAdmin Staff Id : " + idAdminStaffID);
			}
		} catch (MessagingException e) {
			log.error(e.getMessage());
			throw e;
		} catch (UnsupportedEncodingException e1) {
			log.error(e1.getMessage());
			throw e1;
		}
		
		log.debug("end of mail trigger");
	}

	public void startRemainderMailProcess() throws Exception {
		try {
			boolean status = srasdao.getJobConfigStatus(IDMConstantEnhanced.JOB_CONFIG_REMAINDER_MAIL);
			if (status) {
				
				log.debug("START: startRemainderMailProcess****");
				
				MDC.put("LOGID", "IDVS System");
				reminderHM = new HashMap<>();
				log.debug("Fetching Fetching reminder mail data from IDM_separation table started");
				separationProcessingList = new ArrayList<>();
				reminderHM = pulseradao.getRemainderMailData();
				log.debug("End of Fetching reminder mail data from IDM_separation table");

				Set<String> keys = reminderHM.keySet();
				for (String reminderType : keys) {
					
					log.debug("START:  reminderType "+ reminderType);
					
					separationProcessingList = reminderHM.get(reminderType);
					if (separationProcessingList != null) {
						separationList = new ArrayList<>();
						movementList = new ArrayList<>();
						for (int i = 0; i < separationProcessingList.size(); i++) {

							if (separationProcessingList.get(i).getCurrDiv() == null
									&& separationProcessingList.get(i).getPrevDiv() == null) {
								separationList.add(separationProcessingList.get(i));
							} else {
								movementList.add(separationProcessingList.get(i));
							}

						}
						
						HashMap<String, ArrayList<StaffVo>> separationHashMap = new HashMap<String, ArrayList<StaffVo>>();
						HashMap<String, ArrayList<StaffVo>> movementHashMap = new HashMap<String, ArrayList<StaffVo>>();
						
						log.debug("Separation List || " + separationList.toString());
						log.debug("Separation Remainder mail || fetching application list for separation staffs..");
						separationHashMap = srasdao.getSeparationListForReminder(separationList);
						
						log.debug("movementList List || " + movementList.toString());
						log.debug("Movement Remainder mail || fetching application list for Movement staffs..");
						movementHashMap = srasdao.getMovementListForReminder(movementList);
						
						log.info("Sending Reminder mail for Separation Data started..");
						log.info("separationHashMap =>"+separationHashMap);
						sendMail(separationHashMap, reminderType, IDMConstantEnhanced.SEPARATION_MAIL_NOTIFICATION);
						log.info("END: separationHashMap sendMail");
						
						log.info("Sending Reminder mail for Movement Data started..");
						log.info("movementHashMap =>"+movementHashMap);
						sendMail(movementHashMap, reminderType, IDMConstantEnhanced.MOVEMENT_MAIL_NOTIFICATION);
						
						log.info("END: movementHashMap sendMail");
					}
					log.debug("END:  reminderType "+ reminderType);
				}
				
				log.debug("END: startRemainderMailProcess****");
			} else {
				log.debug("Job Inactive");
			}
		} catch (Exception e) {
			log.error(e.getMessage());
			log.debug("Error occured at startReminderMailProcess method");
			log.info(e);
		}
	}

	/**
	 * This method is used to create quarterly reports and also manually forced
	 * reports from UI
	 * 
	 * @throws Exception
	 */
	public void startReportProcess() throws Exception {
		boolean status = srasdao.getJobConfigStatus(IDMConstantEnhanced.JOB_CONFIG_QUARTERLY_RPT);
		if (status) {
			log.debug("Report Processing Started...");
			ArrayList<String> staffIdList = new ArrayList<String>();
			staffIdList = pulseradao.getUsersForReport(); // To get the separation
															// staff id list from
															// idm_user_separation
															// table
			generateReports(staffIdList, 0);
			log.debug("End of Report Process");
		} else {
			log.debug("job inactive");
		}
	}

	/**
	 * Method is to get Division Level data for Quarterly report and calling method
	 * for application and staff data fetching
	 * 
	 * @param staffIdList
	 * @param rptIndicator
	 * @return
	 */
	public String generateReports(ArrayList<String> staffIdList, int rptIndicator) {
		log.debug(" STARt: Generating Report- Division Level");
		try {
			MDC.put("LOGID", "IDVS System");
			HashMap<String, ReportsVo> reportsHashMap = new HashMap<String, ReportsVo>();
			HashMap<String, Object> qReportMapper = new HashMap<String, Object>();
			ReportsVo reportsobj;
			reportsHashMap = srasdao.getDivAppInfo();

			Set<String> keys = reportsHashMap.keySet();
			ArrayList<StaffVo> staffList;
			ArrayList<String> dormantAppList;

			// Following code will generate the information required for reports

			for (String divId : keys) {
				reportsobj = new ReportsVo();
				dormantAppList = new ArrayList<String>();
				reportsobj = reportsHashMap.get(divId);

				staffList = new ArrayList<StaffVo>();

				staffList = srasdao
						.getStaffInfoForReports(IDMUtil.formatArrayListToString(reportsobj.getApplicationList())); // Get
																													// active
																													// users
																													// from
																													// idm_application_users
																													// table

				if (staffList != null) {
					int dormantIdCount = 0;
					int dormantAppCount = 0;
					for (int counter = 0; counter < staffList.size(); counter++) // Comparing staff id list with
																					// separation list to get dormant
																					// ids
					{

						if (staffList.size() > 0) {
							if (staffIdList.contains(staffList.get(counter).getStaffId().trim())) {
								dormantIdCount++;
								if (!dormantAppList.contains(staffList.get(counter).getApplicationName().trim())) {
									dormantAppList.add(staffList.get(counter).getApplicationName().trim());
									dormantAppCount++;
								}
							}
						}
					}
					reportsobj.setDormantIdCount(dormantIdCount);
					int no_of_app = reportsobj.getNo_of_Application();
					int status = 100;
					if (dormantAppCount != 0) {
						status = dormantAppCount / no_of_app;
						status = status / 100;
					}
					String remarks;
					if (dormantIdCount != 0)
						remarks = dormantIdCount + " IDs are dormant ";
					else
						remarks = "All Applications are in compliance";
					reportsobj.setDormantAppCount(dormantAppCount);
					reportsobj.setStaus(status);
					reportsobj.setRemarks(remarks);
					reportsHashMap.put(divId, reportsobj);
				}

			}
			HashMap<String, String> divInfo = pulseradao.getDivisionInfo();

			if (rptIndicator != 2) {

				ArrayList<NonComplianceVo> appLevelData = getAppLevelQReports(
						IDMUtil.formatArrayListToString(staffIdList), divInfo);

				log.debug("Fetching Application Level data from DAO COmpleted :: " + appLevelData.size());

				ArrayList<StaffVo> staffLevelList = getStaffLevelQReports(IDMUtil.formatArrayListToString(staffIdList));

				log.debug("Fetching Staff Level data from DAO COmpleted :: " + staffLevelList.size());

				qReportMapper.put(IDMConstantEnhanced.QR_APP_LEVEL_MAPPER, appLevelData);
				qReportMapper.put(IDMConstantEnhanced.QR_STAFF_LEVEL_MAPPER, staffLevelList);

			}

			qReportMapper.put(IDMConstantEnhanced.QR_DIVISION_LEVEL_MAPPER, reportsHashMap);

			QuarterlyReportGenerator report = new QuarterlyReportGenerator();

			String returnValue = null;

			if (rptIndicator == 2) {

				List<ComplianceVO> compliances = monthlyComplianceProcess(qReportMapper, divInfo); // Monthly Report

				java.util.Date date = new java.util.Date();
				Timestamp ts = new Timestamp(date.getTime());
				Calendar cal = Calendar.getInstance();
				int y1 = cal.get(Calendar.YEAR);
				int m1 = cal.get(Calendar.MONTH);

				srasdao.deleteCompliance(String.valueOf(y1), String.valueOf(m1));

				srasdao.saveMonthlyComplianceProcess(compliances, String.valueOf(y1), String.valueOf(m1));
			} else if (rptIndicator == 1) {
				returnValue = report.generateQuarterReports(qReportMapper, divInfo, 1); // Temporary Report
			} else {
				returnValue = report.generateQuarterReports(qReportMapper, divInfo, 0); // Quarterly Report
			}

			if (returnValue != null && rptIndicator == 0) {
				srasdao.updateGeneratedReport(returnValue);
			}
			return returnValue;
		} catch (IDMPulseraException idmPulseraException) {
			log.error("Database error occured, while connecting to Pulsera system \n"
					+ idmPulseraException.getStackTrace());
			log.error(idmPulseraException.getError());
			return null;
		} catch (IDMDBException idmDBException) {
			log.error(idmDBException.getError());
			return null;
		} catch (IDMException idmException) {
			log.error(idmException.getError());
			return null;
		} catch (Exception exception) {
			log.error(exception.getMessage());
			return null;
		}
		finally {
			log.debug(" END: Generating Report- Division Level");
		}
	}

	/**
	 * Method Calling DAO and to generate Application-Level Quarterly Reports.
	 * 
	 * @param staffIds
	 * @param divInfo
	 * @return
	 */
	public ArrayList<NonComplianceVo> getAppLevelQReports(String staffIds, HashMap<String, String> divInfo)
			throws IDMException {
		log.debug("Calling DAO for App Level Q-Report Data..");
		ArrayList<NonComplianceVo> appLevelData = new ArrayList<NonComplianceVo>();
		ArrayList<NonComplianceVo> appReportData = new ArrayList<NonComplianceVo>();
		try {
			appLevelData.addAll(srasdao.FetchAppLevelQReport(staffIds));
			if (appLevelData != null && !appLevelData.isEmpty()) {
				for (NonComplianceVo nonComplianceVo : appLevelData) {
					String divId = String.valueOf(nonComplianceVo.getDivId());
					if (divInfo != null && divInfo.containsKey(divId)) {
						nonComplianceVo.setDivName(divInfo.get(divId));
					} else {
						nonComplianceVo.setDivName("--Not Available--");
					}
					appReportData.add(nonComplianceVo);
				}
			}
		} catch (IDMDBException idmDBException) {
			log.error(idmDBException.getStackTrace());
			return null;
		} catch (IDMException idmException) {
			log.error(idmException.getStackTrace());
			return null;
		}
		log.debug("Calling DAO for App Level Q-Report Data completed..\nSIZE -" + appLevelData.size());
		return appReportData;
	}

	/**
	 * Method to call DAO to get Staff Level non-compliance data
	 * 
	 * @param staffIds
	 * @return
	 * @throws IDMException
	 */
	public ArrayList<StaffVo> getStaffLevelQReports(String staffIds) throws IDMException {
		ArrayList<StaffVo> staffLevelData = new ArrayList<StaffVo>();
		ArrayList<StaffVo> staffSOReportData = new ArrayList<StaffVo>();
		ArrayList<SystemOwnerVO> systemOwnerList = new ArrayList<SystemOwnerVO>();
		SystemOwnerVO systemOwnerVo;
		log.debug("Calling DAO for Staff Level Q-Report Data..");
		try {
			HashMap<String, Object> applnSOData = srasdao.fetchAllAppSO();
			staffLevelData.addAll(srasdao.fetchStaffLevelQReport(staffIds));
			if (staffLevelData != null && !staffLevelData.isEmpty()) {
				for (StaffVo staffvo : staffLevelData) {
					String appName = staffvo.getApplicationName();
					log.debug("Application Name " + appName);
					if (applnSOData != null && !applnSOData.isEmpty() && applnSOData.containsKey(appName)) {
						// COntinue from here
						systemOwnerVo = new SystemOwnerVO();
						systemOwnerList = new ArrayList<SystemOwnerVO>();
						systemOwnerVo = (SystemOwnerVO) applnSOData.get(appName);
						systemOwnerList.add(systemOwnerVo);
					}
					staffvo.setSystemOwnerList(systemOwnerList);
					staffSOReportData.add(staffvo);
				}
			}

		} catch (IDMDBException idmDBException) {
			log.error(idmDBException.getError());
			return null;
		} catch (IDMException idmException) {
			log.error(idmException.getError());
			return null;
		}
		return staffSOReportData;
	}

	// CHG0018071 _Automated Compliance reporting -produce ID management performance
	// & compliance report
	public void startMonthlyComplianceProcess() throws Exception {
		// boolean status =
		// srasdao.getJobConfigStatus(IDMConstantEnhanced.JOB_CONFIG_QUARTERLY_RPT);

		if (true) {
			log.debug("Report startMonthlyComplianceProcess Started...");
			ArrayList<String> staffIdList = new ArrayList<String>();
			staffIdList = pulseradao.getUsersForReport();
			generateReports(staffIdList, 2);
			log.debug("End of startMonthlyComplianceProcess Process");
		} else {
			log.debug("job inactive");
		}

	}

	// CHG0018071 _Automated Compliance reporting -produce ID management performance
	// & compliance report

	private List<ComplianceVO> monthlyComplianceProcess(HashMap<String, Object> quartelyrReportMapper,
			HashMap<String, String> divInfo) {

		java.util.Date date = new java.util.Date();
		Timestamp ts = new Timestamp(date.getTime());
		Calendar cal = Calendar.getInstance();
		int y1 = cal.get(Calendar.YEAR);
		int m1 = cal.get(Calendar.MONTH);

		HashMap<String, ReportsVo> divReportData = new HashMap<String, ReportsVo>();

		if (quartelyrReportMapper != null && !quartelyrReportMapper.isEmpty()) {

			if (quartelyrReportMapper.get(IDMConstantEnhanced.QR_DIVISION_LEVEL_MAPPER) != null
					&& !quartelyrReportMapper.get(IDMConstantEnhanced.QR_DIVISION_LEVEL_MAPPER).equals(null)) {
				divReportData.putAll((Map<String, ReportsVo>) quartelyrReportMapper
						.get(IDMConstantEnhanced.QR_DIVISION_LEVEL_MAPPER));
			}
		}

		List<ComplianceVO> compliances = new ArrayList<>();

		for (String divId : divReportData.keySet()) {
			String division = divInfo.get(divId);
			double application = divReportData.get(divId).getNo_of_Application();
			double complainceApplication = divReportData.get(divId).getNo_of_Application()
					- divReportData.get(divId).getDormantAppCount();
			String remark = divReportData.get(divId).getRemarks();

			DecimalFormat df = new DecimalFormat("#");
			df.setRoundingMode(RoundingMode.CEILING);

			String compliancePercent = df.format(Math.round((complainceApplication/application)*100));

			ComplianceVO complianceVO = new ComplianceVO();
			complianceVO.setApplications(String.valueOf((int) application));
			complianceVO.setComplianceApplications(String.valueOf((int) complainceApplication));
			complianceVO.setDivision(division);
			complianceVO.setCompliancePercent(compliancePercent);
			complianceVO.setRemarks(remark);
			complianceVO.setComplianceYear(String.valueOf(y1));
			complianceVO.setComplianceMonth(String.valueOf(m1));
			compliances.add(complianceVO);
		}

		return compliances;
	}

}
