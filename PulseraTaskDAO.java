package com.mas.idm.dao;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.MDC;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.InvalidResultSetAccessException;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.support.JdbcDaoSupport;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.util.CollectionUtils;

import com.mas.idm.bean.ApplicationUsersVo;
import com.mas.idm.bean.ApplicationVo;
import com.mas.idm.bean.StaffVo;
import com.mas.idm.enums.ErrorCodeEnums;
import com.mas.idm.exception.IDMDBException;
import com.mas.idm.exception.IDMException;
import com.mas.idm.exception.IDMPulseraException;
import com.mas.idm.util.IDMUtil;
import org.apache.logging.log4j.LogManager;import org.apache.logging.log4j.Logger;

public class PulseraTaskDAO extends JdbcDaoSupport {

	private static final Logger log = LogManager.getLogger(PulseraTaskDAO.class);
	private SqlRowSet rowSet;
	private SqlRowSet rowSet1;
	private SqlRowSet rowSet2;
	private SqlRowSet rowSet3;
	private StaffVo staffobj;
	private SrasTasksDAO srasdao;
	private boolean fetchError;
	private ArrayList<StaffVo> separationProcessingList;
	private ArrayList<StaffVo> reminderProcessingList;
	private ArrayList<StaffVo> updationProcessingList;
	private ArrayList<String> ncProcessingList;
	private HashMap<String, ArrayList<StaffVo>> reminderHashMap;
	private ApplicationVo appobj;

	public ApplicationVo getAppobj() {
		return appobj;
	}

	public void setAppobj(ApplicationVo appobj) {
		this.appobj = appobj;
	}

	public SrasTasksDAO getSrasdao() {
		return srasdao;
	}

	public void setSrasdao(SrasTasksDAO srasdao) {
		this.srasdao = srasdao;
	}

	/**
	 * This method is used to get the Pulsera separation data from views and store
	 * it in separation table.
	 * 
	 * @param last_processesd_date
	 */
	public void getPulseraData(String last_processesd_date) {

		MDC.put("LOGID", "IDVS System");
		log.info("          || Fetching Separation Data from Separation Table/View   || ");

		java.util.Date date = new java.util.Date();
		Timestamp ts = new Timestamp(date.getTime());
		String current_date = new SimpleDateFormat("dd-MMM-yyyy").format(ts);

		try {
			newHire();
			getNewJoineeInfo();

			movement();

			seperation();

			srasdao.setLastProcessedDate(current_date);

		} catch (InvalidResultSetAccessException e) {
			log.error(e.getMessage());
			log.info(e);
			throw e;
		} catch (DataAccessException e) {
			log.error(e.getMessage());
			fetchError = true;
			log.info(e);
			throw e;
		}
	}

	private void seperation() {
		ArrayList<StaffVo> separationList = new ArrayList<>();

		rowSet = getJdbcTemplate().queryForRowSet(
				"SELECT Full_NAME,EMPLOYEE_NUMBER,DATE_FORMAT(TERMINATION_DATE,'%Y-%b-%d %T') as TERMINATION_DATE FROM  IDVS_MASTER_TABLE WHERE DATE_FORMAT(LAST_UPDATED_DATE,'%Y-%m-%d')= DATE_FORMAT(NOW(),'%Y-%m-%d') AND PROCESS_TYPE='TERMINATION'");

		ArrayList<String> reverseCheckList = new ArrayList<>();
		while (rowSet.next()) {
			if (rowSet.getString("TERMINATION_DATE") == null) {
				reverseCheckList.add(rowSet.getString("EMPLOYEE_NUMBER"));
			} else {
				staffobj = new StaffVo();
				staffobj.setStaffName(rowSet.getString("Full_NAME"));
				staffobj.setStaffId(rowSet.getString("EMPLOYEE_NUMBER"));
				staffobj.setSeparationDate(rowSet.getString("TERMINATION_DATE"));
				separationList.add(staffobj);
			}
		}

		log.debug("         ||  Number of Separation data          || " + separationList.size());
		log.debug("         ||  Number of Reverse Termination data || " + reverseCheckList.size());

		checkAndUpdateReverseTermination(IDMUtil.formatArrayListToString(reverseCheckList));

		separationList = checkAndUpdateAvailableRecord(separationList);

		insertSeparationData(separationList);
	}

	private void newHire() {

		ArrayList<StaffVo> newStaffList;

		try {

			/*********************************************
			 * NEW STAFF
			 *****************************************************************/
			log.info("          || Fetching New Staff Data from New and Rehire Table/View || ");

			newStaffList = new ArrayList<>();

			rowSet2 = getJdbcTemplate().queryForRowSet(
					"SELECT Full_NAME, EMPLOYEE_NUMBER, DEPARTMENT,DIVISION, DATE_FORMAT(DATE_START, '%d-%b-%Y %T') AS JOINING_DATE "
							+ "FROM IDVS_MASTER_TABLE " + "WHERE DATE_FORMAT(LAST_UPDATED_DATE,'%Y-%m-%d')= "
							+ "DATE_FORMAT(NOW(),'%Y-%m-%d') AND PROCESS_TYPE='NEW_PROFILE' ");

			while (rowSet2.next()) {
				staffobj = new StaffVo();
				staffobj.setStaffName(rowSet2.getString("Full_NAME"));
				staffobj.setStaffId(rowSet2.getString("EMPLOYEE_NUMBER"));
				staffobj.setJoiningDate(rowSet2.getString("JOINING_DATE"));
				staffobj.setJoiningDept(rowSet2.getString("DEPARTMENT"));
				staffobj.setJoiningDivision(rowSet2.getString("DIVISION"));
				newStaffList.add(staffobj);

			}
			log.debug("         ||  Number of New Staff List || " + newStaffList.size());
			log.debug("         ||  New Staff List           || " + newStaffList.toString());

			if (!CollectionUtils.isEmpty(newStaffList)) {
				try {
					insertNewJoineeData(newStaffList);
				} catch (Exception e) {
					log.error(e.getMessage());
					log.debug("insertion of new joinee data failed");

					log.error(e);

					log.info(e);
				}
			}
		} catch (Exception e) {
			log.error(e.getMessage());

		}

	}

	private void movement() {

		ArrayList<StaffVo> movementList = new ArrayList<>();

		ArrayList<StaffVo> masterDataList = new ArrayList<>();
		ArrayList<StaffVo> masterDataHistoryList = new ArrayList<>();

		log.info("          || Fetching Movement Data from Movement Table/View   || ");

		rowSet1 = getJdbcTemplate().queryForRowSet(
				"SELECT Full_NAME,EMPLOYEE_NUMBER,DIVISION,DATE_FORMAT(DATE_START, '%Y-%b-%d %T') as MOVEMENT_DATE FROM  IDVS_MASTER_TABLE WHERE DATE_FORMAT(LAST_UPDATED_DATE,'%Y-%m-%d') = DATE_FORMAT(NOW(),'%Y-%m-%d') AND PROCESS_TYPE='PROFILE_UPDATE'");

		while (rowSet1.next()) {

			StaffVo masterdata = new StaffVo();
			masterdata.setStaffName(rowSet1.getString("Full_NAME"));
			masterdata.setStaffId(rowSet1.getString("EMPLOYEE_NUMBER"));
			masterdata.setMovementDate(rowSet1.getString("MOVEMENT_DATE"));
			masterdata.setCurrDiv(rowSet1.getString("DIVISION"));

			masterDataList.add(masterdata);
		}

		if (CollectionUtils.isEmpty(masterDataList)) {
			return;
		}
		SqlRowSet historyRowset = getJdbcTemplate().queryForRowSet(
				"SELECT Full_NAME,EMPLOYEE_NUMBER,DIVISION,DATE_FORMAT(DATE_START, '%Y-%b-%d %T') as MOVEMENT_DATE FROM  IDVS_HISTORY_TABLE WHERE DATE_FORMAT(UPDATED_DATE,'%Y-%m-%d') = DATE_FORMAT(NOW(),'%Y-%m-%d')");

		while (historyRowset.next()) {

			StaffVo history = new StaffVo();
			history.setStaffName(historyRowset.getString("Full_NAME"));
			history.setStaffId(historyRowset.getString("EMPLOYEE_NUMBER"));
			history.setMovementDate(historyRowset.getString("MOVEMENT_DATE"));
			history.setCurrDiv(historyRowset.getString("DIVISION"));

			masterDataHistoryList.add(history);
		}

		for (StaffVo masterdata : masterDataList) {

			staffobj = new StaffVo();
			staffobj.setStaffName(masterdata.getStaffName());
			staffobj.setStaffId(masterdata.getStaffId());
			staffobj.setMovementDate(masterdata.getMovementDate());

			StaffVo historyTemp = null;
			for (StaffVo history : masterDataHistoryList) {

				if (masterdata.getStaffId().equalsIgnoreCase(history.getStaffId())) {
					historyTemp = history;
					break;
				}
			}

			if (historyTemp != null) {
				if (historyTemp.getCurrDiv() != null) {
					staffobj.setPrevDiv(historyTemp.getCurrDiv());
				} else {
					staffobj.setPrevDiv(null);
				}
			} else {
				staffobj.setPrevDiv(null);
			}

			if (masterdata.getCurrDiv() != null) {
				staffobj.setCurrDiv(masterdata.getCurrDiv());
			} else {
				staffobj.setCurrDiv(null);
			}
			// added
			if (staffobj.getPrevDiv() != null && staffobj.getCurrDiv() != null
					&& !(staffobj.getPrevDiv().equalsIgnoreCase(staffobj.getCurrDiv()))) {
				movementList.add(staffobj);
			}

		}

		log.debug("         ||  Number of Movement data || " + movementList.size());
		log.debug("         ||  MovementList List       || " + movementList.toString());

		movementList = checkAndUpdateMovementRecord(movementList);

		insertMovementData(movementList);

	}

	private void insertNewJoineeData(final ArrayList<StaffVo> newStaffList2) {

		MDC.put("LOGID", "IDVS System");
		log.debug("             || Inserting New Staff Data started            || ");

		if (!CollectionUtils.isEmpty(newStaffList2)) {

			List<StaffVo> insertUserInfos = new ArrayList<>();
			List<StaffVo> updateUserInfos = new ArrayList<>();
			try {
				StringBuffer staffSb = new StringBuffer();
				List<String> stafs = new ArrayList<>();

				for (StaffVo staff : newStaffList2) {
					staffSb.append("'").append(staff.getStaffId()).append("',");
				}
				String param = staffSb.substring(0, staffSb.length() - 1);

				SqlRowSet rowSetSO = getJdbcTemplate()
						.queryForRowSet("select STAFF_ID from MAS_IDM_NEW_USER_INFO WHERE STAFF_ID in (" + param + ")");

				while (rowSetSO.next()) {
					stafs.add(rowSetSO.getString("STAFF_ID"));
				}

				for (StaffVo staffVo : newStaffList2) {
					if (stafs.contains(staffVo.getStaffId())) {
						updateUserInfos.add(staffVo);
					} else {
						insertUserInfos.add(staffVo);
					}
				}

				String updateSql = "UPDATE MAS_IDM_NEW_USER_INFO SET JOINING_DATE=str_to_date(?, '%d-%b-%Y %T'),STATUS=? "
						+ " WHERE STAFF_ID =? AND DATE_FORMAT(JOINING_DATE,'%d-%b-%Y %T')!=?";

				getJdbcTemplate().batchUpdate(updateSql, new BatchPreparedStatementSetter() {

					@Override
					public void setValues(PreparedStatement ps, int i) throws SQLException {
						StaffVo sobj = updateUserInfos.get(i);
						ps.setString(1, sobj.getJoiningDate());
						ps.setString(2, "NOT_STARTED");
						ps.setString(3, sobj.getStaffId());
						ps.setString(4, sobj.getJoiningDate());
					}

					@Override
					public int getBatchSize() {
						return updateUserInfos.size();
					}
				});

				log.debug(" UPDATED " + updateUserInfos.size() + " ," + updateUserInfos);

				String insertSql = "INSERT INTO MAS_IDM_NEW_USER_INFO"
						+ "(STAFF_ID, STAFF_NAME, JOINING_DATE,STATUS) VALUES (?,?,str_to_date(?, '%d-%b-%Y %T'),?)";
				getJdbcTemplate().batchUpdate(insertSql, new BatchPreparedStatementSetter() {

					@Override
					public void setValues(PreparedStatement ps, int i) throws SQLException {
						StaffVo sobj = insertUserInfos.get(i);
						ps.setString(1, sobj.getStaffId());
						ps.setString(2, sobj.getStaffName());
						ps.setString(3, sobj.getJoiningDate());
						ps.setString(4, "NOT_STARTED");
					}

					@Override
					public int getBatchSize() {
						return insertUserInfos.size();
					}
				});

				log.debug(" INSERTED " + insertUserInfos.size() + " ," + insertUserInfos);

				log.debug("     || Insertion completed successfully            || ");
			}

			catch (DataAccessException e) {
				log.error(e.getMessage());
				log.info(e);
				log.debug("Database Connection could not be established");
				throw e;
			}
		}
	}

	private ArrayList<StaffVo> checkAndUpdateMovementRecord(ArrayList<StaffVo> movementList2) {

		if (CollectionUtils.isEmpty(movementList2)) {
			return null;
		}

		ArrayList<StaffVo> movList = new ArrayList<>();

		ArrayList<StaffVo> movementSeperationList = new ArrayList<>();

		log.debug("         || Check and Update Movement Data Started || ");

		rowSet1 = getJdbcTemplate().queryForRowSet("SELECT STAFF_NAME,STAFF_ID,PREV_DIVISION,CURR_DIVISION,"
				+ "DATE_FORMAT(ACTUAL_SEPARATION_DATE, '%d-%b-%Y %T') as TERMINATION_DATE,SEPARATION_STATUS"
				+ " FROM  MAS_IDM_USER_SEPARATION_LIST WHERE  SEPARATION_STATUS='NOT_STARTED' AND PREV_DIVISION IS NOT NULL AND CURR_DIVISION IS NOT NULL  ");

		while (rowSet1.next()) {

			StaffVo staffVo = new StaffVo();
			staffVo.setStaffId(rowSet1.getString("STAFF_ID"));
			staffVo.setStaffName(rowSet1.getString("STAFF_NAME"));
			staffVo.setCurrDiv(rowSet1.getString("CURR_DIVISION"));
			staffVo.setPrevDiv(rowSet1.getString("PREV_DIVISION"));
			staffVo.setSeparationDate(rowSet1.getString("TERMINATION_DATE"));
			staffVo.setSeparationStatus(rowSet1.getString("SEPARATION_STATUS"));

			movementSeperationList.add(staffVo);
		}

		for (StaffVo staffVoMovement : movementList2) {

			try {
				boolean rowCheck = false;
				for (StaffVo seperation : movementSeperationList) {

					if (staffVoMovement.getStaffId().equals(seperation.getStaffId())) {
						rowCheck = true;
						if (seperation.getCurrDiv().equalsIgnoreCase(staffVoMovement.getPrevDiv())
								&& seperation.getPrevDiv().equalsIgnoreCase(staffVoMovement.getCurrDiv())) {
							try {

								getJdbcTemplate().update("UPDATE MAS_IDM_USER_SEPARATION_LIST "
										+ "SET SEPARATION_STATUS='REVERSE_MOVEMENT'"
										+ " WHERE STAFF_ID=? AND ACTUAL_SEPARATION_DATE = str_to_date(?,'%d-%b-%Y %T') AND SEPARATION_STATUS='NOT_STARTED' ",
										staffVoMovement.getStaffId(), seperation.getSeparationDate());

							} catch (Exception e) {
								log.error(e.getMessage());
								log.info(e);
							}
						}
						if (!seperation.getSeparationDate().equalsIgnoreCase(staffVoMovement.getMovementDate())) {
							try {

								getJdbcTemplate().update("UPDATE MAS_IDM_USER_SEPARATION_LIST "
										+ "SET  ACTUAL_SEPARATION_DATE = str_to_date(?, '%Y-%b-%d %T') "
										+ " WHERE STAFF_ID=? AND ACTUAL_SEPARATION_DATE = str_to_date(?,'%d-%b-%Y %T') AND SEPARATION_STATUS='NOT_STARTED' ",
										staffVoMovement.getMovementDate(), staffVoMovement.getStaffId(),
										seperation.getSeparationDate());

							} catch (Exception e) {

								log.error(e.getMessage());
								log.info(e);
							}
						}

					}
				}
				if (rowCheck == false) {
					movList.add(staffVoMovement);
				}

			} catch (InvalidResultSetAccessException e) {
				log.error(e.getMessage());
				log.info(e);

				log.error(e);
			} catch (DataAccessException e) {
				log.error(e.getMessage());
				log.info(e);
				// e.printStackTrace();
			}
		}
		log.debug("             || Check and Update Movement Record completed successfully || ");
		return movList;
	}

	/**
	 * This method checks for the records already available in the user separation
	 * table and updates it if any change is required.
	 * 
	 * @param separationList2
	 */
	private ArrayList<StaffVo> checkAndUpdateAvailableRecord(ArrayList<StaffVo> separationList2) {

		if (CollectionUtils.isEmpty(separationList2)) {
			return null;
		}

		log.debug("         ||  Check and Update Existing Separation Data Started || ");

		ArrayList<StaffVo> sepList = new ArrayList<>();
		ArrayList<StaffVo> seperatonInnerList = new ArrayList<>();

		rowSet1 = getJdbcTemplate().queryForRowSet("SELECT STAFF_NAME,STAFF_ID,PREV_DIVISION,CURR_DIVISION,"
				+ "DATE_FORMAT(ACTUAL_SEPARATION_DATE, '%d-%b-%Y %T') as TERMINATION_DATE,SEPARATION_STATUS"
				+ " FROM  MAS_IDM_USER_SEPARATION_LIST WHERE  SEPARATION_STATUS='NOT_STARTED' AND PREV_DIVISION IS NULL ");

		while (rowSet1.next()) {

			StaffVo seperationInner = new StaffVo();
			seperationInner.setStaffId(rowSet1.getString("STAFF_ID"));
			seperationInner.setSeparationDate(rowSet1.getString("TERMINATION_DATE"));

			seperatonInnerList.add(seperationInner);
		}

		for (StaffVo staffVo : separationList2) {
			try {
				boolean rowCheck = false;
				for (StaffVo seperationInner : seperatonInnerList) {

					if (staffVo.getStaffId().equalsIgnoreCase(seperationInner.getStaffId())) {
						rowCheck = true;
						if (!seperationInner.getSeparationDate().equalsIgnoreCase(staffVo.getSeparationDate())) {
							try {

								getJdbcTemplate().update("UPDATE MAS_IDM_USER_SEPARATION_LIST "
										+ "SET  ACTUAL_SEPARATION_DATE = str_to_date(?, '%Y-%b-%d %T')  "
										+ " WHERE STAFF_ID=? AND ACTUAL_SEPARATION_DATE = str_to_date(?,'%d-%b-%Y %T') "
										+ "AND SEPARATION_STATUS='NOT_STARTED' ", staffVo.getSeparationDate(),
										staffVo.getStaffId(), seperationInner.getSeparationDate());

							} catch (Exception e) {
								log.error(e.getMessage());
								log.info(e);
							}
						}
					}
				}
				if (rowCheck == false) {
					sepList.add(staffVo);
				}
			} catch (InvalidResultSetAccessException e) {
				log.error(e.getMessage());
				log.info(e);
			} catch (DataAccessException e) {
				log.error(e.getMessage());
				log.info(e);
			}
		}

		log.debug("         ||  Check and Update Existing Separation Data Completed Successfully || ");

		return sepList;

	}

	/**
	 * This method is used to check a employee for reverse termination process
	 * 
	 * @param staffId
	 */

	private void checkAndUpdateReverseTermination(String staffIdList) {

		try {

			if (StringUtils.isEmpty(staffIdList)) {
				return;
			}
			log.debug("         ||  Check and Update for Reverse Termination started  || ");

			rowSet1 = getJdbcTemplate()
					.queryForRowSet("SELECT * FROM  MAS_IDM_USER_SEPARATION_LIST" + " WHERE STAFF_ID IN (" + staffIdList
							+ ") AND PREV_DIVISION IS NULL AND SEPARATION_STATUS!='COMPLETED' ");
			while (rowSet1.next()) {
				try {
					getJdbcTemplate()
							.update("UPDATE MAS_IDM_USER_SEPARATION_LIST SET SEPARATION_STATUS = 'REVERSE_TERMINATED', "
									+ "LAST_UPDATED_DATE = DATE_FORMAT(NOW(),'%Y-%m-%d %T')  WHERE STAFF_ID='"
									+ rowSet1.getString("STAFF_ID") + "' ");
				} catch (Exception e) {
					log.error(e.getMessage());
					log.debug("Update for reverse termination data failed");
					log.info(e);
				}
			}

			log.debug("         || Check and Update for Reverse Termination completed successfully  || ");
		} catch (InvalidResultSetAccessException e) {
			log.error(e.getMessage());
			log.debug("Invalid result set is accessed");
			log.info(e);
		} catch (DataAccessException e) {
			log.error(e.getMessage());
			log.debug("Specified column is not available in the table");
			log.info(e);
		} catch (Exception e) {
			log.error(e.getMessage());
			log.debug("Some other exception occured");
			log.info(e);
		}

	}

	/**
	 * This method is used to insert the pulsera data into IDM_USER_SEPARATION_TABLE
	 * 
	 * @param separationList2
	 * @param movementList2
	 */
	public void insertSeparationData(final ArrayList<StaffVo> separationList2) {
		MDC.put("LOGID", "IDVS System");
		if (!CollectionUtils.isEmpty(separationList2)) {
			try {
				log.debug("     || Insertion of Separation Data started        || ");

				String sql = "INSERT INTO MAS_IDM_USER_SEPARATION_LIST"
						+ "(ID,STAFF_ID, STAFF_NAME, ACTUAL_SEPARATION_DATE,SEPARATION_STATUS,EMAIL_STATUS,CREATED_DATE,AD_UPDATE,VPN_UPDATE) "
						+ "VALUES (default,?,?,str_to_date(?, '%Y-%b-%d %T'),?,?,DATE_FORMAT(NOW(),'%Y-%m-%d %T'),?,?)";
				getJdbcTemplate().batchUpdate(sql, new BatchPreparedStatementSetter() {

					@Override
					public void setValues(PreparedStatement ps, int i) throws SQLException {
						StaffVo sobj = separationList2.get(i);
						ps.setString(1, sobj.getStaffId());
						ps.setString(2, sobj.getStaffName());
						ps.setString(3, sobj.getSeparationDate());
						ps.setString(4, "NOT_STARTED");
						ps.setString(5, "NOT_SENT");
						ps.setString(6, "NOT_STARTED");
						ps.setString(7, "NOT_STARTED");
					}

					@Override
					public int getBatchSize() {
						return separationList2.size();
					}
				});
				log.debug("     || Total Number of Separation Records Inserted || " + separationList2.size());
				log.debug("     || Insertion completed successfully            || ");

			} catch (DataAccessException e) {
				log.error(e.getMessage());
				log.info(e);
				log.debug("Database Connection could not be established");
				throw e;
			}
		}
	}

	public void insertMovementData(final ArrayList<StaffVo> movementList2) {
		MDC.put("LOGID", "IDVS System");

		if (!CollectionUtils.isEmpty(movementList2)) {
			try {
				log.debug("     || Insertion of Movement Data started          || ");

				String sql = "INSERT INTO MAS_IDM_USER_SEPARATION_LIST"
						+ "(ID,STAFF_ID, STAFF_NAME,PREV_DIVISION,CURR_DIVISION, ACTUAL_SEPARATION_DATE,SEPARATION_STATUS,EMAIL_STATUS,CREATED_DATE,AD_UPDATE,VPN_UPDATE) "
						+ "VALUES (default,?,?,?,?,str_to_date(?,'%Y-%b-%d %T'),?,?,DATE_FORMAT(NOW(),'%Y-%m-%d %T'),?,?)";
				getJdbcTemplate().batchUpdate(sql, new BatchPreparedStatementSetter() {

					@Override
					public void setValues(PreparedStatement ps, int i) throws SQLException {
						StaffVo sobj = movementList2.get(i);
						ps.setString(1, sobj.getStaffId());
						ps.setString(2, sobj.getStaffName());
						ps.setString(3, sobj.getPrevDiv());
						ps.setString(4, sobj.getCurrDiv());
						ps.setString(5, sobj.getMovementDate());
						ps.setString(6, "NOT_STARTED");
						ps.setString(7, "NOT_SENT");
						ps.setString(8, "NOT_STARTED");
						ps.setString(9, "NOT_STARTED");

					}

					@Override
					public int getBatchSize() {
						return movementList2.size();
					}
				});
				log.debug("     || Total Number of Movement Records Inserted   || " + movementList2.size());
				log.debug("     || Insertion completed successfully            || ");

			} catch (DataAccessException e) {
				log.error(e.getMessage());
				log.info(e);
				log.debug("Database Connection could not be established");
				throw e;
			}
		}
	}

	/**
	 * This method is used to do the separation processing by fetching the staffs
	 * who are getting separated / moved for current system date
	 * 
	 * @return
	 */
	public ArrayList<StaffVo> getSeparationData() {
		try {
			log.debug("         || Fetching Separation data                    || ");
			separationProcessingList = new ArrayList<>();

			rowSet3 = getJdbcTemplate().queryForRowSet(
					"SELECT STAFF_NAME,STAFF_ID,PREV_DIVISION,CURR_DIVISION, DATE_FORMAT(ACTUAL_SEPARATION_DATE, '%d-%b-%Y %T') as TERMINATION_DATE,SEPARATION_STATUS "
							+ "FROM MAS_IDM_USER_SEPARATION_LIST"
							+ " WHERE ACTUAL_SEPARATION_DATE <= NOW() AND SEPARATION_STATUS='NOT_STARTED'");

			while (rowSet3.next()) {

				staffobj = new StaffVo();
				staffobj.setStaffName(rowSet3.getString("STAFF_NAME"));
				staffobj.setStaffId(rowSet3.getString("STAFF_ID"));
				staffobj.setPrevDiv(rowSet3.getString("PREV_DIVISION"));
				staffobj.setCurrDiv(rowSet3.getString("CURR_DIVISION"));
				staffobj.setSeparationDate(rowSet3.getString("TERMINATION_DATE"));
				staffobj.setSeparationStatus(rowSet3.getString("SEPARATION_STATUS"));
				separationProcessingList.add(staffobj);

			}
			log.debug("         || Number of Separation Records Fetched || " + separationProcessingList.size());
			log.debug("         || Separation Records Fetched           || " + separationProcessingList.toString());
			return separationProcessingList;
		} catch (InvalidResultSetAccessException e) {
			log.error(e.getMessage());
			log.info(e);
			log.debug("Database Connection could not be established / Resultset fetch error");
			throw e;
		} catch (DataAccessException e) {
			log.error(e.getMessage());
			log.info(e);

			log.debug("Database Connection could not be established");
			throw e;
		}
	}

	/**
	 * This method is used to update the entries in separation table for whom
	 * separation email alerts have been sent using triggermail method
	 * 
	 * @param applicationName
	 * @param staffsList
	 * @param reminderType
	 */
	public void updateEmailSentItems(final ArrayList<StaffVo> staffsList, final String reminderType) {

		try {
			String sql = new String();
			if (reminderType.equalsIgnoreCase("NormalMail")) {
				sql = "UPDATE MAS_IDM_USER_SEPARATION_LIST "
						+ " SET SEPARATION_STATUS=?, EMAIL_STATUS=?, EMAIL_SENT_DATE = DATE_FORMAT(NOW(),'%Y-%m-%d %T'),AD_UPDATE=?,VPN_UPDATE=?, "
						+ "LAST_UPDATED_DATE = DATE_FORMAT(NOW(),'%Y-%m-%d %T') "
						+ "WHERE STAFF_ID=? AND SEPARATION_STATUS='NOT_STARTED' AND ACTUAL_SEPARATION_DATE = str_to_date(?,'%d-%b-%Y %T')  ";
			} else if (reminderType.equalsIgnoreCase("Reminder1")) {
				sql = "UPDATE MAS_IDM_USER_SEPARATION_LIST "
						+ " SET SEPARATION_STATUS=?, EMAIL_STATUS=?, REMINDER1_SENT_DATE = DATE_FORMAT(NOW(),'%Y-%m-%d %T'), LAST_UPDATED_DATE = DATE_FORMAT(NOW(),'%Y-%m-%d %T') "
						+ "WHERE STAFF_ID=? AND SEPARATION_STATUS='OPEN' AND ACTUAL_SEPARATION_DATE = str_to_date(?,'%d-%b-%Y %T') ";
			} else if (reminderType.equalsIgnoreCase("Reminder2")) {
				sql = "UPDATE MAS_IDM_USER_SEPARATION_LIST "
						+ " SET SEPARATION_STATUS=?, EMAIL_STATUS=?, REMINDER2_SENT_DATE = DATE_FORMAT(NOW(),'%Y-%m-%d %T'), LAST_UPDATED_DATE = DATE_FORMAT(NOW(),'%Y-%m-%d %T') "
						+ "WHERE STAFF_ID=? AND SEPARATION_STATUS='OPEN' AND ACTUAL_SEPARATION_DATE = str_to_date(?,'%d-%b-%Y %T') ";
			} else {
				sql = "UPDATE MAS_IDM_USER_SEPARATION_LIST "
						+ " SET SEPARATION_STATUS=?, EMAIL_STATUS=?, WARNING_MAIL_SENT_DATE = DATE_FORMAT(NOW(),'%Y-%m-%d %T'), LAST_UPDATED_DATE = DATE_FORMAT(NOW(),'%Y-%m-%d %T') "
						+ "WHERE STAFF_ID=? AND SEPARATION_STATUS='OPEN' AND ACTUAL_SEPARATION_DATE = str_to_date(?,'%d-%b-%Y %T') ";
			}
			getJdbcTemplate().batchUpdate(sql, new BatchPreparedStatementSetter() {

				@Override
				public void setValues(PreparedStatement ps, int i) throws SQLException {
					StaffVo sobj = staffsList.get(i);
					ps.setString(1, "OPEN");
					if (reminderType.equalsIgnoreCase("NormalMail")) {
						ps.setString(2, "EMAIL_SENT");
						ps.setString(3, "UPDATED");
						ps.setString(4, "UPDATED");
						ps.setString(5, sobj.getStaffId());
						ps.setString(6, sobj.getSeparationDate());
					} else if (reminderType.equalsIgnoreCase("Reminder1")) {
						ps.setString(2, "REMINDER1_SENT");
						ps.setString(3, sobj.getStaffId());
						ps.setString(4, sobj.getSeparationDate());
					} else if (reminderType.equalsIgnoreCase("Reminder2")) {
						ps.setString(2, "REMINDER2_SENT");
						ps.setString(3, sobj.getStaffId());
						ps.setString(4, sobj.getSeparationDate());
					} else {
						ps.setString(2, "WARNINGMAIL_SENT");
						ps.setString(3, sobj.getStaffId());
						ps.setString(4, sobj.getSeparationDate());
					}
				}

				@Override
				public int getBatchSize() {
					return staffsList.size();
				}
			});

			log.debug("Updating Separation table after sending mail completed");
		} catch (DataAccessException e) {
			log.error(e.getMessage());
			log.info(e);
			log.debug("Error occured at updateEmailSentItems method ");
			throw e;
		}

	}

	public HashMap<String, ArrayList<StaffVo>> getRemainderMailData() {
		try {
			log.debug("get Reminder Mail data");
			reminderHashMap = new HashMap<>();

			rowSet = getJdbcTemplate().queryForRowSet(
					"SELECT STAFF_NAME,STAFF_ID,PREV_DIVISION,CURR_DIVISION,DATE_FORMAT(ACTUAL_SEPARATION_DATE, '%d-%b-%Y %T') as TERMINATION_DATE,SEPARATION_STATUS FROM  MAS_IDM_USER_SEPARATION_LIST"
							+ " WHERE DATE_FORMAT(EMAIL_SENT_DATE,'%Y-%m-%d') between DATE_FORMAT(NOW() - INTERVAL 3 DAY,'%Y-%m-%d') AND DATE_FORMAT(NOW() - INTERVAL 2 DAY,'%Y-%m-%d') AND SEPARATION_STATUS='OPEN' AND REMINDER1_SENT_DATE IS NULL ");
			rowSet2 = getJdbcTemplate().queryForRowSet(
					"SELECT STAFF_NAME,STAFF_ID,PREV_DIVISION,CURR_DIVISION,DATE_FORMAT(ACTUAL_SEPARATION_DATE, '%d-%b-%Y %T') as TERMINATION_DATE,SEPARATION_STATUS FROM  MAS_IDM_USER_SEPARATION_LIST"
							+ " WHERE DATE_FORMAT(EMAIL_SENT_DATE,'%Y-%m-%d') between DATE_FORMAT(NOW() - INTERVAL 5 DAY,'%Y-%m-%d') AND DATE_FORMAT(NOW() - INTERVAL 4 DAY,'%Y-%m-%d') AND SEPARATION_STATUS='OPEN' AND REMINDER2_SENT_DATE IS NULL  ");
			rowSet3 = getJdbcTemplate().queryForRowSet(
					"SELECT STAFF_NAME,STAFF_ID,PREV_DIVISION,CURR_DIVISION,DATE_FORMAT(ACTUAL_SEPARATION_DATE, '%d-%b-%Y %T') as TERMINATION_DATE,SEPARATION_STATUS FROM  MAS_IDM_USER_SEPARATION_LIST"
							+ " WHERE DATE_FORMAT(EMAIL_SENT_DATE,'%Y-%m-%d') <= DATE_FORMAT(NOW() - INTERVAL 6 DAY,'%Y-%m-%d') AND SEPARATION_STATUS='OPEN' AND WARNING_MAIL_SENT_DATE IS NULL ");
			while (rowSet.next()) {

				staffobj = new StaffVo();
				if (reminderHashMap.containsKey("Reminder1")) {
					reminderProcessingList = reminderHashMap.get("Reminder1");
					staffobj.setStaffName(rowSet.getString("STAFF_NAME"));

					staffobj.setStaffId(rowSet.getString("STAFF_ID"));

					staffobj.setPrevDiv(rowSet.getString("PREV_DIVISION"));

					staffobj.setCurrDiv(rowSet.getString("CURR_DIVISION"));

					staffobj.setSeparationDate(rowSet.getString("TERMINATION_DATE"));

					staffobj.setSeparationStatus(rowSet.getString("SEPARATION_STATUS"));

					reminderProcessingList.add(staffobj);
					reminderHashMap.put("Reminder1", reminderProcessingList);
				} else {
					reminderProcessingList = new ArrayList<>();
					staffobj.setStaffName(rowSet.getString("STAFF_NAME"));
					staffobj.setStaffId(rowSet.getString("STAFF_ID"));
					staffobj.setPrevDiv(rowSet.getString("PREV_DIVISION"));
					staffobj.setCurrDiv(rowSet.getString("CURR_DIVISION"));
					staffobj.setSeparationDate(rowSet.getString("TERMINATION_DATE"));
					staffobj.setSeparationStatus(rowSet.getString("SEPARATION_STATUS"));

					reminderProcessingList.add(staffobj);
					reminderHashMap.put("Reminder1", reminderProcessingList);
				}
			}

			while (rowSet2.next()) {

				staffobj = new StaffVo();
				if (reminderHashMap.containsKey("Reminder2")) {
					reminderProcessingList = reminderHashMap.get("Reminder2");
					staffobj.setStaffName(rowSet2.getString("STAFF_NAME"));

					staffobj.setStaffId(rowSet2.getString("STAFF_ID"));

					staffobj.setPrevDiv(rowSet2.getString("PREV_DIVISION"));

					staffobj.setCurrDiv(rowSet2.getString("CURR_DIVISION"));

					staffobj.setSeparationDate(rowSet2.getString("TERMINATION_DATE"));

					staffobj.setSeparationStatus(rowSet2.getString("SEPARATION_STATUS"));

					reminderProcessingList.add(staffobj);
					reminderHashMap.put("Reminder2", reminderProcessingList);
				} else {
					reminderProcessingList = new ArrayList<>();
					staffobj.setStaffName(rowSet2.getString("STAFF_NAME"));
					staffobj.setStaffId(rowSet2.getString("STAFF_ID"));
					staffobj.setPrevDiv(rowSet2.getString("PREV_DIVISION"));
					staffobj.setCurrDiv(rowSet2.getString("CURR_DIVISION"));
					staffobj.setSeparationDate(rowSet2.getString("TERMINATION_DATE"));
					staffobj.setSeparationStatus(rowSet2.getString("SEPARATION_STATUS"));

					reminderProcessingList.add(staffobj);
					reminderHashMap.put("Reminder2", reminderProcessingList);
				}
			}

			while (rowSet3.next()) {

				staffobj = new StaffVo();
				if (reminderHashMap.containsKey("Warning")) {
					reminderProcessingList = reminderHashMap.get("Warning");
					staffobj.setStaffName(rowSet3.getString("STAFF_NAME"));

					staffobj.setStaffId(rowSet3.getString("STAFF_ID"));

					staffobj.setPrevDiv(rowSet3.getString("PREV_DIVISION"));
					staffobj.setCurrDiv(rowSet3.getString("CURR_DIVISION"));

					staffobj.setSeparationDate(rowSet3.getString("TERMINATION_DATE"));

					staffobj.setSeparationStatus(rowSet3.getString("SEPARATION_STATUS"));

					reminderProcessingList.add(staffobj);
					reminderHashMap.put("Warning", reminderProcessingList);
				} else {
					reminderProcessingList = new ArrayList<>();

					staffobj.setStaffName(rowSet3.getString("STAFF_NAME"));
					staffobj.setStaffId(rowSet3.getString("STAFF_ID"));
					staffobj.setPrevDiv(rowSet3.getString("PREV_DIVISION"));
					staffobj.setCurrDiv(rowSet3.getString("CURR_DIVISION"));
					staffobj.setSeparationDate(rowSet3.getString("TERMINATION_DATE"));
					staffobj.setSeparationStatus(rowSet3.getString("SEPARATION_STATUS"));

					reminderProcessingList.add(staffobj);
					reminderHashMap.put("Warning", reminderProcessingList);
				}
			}

			log.debug("Reminder data fetched:" + reminderHashMap.toString());

			return reminderHashMap;
		} catch (InvalidResultSetAccessException e) {
			log.error(e.getMessage());
			log.info(e);
			log.debug("Database Connection could not be established / Resultset fetch error");
			throw e;
		} catch (DataAccessException e) {
			log.error(e.getMessage());
			log.info(e);

			log.debug("Database Connection could not be established");
			throw e;
		}
	}

	public ArrayList<StaffVo> getUpdationList() {
		try {
			log.debug("get Updation Mail data");
			updationProcessingList = new ArrayList<>();

			rowSet = getJdbcTemplate().queryForRowSet(
					"SELECT STAFF_NAME,STAFF_ID,PREV_DIVISION,CURR_DIVISION,DATE_FORMAT(ACTUAL_SEPARATION_DATE, '%d-%b-%Y %T') as TERMINATION_DATE, "
							+ "SEPARATION_STATUS FROM  MAS_IDM_USER_SEPARATION_LIST"
							+ " WHERE SEPARATION_STATUS='OPEN' AND (EMAIL_SENT_DATE < DATE_FORMAT(NOW(),'%Y-%m-%d %T') OR EMAIL_SENT_DATE is NULL)");

			while (rowSet.next()) {
				staffobj = new StaffVo();
				staffobj.setStaffName(rowSet.getString("STAFF_NAME"));
				staffobj.setStaffId(rowSet.getString("STAFF_ID"));
				staffobj.setPrevDiv(rowSet.getString("PREV_DIVISION"));
				staffobj.setCurrDiv(rowSet.getString("CURR_DIVISION"));
				staffobj.setSeparationDate(rowSet.getString("TERMINATION_DATE"));
				staffobj.setSeparationStatus(rowSet.getString("SEPARATION_STATUS"));
				updationProcessingList.add(staffobj);
			}

			log.debug("Records to be checked for separation status:" + updationProcessingList.toString());
			return updationProcessingList;
		} catch (InvalidResultSetAccessException e) {
			log.error(e.getMessage());
			log.info(e);
			log.debug("Database Connection could not be established / Resultset fetch error - getUpdationList");
			throw e;
		} catch (DataAccessException e) {
			log.error(e.getMessage());
			log.info(e);

			log.debug("Database Connection could not be established  - getUpdationList");
			throw e;
		}

	}

	public void updateSeparationClosedItems(final ArrayList<StaffVo> updationStaffList) {
		log.debug("     || Updating Separation closed items started               ||");
		try {
			String sql = "UPDATE MAS_IDM_USER_SEPARATION_LIST "
					+ " SET SEPARATION_STATUS=?, LAST_UPDATED_DATE = DATE_FORMAT(NOW(),'%Y-%m-%d %T')"
					+ "WHERE STAFF_ID=? AND ACTUAL_SEPARATION_DATE = str_to_date(?,'%d-%b-%Y %T') ";
			getJdbcTemplate().batchUpdate(sql, new BatchPreparedStatementSetter() {

				@Override
				public void setValues(PreparedStatement ps, int i) throws SQLException {
					StaffVo sobj = updationStaffList.get(i);
					ps.setString(1, "CLOSED");
					ps.setString(2, sobj.getStaffId());
					ps.setString(3, sobj.getSeparationDate());

				}

				@Override
				public int getBatchSize() {
					return updationStaffList.size();
				}
			});

			log.debug("         || Updating Separation table for closed requests completed || ");
		} catch (DataAccessException e) {
			log.error(e.getMessage());
			log.debug("Updating Separation table for closed requests FAILED due to ERROR ");
			log.info(e);
		}

	}

	public void updateForNoAppUsers(String staffId, String separationDate) {

		try {
			getJdbcTemplate().update(
					"UPDATE MAS_IDM_USER_SEPARATION_LIST SET SEPARATION_STATUS='CLOSED', AD_UPDATE='UPDATED', VPN_UPDATE='UPDATED', "
							+ "LAST_UPDATED_DATE = DATE_FORMAT(NOW(),'%Y-%m-%d %T')"
							+ " WHERE ACTUAL_SEPARATION_DATE = str_to_date(?,'%d-%b-%Y %T')  AND STAFF_ID='" + staffId
							+ "'",
					separationDate);
		} catch (DataAccessException e) {
			log.error(e.getMessage());
			log.debug("Database Connection error in updateForNoAppUsers method");

			log.error(e);
		}
	}

	public void createEntryInSeparationList(ApplicationUsersVo appusersobj) {
		try {

			getJdbcTemplate().update("INSERT INTO MAS_IDM_USER_SEPARATION_LIST"
					+ "(ID,STAFF_ID, STAFF_NAME, ACTUAL_SEPARATION_DATE,SEPARATION_STATUS,EMAIL_STATUS,CREATED_DATE,AD_UPDATE,VPN_UPDATE) "
					+ "VALUES (default,?,?,str_to_date(?,'%d-%b-%Y %T'),?,?,DATE_FORMAT(NOW(),'%Y-%m-%d %T'),?,?)",
					appusersobj.getStaffId(), appusersobj.getStaffName(), appusersobj.getSeparationDate(),
					"NOT_STARTED", "NOT_SENT", "NOT_STARTED", "NOT_STARTED");

		} catch (DataAccessException e) {
			log.error(e.getMessage());
			log.debug("Database Connection error in updateForNoAppUsers method");
			log.info(e);
		}
	}

//NOT IN USE : STARTS
	public ArrayList<String> getNCUsers_OLD() {
		try {
			log.debug("get Users whose separation is open");
			ncProcessingList = new ArrayList<>();
			rowSet = getJdbcTemplate().queryForRowSet(
					"SELECT STAFF_NAME,STAFF_ID,PREV_DIVISION,CURR_DIVISION,to_char(ACTUAL_SEPARATION_DATE, 'DD-MON-YYYY HH24:MI:SS') as TERMINATION_DATE,SEPARATION_STATUS FROM  MAS_IDM_USER_SEPARATION_LIST"
							+ " WHERE SEPARATION_STATUS='OPEN' AND EMAIL_SENT_DATE<trunc(sysdate)");

			while (rowSet.next()) {
				staffobj = new StaffVo();
				ncProcessingList.add(rowSet.getString("STAFF_ID"));
			}
			log.debug("Non-compliance List:" + ncProcessingList.toString());
			return ncProcessingList;
		} catch (Exception e) {
			log.error(e.getMessage());
			log.debug("Database Error occured at getNCUsers method");

			log.info(e);
			return null;
		}
	}
	// NOT IN USE :ENDS

	// ADDED FOR ENHANCEMENT :END
	public ArrayList<StaffVo> getNCUsers() throws IDMDBException, IDMPulseraException, IDMException {
		try {
			log.debug("Fetching SEPERATION Staff's Status as OPEN .. ");
			ArrayList<StaffVo> ncStaffList = new ArrayList<StaffVo>();
			StaffVo staffVo;
			SqlRowSet rsSeperationStaffs;
			rsSeperationStaffs = getJdbcTemplate().queryForRowSet(
					"SELECT ID,STAFF_NAME,STAFF_ID,ACTUAL_SEPARATION_DATE,SEPARATION_STATUS,WARNING_MAIL_SENT_DATE FROM MAS_IDM_USER_SEPARATION_LIST where (SEPARATION_STATUS = ? OR SEPARATION_STATUS = ? ) AND EMAIL_STATUS = ? AND WARNING_MAIL_SENT_DATE IS NOT NULL ",
					"OPEN", "NOT_STARTED", "WARNINGMAIL_SENT");
			while (rsSeperationStaffs.next()) {
				staffVo = new StaffVo();
				if (rsSeperationStaffs.getString("STAFF_ID") != null) {
					staffVo.setStaffId(rsSeperationStaffs.getString("STAFF_ID"));
				} else {
					staffVo.setStaffId("");
				}
				if (rsSeperationStaffs.getString("STAFF_NAME") != null) {
					staffVo.setStaffName(rsSeperationStaffs.getString("STAFF_NAME"));
				} else {
					staffVo.setStaffName("");
				}
				if (rsSeperationStaffs.getDate("ACTUAL_SEPARATION_DATE") != null) {
					staffVo.setSeparationDate(rsSeperationStaffs.getDate("ACTUAL_SEPARATION_DATE").toString());
				} else {
					staffVo.setSeparationDate("");
				}
				if (rsSeperationStaffs.getString("SEPARATION_STATUS") != null) {
					staffVo.setSeparationStatus(rsSeperationStaffs.getString("SEPARATION_STATUS"));
				} else {
					staffVo.setSeparationStatus("");
				}
				if (rsSeperationStaffs.getDate("WARNING_MAIL_SENT_DATE") != null) {
					staffVo.setWarningMailSentDate(rsSeperationStaffs.getDate("WARNING_MAIL_SENT_DATE").toString());
				} else {
					staffVo.setWarningMailSentDate("");
				}
				ncStaffList.add(staffVo);
			}
			log.debug("Non-compliance List:" + ncStaffList.toString());
			return ncStaffList;
		} catch (DataAccessException dataAccessException) {
			log.info(dataAccessException);
			throw new IDMPulseraException(ErrorCodeEnums.DB_RUNTIME_ERROR.getErrorCode(), dataAccessException);
		} catch (Exception exception) {
			log.info(exception);
			throw new IDMException(ErrorCodeEnums.INTERNAL_SERVER_ERROR.getErrorCode(), exception);
		}

	}
	// ADDED FOR ENHANCEMENT :ENDS

	/**
	 * @return
	 * @throws SQLException
	 */
	public HashMap<String, String> getDivisionInfo() throws IDMPulseraException, IDMException {
		log.debug("Fetching Division Info from Pulsera system..");
		HashMap<String, String> deptInfo = new HashMap<>();
		try {
			rowSet = getJdbcTemplate().queryForRowSet("SELECT * FROM MAS_DIV_DEPT_VIEW WHERE TYPE='DIV'");
			while (rowSet.next()) {
				deptInfo.put(rowSet.getString("ORGANIZATION_ID"), rowSet.getString("ORGANIZATION_NAME"));
			}
		} catch (DataAccessException dataAccessException) {
			log.info(dataAccessException);
			throw new IDMPulseraException(ErrorCodeEnums.DB_RUNTIME_ERROR.getErrorCode(), dataAccessException);
		} catch (Exception exception) {
			log.info(exception);
			throw new IDMException(ErrorCodeEnums.INTERNAL_SERVER_ERROR.getErrorCode(), exception);
		}
		log.debug("Fetching Division Info from Pulsera system completed.." + deptInfo.size());
		return deptInfo;
	}

	public String getDivision(String departmentId) {
		String divId = new String();
		try {
			rowSet = getJdbcTemplate().queryForRowSet("select apps.masper_fnd_div_org.get_div(?) as divId from dual",
					departmentId);
			while (rowSet.next()) {
				divId = rowSet.getString("divId");
				// deptInfo.put(rowSet.getString("ORGANIZATION_ID"),
				// rowSet.getString("ORGANIZATION_NAME"));

			}

			return divId;
		} catch (InvalidResultSetAccessException e) {
			log.error(e.getMessage());
			log.info(e);
			return null;
		} catch (DataAccessException e) {
			log.error(e.getMessage());

			log.info(e);
			return null;
		}
	}

	public ArrayList<String> getUsersForReport() {
		ArrayList<String> staffIdLst = new ArrayList<String>();
		try {
			rowSet = getJdbcTemplate().queryForRowSet(
					"SELECT STAFF_ID FROM  MAS_IDM_USER_SEPARATION_LIST" + " WHERE SEPARATION_STATUS='OPEN'");
			while (rowSet.next()) {
				staffIdLst.add(rowSet.getString("STAFF_ID"));
			}
			return staffIdLst;
		} catch (InvalidResultSetAccessException invalidResultSetAccessException) {
			log.error(invalidResultSetAccessException.getMessage());
			log.info(invalidResultSetAccessException);
			return null;
		} catch (DataAccessException dataAccessExceptione) {
			log.error(dataAccessExceptione.getMessage());
			log.info(dataAccessExceptione);
			return null;
		}

	}

	public String getDivisionId(String prevDiv) {
		log.debug("     || Fetching Division Id started               ||");
		HashMap<String, ArrayList<StaffVo>> deptInfo;
		String divId = null;
		rowSet = getJdbcTemplate()
				.queryForRowSet("SELECT * FROM MAS_DIV_DEPT_VIEW WHERE TYPE='DIV' AND ORGANIZATION_NAME=?", prevDiv);
		while (rowSet.next()) {
			divId = rowSet.getString("ORGANIZATION_ID");

		}
		log.debug("     || Updating closed items completed               ||");
		return divId;
	}

	public Map<String, String> getAllDivision() {

		log.debug("     || Fetching Division Id started               ||");

		Map<String, String> divMap = new LinkedHashMap<String, String>();

		rowSet = getJdbcTemplate().queryForRowSet("SELECT * FROM MAS_DIV_DEPT_VIEW WHERE TYPE='DIV' ");
		while (rowSet.next()) {

			String orgName = rowSet.getString("ORGANIZATION_NAME");
			String orgId = rowSet.getString("ORGANIZATION_ID");
			divMap.put(orgName, orgId);
		}

		return divMap;
	}

	public void getNewJoineeInfo() {
		log.debug("             || Fetching New joinee Info started            ||");
		ArrayList<StaffVo> staffIdLst = new ArrayList<StaffVo>();
		MDC.put("LOGID", "IDVS System");
		try {

			rowSet = getJdbcTemplate().queryForRowSet(
					"SELECT STAFF_ID,STAFF_NAME,DATE_FORMAT(JOINING_DATE, '%d-%b-%Y %T') as JOINING_DATE FROM  MAS_IDM_NEW_USER_INFO"
							+ " WHERE STATUS = 'NOT_STARTED'");

			while (rowSet.next()) {
				staffobj = new StaffVo();
				staffobj.setStaffId(rowSet.getString("STAFF_ID"));
				staffobj.setStaffName(rowSet.getString("STAFF_NAME"));
				staffobj.setJoiningDate(rowSet.getString("JOINING_DATE"));
				staffIdLst.add(staffobj);
			}

		} catch (InvalidResultSetAccessException e) {
			log.error(e.getMessage());
			log.info(e);

		} catch (DataAccessException e) {
			log.error(e.getMessage());
			log.info(e);

		}
		log.debug("             || Fetching New joinee info completed          || ");

		try {
			srasdao.insertNewJoineeToPulsera(staffIdLst);

		} catch (Exception e) {
			log.error(e.getMessage());

			log.info(e);
		}

	}

	public void updateNewJoineeStatus(final List<StaffVo> staffIdLst) {
		log.debug("             || Updating New joinee Info in Pulsera started   || ");
		MDC.put("LOGID", "IDVS System");
		try {
			String sql = "UPDATE MAS_IDM_NEW_USER_INFO "
					+ " SET STATUS=? WHERE STAFF_ID=? AND JOINING_DATE = str_to_date(?,'%d-%b-%Y %T') ";
			getJdbcTemplate().batchUpdate(sql, new BatchPreparedStatementSetter() {

				@Override
				public void setValues(PreparedStatement ps, int i) throws SQLException {
					StaffVo sobj = staffIdLst.get(i);
					ps.setString(1, "STARTED");
					ps.setString(2, sobj.getStaffId());
					ps.setString(3, sobj.getJoiningDate());

				}

				@Override
				public int getBatchSize() {
					return staffIdLst.size();
				}
			});

		} catch (DataAccessException e) {
			log.error(e.getMessage());
			log.debug(
					"Updating Separation table after inserting staffs into Pulsera App in IDVS is FAILED due to ERROR ");
			log.info(e);
		}
		log.debug("Updating new staffs into Pulsera App in IDVS is completed     || ");

	}

	public ArrayList<StaffVo> getInactiveStaffForADVPN() throws IDMPulseraException, IDMException {
		log.debug("Fetching Inactive staffIds for AD & VPN started");
		List<StaffVo> staffDataList = new ArrayList<StaffVo>();
		try {
			rowSet = getJdbcTemplate().queryForRowSet("SELECT DISTINCT STAFF_ID,STAFF_NAME "
					+ "FROM MAS_IDM_USER_SEPARATION_LIST " + "WHERE (DATE_FORMAT(CREATED_DATE,'%Y-%m-%d') "
					+ "BETWEEN DATE_FORMAT(NOW() - INTERVAL 1 MONTH,'%Y-%m-%d') AND DATE_FORMAT(LAST_DAY(NOW() - INTERVAL 1 MONTH), '%Y-%m-%d')) "
					+ "AND (SEPARATION_STATUS IN ('OPEN','CLOSED')) " + "AND (TRIM(PREV_DIVISION) IS NULL) "
					+ "AND (TRIM(CURR_DIVISION) IS NULL) ORDER BY STAFF_ID");
			while (rowSet.next()) {
				StaffVo staffVo = new StaffVo();
				staffVo.setStaffId(rowSet.getString("STAFF_ID"));
				staffVo.setStaffName(rowSet.getString("STAFF_NAME"));
				staffVo.setADStatus("---");
				staffVo.setVPNStatus("---");
				staffDataList.add(staffVo);
			}
		} catch (DataAccessException dataAccessException) {
			log.info(dataAccessException);
			throw new IDMPulseraException(ErrorCodeEnums.DB_RUNTIME_ERROR.getErrorCode(), dataAccessException);
		} catch (Exception exception) {
			log.info(exception);
			throw new IDMException(ErrorCodeEnums.INTERNAL_SERVER_ERROR.getErrorCode(), exception);
		}
		log.debug("Inactive staffIds for AD & VPN.." + staffDataList.size());
		return (ArrayList<StaffVo>) staffDataList;
	}

	// SF INTEGRATION COMMENT : ENDs

	/* CHG0010178 */

	public List<StaffVo> getNewHireData() {

		log.info("Fetching new and rehire details to send mail");
		List<StaffVo> staffDataList = new ArrayList<StaffVo>();
		try {

			rowSet2 = getJdbcTemplate().queryForRowSet(
					"SELECT Full_NAME,FIRST_NAME,LAST_NAME,EMPLOYEE_NUMBER,DATE_FORMAT(DATE_START,'%d-%m-%Y') as DATE_START,USER_PERSON_TYPE,"
							+ "LOCATION,DIVISION,SECTION_NAME,DEPARTMENT,DESIGNATION,JOB_GRADE_ID,EMPLOYEE_CATEGORY,LEGAL_ENTITY,RC,"
							+ " IMMEDIATE_SUPERVISOR_ID,PRIMARY_PHONE_NO,SECONDARY_PHONE_NO,BUSINESS_EMAIL_ADDRESS,GENDER,NATIONAL_ID,DATE_FORMAT(DATE_OF_BIRTH,'%d-%m-%Y') as DATE_OF_BIRTH,NATIONALITY,PLACE_OF_BIRTH ,ADDRESS_1,ADDRESS_2,POSTCODE,CITY FROM IDVS_MASTER_TABLE "
							+ "WHERE DATE_FORMAT(DATE_START,'%Y-%m-%d')>=DATE_FORMAT(NOW(),'%Y-%m-%d') AND PROCESS_TYPE='NEW_PROFILE' AND IS_PROCESSED='YES' AND DATE_FORMAT(MODIFIED_DATE,'%Y-%m-%d')=DATE_FORMAT(NOW(),'%Y-%m-%d')");
			while (rowSet2.next()) {
				staffobj = new StaffVo();
				staffobj.setFullName(rowSet2.getString("Full_NAME"));
				staffobj.setFirstName(rowSet2.getString("FIRST_NAME"));
				staffobj.setStaffName(rowSet2.getString("LAST_NAME"));
				staffobj.setStaffId(rowSet2.getString("EMPLOYEE_NUMBER"));

				staffobj.setJoiningDate(rowSet2.getString("DATE_START"));

				staffobj.setEmploymentType(rowSet2.getString("USER_PERSON_TYPE"));
				staffobj.setLocationCode(rowSet2.getString("LOCATION"));
				staffobj.setDivisionName(rowSet2.getString("DIVISION"));
				staffobj.setCurrentUnit(rowSet2.getString("SECTION_NAME"));
				staffobj.setDepartmentName(rowSet2.getString("DEPARTMENT"));
				staffobj.setDesignation(rowSet2.getString("DESIGNATION"));
				staffobj.setJobGrade(rowSet2.getString("JOB_GRADE_ID"));
				staffobj.setEmployeeCategory(rowSet2.getString("EMPLOYEE_CATEGORY"));
				staffobj.setLegalEntity(rowSet2.getString("LEGAL_ENTITY"));
				staffobj.setCostCenter(rowSet2.getString("RC"));
				staffobj.setImmediateSupervisor(rowSet2.getString("IMMEDIATE_SUPERVISOR_ID"));
				staffobj.setPhoneNO(rowSet2.getString("PRIMARY_PHONE_NO"));
				staffobj.setSecondaryNO(rowSet2.getString("SECONDARY_PHONE_NO"));
				staffobj.setBusinessEmail(rowSet2.getString("BUSINESS_EMAIL_ADDRESS"));
				staffobj.setGender(rowSet2.getString("GENDER"));
				staffobj.setNationalID(rowSet2.getString("NATIONAL_ID"));
				staffobj.setDob(rowSet2.getString("DATE_OF_BIRTH"));
				staffobj.setNationality(rowSet2.getString("NATIONALITY"));
				staffobj.setPlaceOfBirth(rowSet2.getString("PLACE_OF_BIRTH"));
				staffobj.setAddress1(rowSet2.getString("ADDRESS_1"));
				staffobj.setAddress2(rowSet2.getString("ADDRESS_2"));
				staffobj.setPostcode(rowSet2.getString("POSTCODE"));
				staffobj.setCity(rowSet2.getString("CITY"));

				staffDataList.add(staffobj);

			}
			log.debug("         ||  Number of New Staff List || " + staffDataList.size());

		} catch (Exception msg) {
			log.info(msg);

		}
		return staffDataList;
	}

	public List<StaffVo> getMovementData() {

		log.info("Fetching movement details to send mail");
		List<StaffVo> movementDataList = new ArrayList<StaffVo>();
		try {

			rowSet1 = getJdbcTemplate().queryForRowSet(
					"SELECT Full_NAME,FIRST_NAME,LAST_NAME,EMPLOYEE_NUMBER,DATE_FORMAT(DATE_START,'%d-%m-%Y') as DATE_START,USER_PERSON_TYPE,"
							+ "LOCATION,DIVISION,SECTION_NAME,DEPARTMENT,DESIGNATION,JOB_GRADE_ID,EMPLOYEE_CATEGORY,LEGAL_ENTITY,RC,"
							+ " IMMEDIATE_SUPERVISOR_ID,PRIMARY_PHONE_NO,SECONDARY_PHONE_NO,BUSINESS_EMAIL_ADDRESS,GENDER,NATIONAL_ID,DATE_FORMAT(DATE_OF_BIRTH,'%d-%m-%Y') as DATE_OF_BIRTH,NATIONALITY,PLACE_OF_BIRTH ,ADDRESS_1,ADDRESS_2,POSTCODE,CITY FROM IDVS_MASTER_TABLE "
							+ "WHERE DATE_FORMAT(DATE_START,'%Y-%m-%d')<=DATE_FORMAT(NOW(),'%Y-%m-%d') AND PROCESS_TYPE='PROFILE_UPDATE' AND IS_PROCESSED='YES' AND DATE_FORMAT(MODIFIED_DATE,'%Y-%m-%d')=DATE_FORMAT(NOW(),'%Y-%m-%d')");

			while (rowSet1.next()) {
				staffobj = new StaffVo();
				staffobj.setFullName(rowSet1.getString("Full_NAME"));
				staffobj.setFirstName(rowSet1.getString("FIRST_NAME"));
				staffobj.setStaffName(rowSet1.getString("LAST_NAME"));
				staffobj.setStaffId(rowSet1.getString("EMPLOYEE_NUMBER"));
				staffobj.setJoiningDate(rowSet1.getString("DATE_START"));
				// log.info(staffobj.getJoiningDate());
				staffobj.setEmploymentType(rowSet1.getString("USER_PERSON_TYPE"));
				staffobj.setLocationCode(rowSet1.getString("LOCATION"));
				staffobj.setDivisionName(rowSet1.getString("DIVISION"));
				staffobj.setCurrentUnit(rowSet1.getString("SECTION_NAME"));
				staffobj.setDepartmentName(rowSet1.getString("DEPARTMENT"));
				staffobj.setDesignation(rowSet1.getString("DESIGNATION"));
				staffobj.setJobGrade(rowSet1.getString("JOB_GRADE_ID"));
				staffobj.setEmployeeCategory(rowSet1.getString("EMPLOYEE_CATEGORY"));
				staffobj.setLegalEntity(rowSet1.getString("LEGAL_ENTITY"));
				staffobj.setCostCenter(rowSet1.getString("RC"));
				staffobj.setImmediateSupervisor(rowSet1.getString("IMMEDIATE_SUPERVISOR_ID"));
				staffobj.setPhoneNO(rowSet1.getString("PRIMARY_PHONE_NO"));
				staffobj.setSecondaryNO(rowSet1.getString("SECONDARY_PHONE_NO"));
				staffobj.setBusinessEmail(rowSet1.getString("BUSINESS_EMAIL_ADDRESS"));
				staffobj.setGender(rowSet1.getString("GENDER"));
				staffobj.setNationalID(rowSet1.getString("NATIONAL_ID"));
				staffobj.setDob(rowSet1.getString("DATE_OF_BIRTH"));
				staffobj.setNationality(rowSet1.getString("NATIONALITY"));
				staffobj.setPlaceOfBirth(rowSet1.getString("PLACE_OF_BIRTH"));
				staffobj.setAddress1(rowSet1.getString("ADDRESS_1"));
				staffobj.setAddress2(rowSet1.getString("ADDRESS_2"));
				staffobj.setPostcode(rowSet1.getString("POSTCODE"));
				staffobj.setCity(rowSet1.getString("CITY"));

				movementDataList.add(staffobj);
			}

			log.debug("         ||  Number of Movement data || " + movementDataList.size());
			log.debug("         ||  MovementList List       || " + movementDataList.toString());

		} catch (Exception msg) {
			log.info(msg);
		}
		return movementDataList;

	}

	public List<StaffVo> getOffBoardingData() {

		log.info("Fetching separation details to send mail");
		List<StaffVo> separationDataList = new ArrayList<StaffVo>();
		try {

			rowSet1 = getJdbcTemplate().queryForRowSet(
					"SELECT Full_NAME, EMPLOYEE_NUMBER,DATE_FORMAT(TERMINATION_DATE,'%d-%m-%Y') as ACTUAL_TERMINATION_DATE ,LAST_UPDATED_DATE FROM  IDVS_MASTER_TABLE WHERE DATE_FORMAT(TERMINATION_DATE,'%Y-%m-%d')<=DATE_FORMAT(NOW(),'%Y-%m-%d') AND PROCESS_TYPE='TERMINATION' AND IS_PROCESSED='YES' AND DATE_FORMAT(MODIFIED_DATE,'%Y-%m-%d')=DATE_FORMAT(NOW(),'%Y-%m-%d')");
			while (rowSet1.next()) {
				staffobj = new StaffVo();
				staffobj.setStaffName(rowSet1.getString("Full_NAME"));
				staffobj.setStaffId(rowSet1.getString("EMPLOYEE_NUMBER"));
				staffobj.setSeparationDate(rowSet1.getString("ACTUAL_TERMINATION_DATE"));
				staffobj.setLastUpdatedDate(rowSet1.getDate("LAST_UPDATED_DATE"));

				separationDataList.add(staffobj);
			}

			log.debug("         ||  Number of Separation data || " + separationDataList.size());
			log.debug("         ||  SeparationList List       || " + separationDataList.toString());

		} catch (Exception msg) {
			log.info(msg);
		}
		return separationDataList;

	}

	public void updateStatusInMaster(String status, String staffId, String processed) {
		DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		Calendar cal = Calendar.getInstance();
		String todaysDate = formatter.format(cal.getTime());
		java.util.Date date;
		try {
			date = formatter.parse(todaysDate);

			java.sql.Date modifiedDate = new java.sql.Date(date.getTime());
			log.info("Inside Status updation of user" + staffId + "into IDVS MASTER TABLE");
			getJdbcTemplate().update(
					"UPDATE IDVS_MASTER_TABLE SET STATUS=?,IS_PROCESSED=?,MODIFIED_DATE=? WHERE EMPLOYEE_NUMBER = ? AND LEGAL_ENTITY !='FY'",
					status, processed, modifiedDate, staffId);

			log.info("Status for terminated staff: " + staffId + " updated in MASTER table");
		} catch (Exception e) {
			log.error(e);
		}
	}
	
	
	public void updateStatusInFYMaster(String status, String staffId, String processed) {
		DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		Calendar cal = Calendar.getInstance();
		String todaysDate = formatter.format(cal.getTime());
		java.util.Date date;
		try {
			date = formatter.parse(todaysDate);

			java.sql.Date modifiedDate = new java.sql.Date(date.getTime());
			log.info("Inside Status updation of user" + staffId + "into IDVS MASTER TABLE");
			getJdbcTemplate().update(
					"UPDATE IDVS_MASTER_TABLE SET STATUS=?,IS_PROCESSED=?,MODIFIED_DATE=? WHERE EMPLOYEE_NUMBER = ? AND LEGAL_ENTITY ='FY'",
					status, processed, modifiedDate, staffId);

			log.info("Status for terminated staff: " + staffId + " updated in MASTER table");
		} catch (Exception e) {
			log.error(e);
		}
	}

	public List<StaffVo> getNewHireDataOnDateStart() {

		log.info("Fetching new and rehire details to send mail");
		List<StaffVo> staffDataList = new ArrayList<StaffVo>();
		try {

			rowSet2 = getJdbcTemplate().queryForRowSet(
					"SELECT Full_NAME,FIRST_NAME,LAST_NAME,EMPLOYEE_NUMBER,DATE_FORMAT(DATE_START,'%d-%m-%Y') as DATE_START,USER_PERSON_TYPE,"
							+ " LOCATION,DIVISION,SECTION_NAME,DEPARTMENT,DESIGNATION,JOB_GRADE_ID,EMPLOYEE_CATEGORY,LEGAL_ENTITY,RC,"
							+ " IMMEDIATE_SUPERVISOR_ID,PRIMARY_PHONE_NO,SECONDARY_PHONE_NO,BUSINESS_EMAIL_ADDRESS,GENDER,NATIONAL_ID,DATE_FORMAT(DATE_OF_BIRTH,'%d-%m-%Y') as DATE_OF_BIRTH,NATIONALITY,PLACE_OF_BIRTH ,ADDRESS_1,ADDRESS_2,POSTCODE,CITY FROM IDVS_MASTER_TABLE "
							+ " WHERE DATE_FORMAT(DATE_START,'%Y-%m-%d') = DATE_FORMAT(NOW(),'%Y-%m-%d') AND PROCESS_TYPE='NEW_PROFILE' AND IS_PROCESSED='YES'");

			while (rowSet2.next()) {
				staffobj = new StaffVo();
				staffobj.setFullName(rowSet2.getString("Full_NAME"));
				staffobj.setFirstName(rowSet2.getString("FIRST_NAME"));
				staffobj.setStaffName(rowSet2.getString("LAST_NAME"));
				staffobj.setStaffId(rowSet2.getString("EMPLOYEE_NUMBER"));

				staffobj.setJoiningDate(rowSet2.getString("DATE_START"));

				staffobj.setEmploymentType(rowSet2.getString("USER_PERSON_TYPE"));
				staffobj.setLocationCode(rowSet2.getString("LOCATION"));
				staffobj.setDivisionName(rowSet2.getString("DIVISION"));
				staffobj.setCurrentUnit(rowSet2.getString("SECTION_NAME"));
				staffobj.setDepartmentName(rowSet2.getString("DEPARTMENT"));
				staffobj.setDesignation(rowSet2.getString("DESIGNATION"));
				staffobj.setJobGrade(rowSet2.getString("JOB_GRADE_ID"));
				staffobj.setEmployeeCategory(rowSet2.getString("EMPLOYEE_CATEGORY"));
				staffobj.setLegalEntity(rowSet2.getString("LEGAL_ENTITY"));
				staffobj.setCostCenter(rowSet2.getString("RC"));
				staffobj.setImmediateSupervisor(rowSet2.getString("IMMEDIATE_SUPERVISOR_ID"));
				staffobj.setPhoneNO(rowSet2.getString("PRIMARY_PHONE_NO"));
				staffobj.setSecondaryNO(rowSet2.getString("SECONDARY_PHONE_NO"));
				staffobj.setBusinessEmail(rowSet2.getString("BUSINESS_EMAIL_ADDRESS"));
				staffobj.setGender(rowSet2.getString("GENDER"));
				staffobj.setNationalID(rowSet2.getString("NATIONAL_ID"));
				staffobj.setDob(rowSet2.getString("DATE_OF_BIRTH"));
				staffobj.setNationality(rowSet2.getString("NATIONALITY"));
				staffobj.setPlaceOfBirth(rowSet2.getString("PLACE_OF_BIRTH"));
				staffobj.setAddress1(rowSet2.getString("ADDRESS_1"));
				staffobj.setAddress2(rowSet2.getString("ADDRESS_2"));
				staffobj.setPostcode(rowSet2.getString("POSTCODE"));
				staffobj.setCity(rowSet2.getString("CITY"));

				staffDataList.add(staffobj);

			}
			log.debug("         ||  DOJ Number of New Staff List || " + staffDataList.size() + " ," + staffDataList);

		} catch (Exception msg) {
			log.info(msg);

		}
		return staffDataList;
	}

}
