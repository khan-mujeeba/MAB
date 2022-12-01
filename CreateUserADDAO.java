/**
 * 
 */
package com.mas.idm.dao;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.InvalidResultSetAccessException;
import org.springframework.jdbc.core.support.JdbcDaoSupport;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.util.CollectionUtils;

import com.mas.idm.bean.ADStation;
import com.mas.idm.bean.ADUpdateVO;
import com.mas.idm.enums.ErrorCodeEnums;
import com.mas.idm.exception.IDMDBException;
import com.mas.idm.exception.IDMException;
import org.apache.logging.log4j.LogManager;import org.apache.logging.log4j.Logger;

/**
 * @author 879989
 *
 */
public class CreateUserADDAO extends JdbcDaoSupport {
	private static final Logger log = LogManager.getLogger(CreateUserADDAO.class);
	
	private ADUpdateVO newHireObj;
	private ADUpdateVO movementObj;
	private SqlRowSet rowSet;
	
	//private SqlRowSet rowSet1;
//	private SqlRowSet rowSet2;
//	private SqlRowSet MALAYSIA;
//	private SqlRowSet INTLW;
//	private SqlRowSet INTLE;
	

	public ArrayList<ADUpdateVO> getNewHireData() {

		log.info("Started Fetching Data from DB getNewHireData");
		try {
			 ArrayList<ADUpdateVO> 	newHireList = new ArrayList<>();

			log.info("          || New Hire and Rehire Table/View   || ");
			// Fetching records from MASTER table for the current joining date
			rowSet = getJdbcTemplate().queryForRowSet(
					"SELECT * FROM IDVS_MASTER_TABLE WHERE PROCESS_TYPE='NEW_PROFILE' AND IS_PROCESSED='NO' AND LEGAL_ENTITY !='FY'");
			//		"SELECT * FROM IDVS_MASTER_TABLE WHERE EMPLOYEE_NUMBER ='2354579'");

			// Fetching data for AD OU -- start
			Map<String, Set<String>> adOuDetailsMap = getADOUDetails();
			// Fetching data for AD OU -- End

			// fetching AD Station list- START
			List<ADStation> adStationList = getADStationList();

			// fetching AD station list-END

			while (rowSet.next()) {


				newHireObj = new ADUpdateVO();

				// nik
				/*
				 * MALAYSIA =
				 * getJdbcTemplate().queryForRowSet("select MALAYSIA from AD_OU_DETAILS WHERE '"
				 * + rowSet.getString("LOCATION") + "' IN (MALAYSIA)"); INTLW =
				 * getJdbcTemplate().queryForRowSet( "select INTLW from AD_OU_DETAILS WHERE '" +
				 * rowSet.getString("LOCATION") + "' IN (INTLW)"); if (MALAYSIA.next()) {
				 * newHireObj.setAdOU("MALAYSIA"); } else if (INTLW.next()) {
				 * newHireObj.setAdOU("INTLW"); } else { newHireObj.setAdOU("INTLE"); }
				 */

				// end

				// ADDRESS UPDATE Raji

				/*
				 * if (rowSet.getString("LOCATION").equalsIgnoreCase("KULAP")) {
				 * 
				 * String division = rowSet.getString("DIVISION").replaceAll("'", "''"); String
				 * department = rowSet.getString("DEPARTMENT").replaceAll("'", "''"); String
				 * query = ""; String queryForCount =
				 * "SELECT count(*) FROM AD_STATION_LIST WHERE DIVISION='" + division +
				 * "' AND DEPARTMENT='" + department + "' AND LOCATION_CODE='" +
				 * rowSet.getString("LOCATION") + "'";
				 * 
				 * int count1 = getJdbcTemplate().queryForInt(queryForCount); if (count1 == 0) {
				 * queryForCount = "SELECT count(*) FROM AD_STATION_LIST WHERE DIVISION='" +
				 * division + "' AND DEPARTMENT ='' AND LOCATION_CODE='" +
				 * rowSet.getString("LOCATION") + "'"; int count2 =
				 * getJdbcTemplate().queryForInt(queryForCount);
				 * 
				 * if (count2 == 0) { query =
				 * "SELECT STREET_ADDRESS,CITY,POSTAL_CODE,STATE,COUNTRY FROM AD_STATION_LIST WHERE DIVISION='' AND DEPARTMENT ='' AND LOCATION_CODE='"
				 * + rowSet.getString("LOCATION") + "'"; } else {
				 * 
				 * query =
				 * "SELECT STREET_ADDRESS,CITY,POSTAL_CODE,STATE,COUNTRY FROM AD_STATION_LIST WHERE DIVISION='"
				 * + division + "' AND DEPARTMENT ='' AND LOCATION_CODE='" +
				 * rowSet.getString("LOCATION") + "'"; } } else { query =
				 * "SELECT STREET_ADDRESS,POSTAL_CODE,CITY,STATE,COUNTRY FROM AD_STATION_LIST WHERE DIVISION='"
				 * + division + "' AND DEPARTMENT='" + department + "' AND LOCATION_CODE='" +
				 * rowSet.getString("LOCATION") + "'"; } SqlRowSet value =
				 * getJdbcTemplate().queryForRowSet(query); if (value.next()) {
				 * newHireObj.setStreetAddress(value.getString("STREET_ADDRESS"));
				 * 
				 * newHireObj.setState(value.getString("STATE"));
				 * newHireObj.setCity(value.getString("CITY"));
				 * 
				 * newHireObj.setPostalCode(value.getString("POSTAL_CODE"));
				 * 
				 * newHireObj.setCountry(value.getString("COUNTRY"));
				 * 
				 * } else { newHireObj.setStreetAddress("null"); newHireObj.setCity("null");
				 * newHireObj.setState("null"); newHireObj.setPostalCode("null");
				 * newHireObj.setCountry("null");
				 * 
				 * }
				 * 
				 * }
				 * 
				 * else { SqlRowSet value = getJdbcTemplate().queryForRowSet(
				 * "SELECT * FROM AD_STATION_LIST WHERE LOCATION_CODE='" +
				 * rowSet.getString("LOCATION") + "'"); if (value.next()) { if
				 * (value.getString("STREET_ADDRESS").equalsIgnoreCase(null)) {
				 * newHireObj.setStreetAddress("null"); } else {
				 * newHireObj.setStreetAddress(value.getString("STREET_ADDRESS")); }
				 * 
				 * if (value.getString("CITY").equalsIgnoreCase(null)) {
				 * newHireObj.setCity("null"); } else {
				 * newHireObj.setCity(value.getString("CITY")); } if
				 * (value.getString("STATE").equalsIgnoreCase(null)) {
				 * newHireObj.setState("null"); } else {
				 * newHireObj.setState(value.getString("STATE")); } if
				 * (value.getString("POSTAL_CODE").equalsIgnoreCase(null)) {
				 * newHireObj.setPostalCode("null"); } else {
				 * 
				 * newHireObj.setPostalCode(value.getString("POSTAL_CODE")); } if
				 * (value.getString("COUNTRY").equalsIgnoreCase(null)) {
				 * newHireObj.setCountry("null"); } else {
				 * newHireObj.setCountry(value.getString("COUNTRY")); }
				 * 
				 * } else { newHireObj.setStreetAddress("null"); newHireObj.setCity("null");
				 * newHireObj.setState("null"); newHireObj.setPostalCode("null");
				 * newHireObj.setCountry("null");
				 * 
				 * } }
				 */

				// END ADDRESS PART

				String master_loc = rowSet.getString("LOCATION").toString().trim();
				String master_division = rowSet.getString("DIVISION").toString().trim();
				String master_department = rowSet.getString("DEPARTMENT").toString().trim();

				newHireObj.setAdOU(getADOU(adOuDetailsMap, master_loc));

				/// AD STATION LIST-- START

				ADStation adStationTemp = getADStation(master_loc, master_division, master_department, adStationList);

				if (adStationTemp != null) {
					newHireObj.setCountry(adStationTemp.getCountry());
					newHireObj.setStreetAddress(adStationTemp.getStreetAddress());
					newHireObj.setPostalCode(adStationTemp.getPostalCode());
					newHireObj.setCity(adStationTemp.getCity());
					newHireObj.setState(adStationTemp.getState());
				}
				// AD STATION LIST-- END

				newHireObj.setStaffId(rowSet.getString("EMPLOYEE_NUMBER"));

				newHireObj.setFullName(rowSet.getString("Full_NAME"));

				newHireObj.setBusinessEmail(rowSet.getString("BUSINESS_EMAIL_ADDRESS"));

				newHireObj.setFirstName(rowSet.getString("FIRST_NAME"));

				newHireObj.setLastName(rowSet.getString("LAST_NAME"));
				newHireObj.setCostCenter(rowSet.getString("RC"));
				newHireObj.setDivisionName(rowSet.getString("DIVISION"));
				newHireObj.setDepartmentName(rowSet.getString("DEPARTMENT"));
				newHireObj.setDesignation(rowSet.getString("DESIGNATION"));
				newHireObj.setLegalEntity(rowSet.getString("LEGAL_ENTITY"));
				newHireObj.setJobGrade(rowSet.getString("JOB_GRADE_ID"));
				newHireObj.setPhoneNO(rowSet.getString("PRIMARY_PHONE_NO"));
				newHireObj.setSecondryPhoneNO(rowSet.getString("SECONDARY_PHONE_NO"));
				newHireObj.setSuperior(rowSet.getString("IMMEDIATE_SUPERVISOR_ID"));
				newHireObj.setEmployeeCategory(rowSet.getString("EMPLOYEE_CATEGORY"));
				newHireObj.setLocationCode(rowSet.getString("LOCATION"));
				newHireList.add(newHireObj);
			}
			log.info("Passing New and Rehire Data to AD" + newHireList.size() +" ,"+newHireList);
			return newHireList;
		} catch (InvalidResultSetAccessException e) {
			log.error(e.getMessage());
			log.info(e);
			throw e;
		} catch (DataAccessException e) {
			log.error(e.getMessage());
			log.info(e);
			throw e;
		}

	}
	
	// getting new hire data for FY-AD
	// @mujeeba.khan
	
	public ArrayList<ADUpdateVO> getFYNewHireData() {

		log.info("Started Fetching Data from DB getNewHireData");
		try {
			 ArrayList<ADUpdateVO> 	newHireList = new ArrayList<>();

			log.info("          || New Hire and Rehire Table/View   || ");
			// Fetching records from MASTER table for the current joining date
			rowSet = getJdbcTemplate().queryForRowSet(
				//	"SELECT * FROM IDVS_MASTER_TABLE WHERE PROCESS_TYPE='NEW_PROFILE' AND IS_PROCESSED='NO' AND LEGAL_ENTITY ='FY'");
					"SELECT * FROM IDVS_MASTER_TABLE WHERE EMPLOYEE_NUMBER ='2354579' AND LEGAL_ENTITY ='FY'");

			// Fetching data for AD OU -- start
			Map<String, Set<String>> adOuDetailsMap = getADOUDetails();
			// Fetching data for AD OU -- End

			// fetching AD Station list- START
			List<ADStation> adStationList = getADStationList();

			// fetching AD station list-END

			while (rowSet.next()) {


				newHireObj = new ADUpdateVO();

				// nik
				/*
				 * MALAYSIA =
				 * getJdbcTemplate().queryForRowSet("select MALAYSIA from AD_OU_DETAILS WHERE '"
				 * + rowSet.getString("LOCATION") + "' IN (MALAYSIA)"); INTLW =
				 * getJdbcTemplate().queryForRowSet( "select INTLW from AD_OU_DETAILS WHERE '" +
				 * rowSet.getString("LOCATION") + "' IN (INTLW)"); if (MALAYSIA.next()) {
				 * newHireObj.setAdOU("MALAYSIA"); } else if (INTLW.next()) {
				 * newHireObj.setAdOU("INTLW"); } else { newHireObj.setAdOU("INTLE"); }
				 */

				// end

				// ADDRESS UPDATE Raji

				/*
				 * if (rowSet.getString("LOCATION").equalsIgnoreCase("KULAP")) {
				 * 
				 * String division = rowSet.getString("DIVISION").replaceAll("'", "''"); String
				 * department = rowSet.getString("DEPARTMENT").replaceAll("'", "''"); String
				 * query = ""; String queryForCount =
				 * "SELECT count(*) FROM AD_STATION_LIST WHERE DIVISION='" + division +
				 * "' AND DEPARTMENT='" + department + "' AND LOCATION_CODE='" +
				 * rowSet.getString("LOCATION") + "'";
				 * 
				 * int count1 = getJdbcTemplate().queryForInt(queryForCount); if (count1 == 0) {
				 * queryForCount = "SELECT count(*) FROM AD_STATION_LIST WHERE DIVISION='" +
				 * division + "' AND DEPARTMENT ='' AND LOCATION_CODE='" +
				 * rowSet.getString("LOCATION") + "'"; int count2 =
				 * getJdbcTemplate().queryForInt(queryForCount);
				 * 
				 * if (count2 == 0) { query =
				 * "SELECT STREET_ADDRESS,CITY,POSTAL_CODE,STATE,COUNTRY FROM AD_STATION_LIST WHERE DIVISION='' AND DEPARTMENT ='' AND LOCATION_CODE='"
				 * + rowSet.getString("LOCATION") + "'"; } else {
				 * 
				 * query =
				 * "SELECT STREET_ADDRESS,CITY,POSTAL_CODE,STATE,COUNTRY FROM AD_STATION_LIST WHERE DIVISION='"
				 * + division + "' AND DEPARTMENT ='' AND LOCATION_CODE='" +
				 * rowSet.getString("LOCATION") + "'"; } } else { query =
				 * "SELECT STREET_ADDRESS,POSTAL_CODE,CITY,STATE,COUNTRY FROM AD_STATION_LIST WHERE DIVISION='"
				 * + division + "' AND DEPARTMENT='" + department + "' AND LOCATION_CODE='" +
				 * rowSet.getString("LOCATION") + "'"; } SqlRowSet value =
				 * getJdbcTemplate().queryForRowSet(query); if (value.next()) {
				 * newHireObj.setStreetAddress(value.getString("STREET_ADDRESS"));
				 * 
				 * newHireObj.setState(value.getString("STATE"));
				 * newHireObj.setCity(value.getString("CITY"));
				 * 
				 * newHireObj.setPostalCode(value.getString("POSTAL_CODE"));
				 * 
				 * newHireObj.setCountry(value.getString("COUNTRY"));
				 * 
				 * } else { newHireObj.setStreetAddress("null"); newHireObj.setCity("null");
				 * newHireObj.setState("null"); newHireObj.setPostalCode("null");
				 * newHireObj.setCountry("null");
				 * 
				 * }
				 * 
				 * }
				 * 
				 * else { SqlRowSet value = getJdbcTemplate().queryForRowSet(
				 * "SELECT * FROM AD_STATION_LIST WHERE LOCATION_CODE='" +
				 * rowSet.getString("LOCATION") + "'"); if (value.next()) { if
				 * (value.getString("STREET_ADDRESS").equalsIgnoreCase(null)) {
				 * newHireObj.setStreetAddress("null"); } else {
				 * newHireObj.setStreetAddress(value.getString("STREET_ADDRESS")); }
				 * 
				 * if (value.getString("CITY").equalsIgnoreCase(null)) {
				 * newHireObj.setCity("null"); } else {
				 * newHireObj.setCity(value.getString("CITY")); } if
				 * (value.getString("STATE").equalsIgnoreCase(null)) {
				 * newHireObj.setState("null"); } else {
				 * newHireObj.setState(value.getString("STATE")); } if
				 * (value.getString("POSTAL_CODE").equalsIgnoreCase(null)) {
				 * newHireObj.setPostalCode("null"); } else {
				 * 
				 * newHireObj.setPostalCode(value.getString("POSTAL_CODE")); } if
				 * (value.getString("COUNTRY").equalsIgnoreCase(null)) {
				 * newHireObj.setCountry("null"); } else {
				 * newHireObj.setCountry(value.getString("COUNTRY")); }
				 * 
				 * } else { newHireObj.setStreetAddress("null"); newHireObj.setCity("null");
				 * newHireObj.setState("null"); newHireObj.setPostalCode("null");
				 * newHireObj.setCountry("null");
				 * 
				 * } }
				 */

				// END ADDRESS PART

				String master_loc = rowSet.getString("LOCATION").toString().trim();
				String master_division = rowSet.getString("DIVISION").toString().trim();
				String master_department = rowSet.getString("DEPARTMENT").toString().trim();

				newHireObj.setAdOU(getADOU(adOuDetailsMap, master_loc));

				/// AD STATION LIST-- START

				ADStation adStationTemp = getADStation(master_loc, master_division, master_department, adStationList);

				if (adStationTemp != null) {
					newHireObj.setCountry(adStationTemp.getCountry());
					newHireObj.setStreetAddress(adStationTemp.getStreetAddress());
					newHireObj.setPostalCode(adStationTemp.getPostalCode());
					newHireObj.setCity(adStationTemp.getCity());
					newHireObj.setState(adStationTemp.getState());
				}
				// AD STATION LIST-- END

				newHireObj.setStaffId(rowSet.getString("EMPLOYEE_NUMBER"));

				newHireObj.setFullName(rowSet.getString("Full_NAME"));

				newHireObj.setBusinessEmail(rowSet.getString("BUSINESS_EMAIL_ADDRESS"));

				newHireObj.setFirstName(rowSet.getString("FIRST_NAME"));

				newHireObj.setLastName(rowSet.getString("LAST_NAME"));
				newHireObj.setCostCenter(rowSet.getString("RC"));
				newHireObj.setDivisionName(rowSet.getString("DIVISION"));
				newHireObj.setDepartmentName(rowSet.getString("DEPARTMENT"));
				newHireObj.setDesignation(rowSet.getString("DESIGNATION"));
				newHireObj.setLegalEntity(rowSet.getString("LEGAL_ENTITY"));
				newHireObj.setJobGrade(rowSet.getString("JOB_GRADE_ID"));
				newHireObj.setPhoneNO(rowSet.getString("PRIMARY_PHONE_NO"));
				newHireObj.setSecondryPhoneNO(rowSet.getString("SECONDARY_PHONE_NO"));
				newHireObj.setSuperior(rowSet.getString("IMMEDIATE_SUPERVISOR_ID"));
				newHireObj.setEmployeeCategory(rowSet.getString("EMPLOYEE_CATEGORY"));
				newHireObj.setLocationCode(rowSet.getString("LOCATION"));
				newHireList.add(newHireObj);
			}
			log.info("Passing New and Rehire Data to AD" + newHireList.size() +" ,"+newHireList);
			return newHireList;
		} catch (InvalidResultSetAccessException e) {
			log.error(e.getMessage());
			log.info(e);
			throw e;
		} catch (DataAccessException e) {
			log.error(e.getMessage());
			log.info(e);
			throw e;
		}

	}
	

	private String getADOU(Map<String, Set<String>> adOuDetailsMap, String master_loc) {

		Set<String> malaysiaLocList = adOuDetailsMap.get("MALAYSIA");
		Set<String> intlwLocList = adOuDetailsMap.get("INTLW");
		Set<String> intleLocList = adOuDetailsMap.get("INTLE");

		if (malaysiaLocList.contains(master_loc)) {
			return "MALAYSIA";
		} else if (intlwLocList.contains(master_loc)) {
			return "INTLW";
		} else if (intleLocList.contains(master_loc)) {
			return "INTLE";
		}
		return "INTLE";
	}

	private ADStation getADStation(String master_loc, String master_division, String master_department,
			List<ADStation> adStationList) {

		ADStation adStationTemp = null;

		for (ADStation adStation : adStationList) {
			if (master_loc.equalsIgnoreCase(adStation.getLocationCode())
					&& master_division.equalsIgnoreCase(adStation.getDivisionName())
					&& master_department.equalsIgnoreCase(adStation.getDepartmentName())) {
				adStationTemp = adStation;
			}
		}

		// department is empty & valid division
		if (adStationTemp == null) {
			for (ADStation adStation : adStationList) {
				if (master_loc.equalsIgnoreCase(adStation.getLocationCode())
						&& master_division.equalsIgnoreCase(adStation.getDivisionName())
						&& "".equalsIgnoreCase(adStation.getDepartmentName())) {
					adStationTemp = adStation;
				}
			}
		}

		// department is empty and division is empty

		if (adStationTemp == null) {
			for (ADStation adStation : adStationList) {
				if (master_loc.equalsIgnoreCase(adStation.getLocationCode())
						&& "".equalsIgnoreCase(adStation.getDivisionName())) {
					adStationTemp = adStation;
				}
			}
		}
		return adStationTemp;
	}

	private Map<String, Set<String>> getADOUDetails() {

		Map<String, Set<String>> adOuDetailsMap = new LinkedHashMap<>();

		List<Map<String, Object>> locationMap = getJdbcTemplate().queryForList("select * from AD_OU_DETAILS");
		Set<String> malaysiaLocList = new LinkedHashSet<>();
		Set<String> intlwLocList = new LinkedHashSet<>();
		Set<String> intleLocList = new LinkedHashSet<>();
		for (Map<String, Object> locationListRow : locationMap) {
			malaysiaLocList.add(
					locationListRow.get("MALAYSIA") != null ? locationListRow.get("MALAYSIA").toString().trim() : "");
			intlwLocList
					.add(locationListRow.get("INTLW") != null ? locationListRow.get("INTLW").toString().trim() : "");
			intleLocList
					.add(locationListRow.get("INTLE") != null ? locationListRow.get("INTLE").toString().trim() : "");
		}

		adOuDetailsMap.put("MALAYSIA", malaysiaLocList);
		adOuDetailsMap.put("INTLW", intlwLocList);
		adOuDetailsMap.put("INTLE", intleLocList);

		return adOuDetailsMap;
	}

	private List<ADStation> getADStationList() {

		List<Map<String, Object>> ad_station_list = getJdbcTemplate()
				.queryForList("select * from AD_STATION_LIST order by COUNTRY,LOCATION_CODE, DIVISION,DEPARTMENT");
		String locationCode;
		String divisionCode;
		String departmentCode;
		String street;
		String city;
		String state;
		String postal;
		String country;

		List<ADStation> adStationList = new ArrayList<ADStation>();

		for (Map<String, Object> adStationListRow : ad_station_list) {

			locationCode = adStationListRow.get("LOCATION_CODE") != null
					? adStationListRow.get("LOCATION_CODE").toString().trim()
					: "";
			divisionCode = adStationListRow.get("DIVISION") != null ? adStationListRow.get("DIVISION").toString().trim()
					: "";
			departmentCode = adStationListRow.get("DEPARTMENT") != null
					? adStationListRow.get("DEPARTMENT").toString().trim()
					: "";

			street = adStationListRow.get("STREET_ADDRESS") != null
					? adStationListRow.get("STREET_ADDRESS").toString().trim()
					: "";
			city = adStationListRow.get("CITY") != null ? adStationListRow.get("CITY").toString().trim() : "";
			state = adStationListRow.get("STATE") != null ? adStationListRow.get("STATE").toString().trim() : "";
			postal = adStationListRow.get("POSTAL_CODE") != null ? adStationListRow.get("POSTAL_CODE").toString().trim()
					: "";
			country = adStationListRow.get("COUNTRY") != null ? adStationListRow.get("COUNTRY").toString().trim() : "";

			ADStation adStation = new ADStation(locationCode, divisionCode, departmentCode, street, city, state, postal,
					country);
			adStationList.add(adStation);

		}
		return adStationList;
	}

	public ArrayList<ADUpdateVO> getMovementData() {

		log.info("Started Fetching Movement Data from DB");
		try {
			ArrayList<ADUpdateVO> movementList = new ArrayList<>();

			log.info("          || Movement Table/View   || ");

			rowSet=getJdbcTemplate().queryForRowSet("SELECT * FROM IDVS_MASTER_TABLE WHERE DATE_FORMAT(DATE_START,'%Y-%m-%d')<=DATE_FORMAT(NOW(),'%Y-%m-%d') AND PROCESS_TYPE='PROFILE_UPDATE' AND IS_PROCESSED='NO' AND LEGAL_ENTITY !='FY'");

			// Fetching data for AD OU -- start
			Map<String, Set<String>> adOuDetailsMap = getADOUDetails();
			// Fetching data for AD OU -- End

			// fetching AD Station list- START
			List<ADStation> adStationList = getADStationList();

			// fetching AD station list-END

			while (rowSet.next()) {

				// Added in order to update the data in sync with Pulsera

				movementObj = new ADUpdateVO();

				// nik
				/*
				 * MALAYSIA =
				 * getJdbcTemplate().queryForRowSet("select MALAYSIA from AD_OU_DETAILS WHERE '"
				 * + rowSet.getString("LOCATION") + "' IN (MALAYSIA)"); INTLW =
				 * getJdbcTemplate().queryForRowSet( "select INTLW from AD_OU_DETAILS WHERE '" +
				 * rowSet.getString("LOCATION") + "' IN (INTLW)"); if (MALAYSIA.next()) {
				 * movementObj.setAdOU("MALAYSIA"); } else if (INTLW.next()) {
				 * movementObj.setAdOU("INTLW"); } else { movementObj.setAdOU("INTLE"); }
				 */

				// end

				// ADDRESS UPDATE Raji

				/*
				 * if (rowSet.getString("LOCATION").equalsIgnoreCase("KULAP")) {
				 * 
				 * String division = rowSet.getString("DIVISION").replaceAll("'", "''"); String
				 * department = rowSet.getString("DEPARTMENT").replaceAll("'", "''"); String
				 * query = "";
				 * 
				 * String queryForCount =
				 * "SELECT count(*) FROM AD_STATION_LIST WHERE DIVISION='" + division +
				 * "' AND DEPARTMENT='" + department + "' AND LOCATION_CODE='" +
				 * rowSet.getString("LOCATION") + "'";
				 * 
				 * int count1 = getJdbcTemplate().queryForInt(queryForCount);
				 * 
				 * if (count1 == 0) {
				 * 
				 * queryForCount = "SELECT count(*) FROM AD_STATION_LIST WHERE DIVISION='" +
				 * division + "' AND DEPARTMENT ='' AND LOCATION_CODE='" +
				 * rowSet.getString("LOCATION") + "'"; int count2 =
				 * getJdbcTemplate().queryForInt(queryForCount);
				 * 
				 * if (count2 == 0) { query =
				 * "SELECT STREET_ADDRESS,CITY,POSTAL_CODE,STATE,COUNTRY FROM AD_STATION_LIST WHERE DIVISION='' AND DEPARTMENT ='' AND LOCATION_CODE='"
				 * + rowSet.getString("LOCATION") + "'"; } else {
				 * 
				 * query =
				 * "SELECT STREET_ADDRESS,CITY,POSTAL_CODE,STATE,COUNTRY FROM AD_STATION_LIST WHERE DIVISION='"
				 * + division + "' AND DEPARTMENT ='' AND LOCATION_CODE='" +
				 * rowSet.getString("LOCATION") + "'"; } } else { query =
				 * "SELECT STREET_ADDRESS,POSTAL_CODE,CITY,STATE,COUNTRY FROM AD_STATION_LIST WHERE DIVISION='"
				 * + division + "' AND DEPARTMENT='" + department + "' AND LOCATION_CODE='" +
				 * rowSet.getString("LOCATION") + "'"; } SqlRowSet value =
				 * getJdbcTemplate().queryForRowSet(query); if (value.next()) {
				 * movementObj.setStreetAddress(value.getString("STREET_ADDRESS"));
				 * movementObj.setState(value.getString("STATE"));
				 * movementObj.setCity(value.getString("CITY"));
				 * 
				 * movementObj.setPostalCode(value.getString("POSTAL_CODE"));
				 * 
				 * movementObj.setCountry(value.getString("COUNTRY"));
				 * 
				 * } else { movementObj.setStreetAddress("null"); movementObj.setState("null");
				 * movementObj.setCity("null"); movementObj.setPostalCode("null");
				 * movementObj.setCountry("null");
				 * 
				 * }
				 * 
				 * }
				 * 
				 * else { SqlRowSet value = getJdbcTemplate().queryForRowSet(
				 * "SELECT * FROM AD_STATION_LIST WHERE LOCATION_CODE='" +
				 * rowSet.getString("LOCATION") + "'"); if (value.next()) { if
				 * (value.getString("STREET_ADDRESS").equalsIgnoreCase(null)) {
				 * movementObj.setStreetAddress("null"); } else {
				 * movementObj.setStreetAddress(value.getString("STREET_ADDRESS")); } if
				 * (value.getString("STATE").equalsIgnoreCase(null)) {
				 * movementObj.setState("null"); } else {
				 * movementObj.setState(value.getString("STATE")); } if
				 * (value.getString("CITY").equalsIgnoreCase(null)) {
				 * movementObj.setCity("null"); } else {
				 * movementObj.setCity(value.getString("CITY")); } if
				 * (value.getString("POSTAL_CODE").equalsIgnoreCase(null)) {
				 * movementObj.setPostalCode("null"); } else {
				 * 
				 * movementObj.setPostalCode(value.getString("POSTAL_CODE")); } if
				 * (value.getString("COUNTRY").equalsIgnoreCase(null)) {
				 * movementObj.setCountry("null"); } else {
				 * movementObj.setCountry(value.getString("COUNTRY")); } //
				 * log.info("Country"+newHireObj.getCountry()); } else {
				 * movementObj.setStreetAddress("null"); movementObj.setState("null");
				 * movementObj.setCity("null"); movementObj.setPostalCode("null");
				 * movementObj.setCountry("null");
				 * 
				 * } }
				 */

				// End

				String master_loc = rowSet.getString("LOCATION").toString().trim();
				String master_division = rowSet.getString("DIVISION").toString().trim();
				String master_department = rowSet.getString("DEPARTMENT").toString().trim();

				String adou = getADOU(adOuDetailsMap, master_loc);
				movementObj.setAdOU(adou);

				/// AD STATION LIST-- START

				ADStation adStationTemp = getADStation(master_loc, master_division, master_department, adStationList);

				if (adStationTemp != null) {
					movementObj.setCountry(adStationTemp.getCountry());
					movementObj.setStreetAddress(adStationTemp.getStreetAddress());
					movementObj.setPostalCode(adStationTemp.getPostalCode());
					movementObj.setCity(adStationTemp.getCity());
					movementObj.setState(adStationTemp.getState());
				}

				movementObj.setStaffId(rowSet.getString("EMPLOYEE_NUMBER"));

				movementObj.setFullName(rowSet.getString("Full_NAME"));

				movementObj.setBusinessEmail(rowSet.getString("BUSINESS_EMAIL_ADDRESS"));

				movementObj.setFirstName(rowSet.getString("FIRST_NAME"));

				movementObj.setLastName(rowSet.getString("LAST_NAME"));
				movementObj.setCostCenter(rowSet.getString("RC"));
				movementObj.setDivisionName(rowSet.getString("DIVISION"));
				movementObj.setDepartmentName(rowSet.getString("DEPARTMENT"));
				movementObj.setDesignation(rowSet.getString("DESIGNATION"));
				movementObj.setLegalEntity(rowSet.getString("LEGAL_ENTITY"));
				movementObj.setJobGrade(rowSet.getString("JOB_GRADE_ID"));
				movementObj.setPhoneNO(rowSet.getString("PRIMARY_PHONE_NO"));
				movementObj.setSecondryPhoneNO(rowSet.getString("SECONDARY_PHONE_NO"));
				movementObj.setSuperior(rowSet.getString("IMMEDIATE_SUPERVISOR_ID"));
				movementObj.setEmployeeCategory(rowSet.getString("EMPLOYEE_CATEGORY"));
				movementObj.setLocationCode(rowSet.getString("LOCATION"));

				movementList.add(movementObj);
			}
			log.info("Passing Movement Data to AD----" + movementList.size() +" ,"+ movementList);
			return movementList;
		} catch (InvalidResultSetAccessException e) {
			log.error(e.getMessage());
			log.info(e);
			throw e;
		} catch (DataAccessException e) {
			log.error(e.getMessage());
			log.info(e);
			throw e;
		}

	}

	
	//getting movement data for FY-AD user
	//@mujeeba.khan
	
	public ArrayList<ADUpdateVO> getFYMovementData() {

		log.info("Started Fetching Movement Data from DB");
		try {
			ArrayList<ADUpdateVO> movementList = new ArrayList<>();

			log.info("          || Movement Table/View   || ");

			rowSet=getJdbcTemplate().queryForRowSet(
			//		"SELECT * FROM IDVS_MASTER_TABLE WHERE DATE_FORMAT(DATE_START,'%Y-%m-%d')<=DATE_FORMAT(NOW(),'%Y-%m-%d') AND PROCESS_TYPE='PROFILE_UPDATE' AND IS_PROCESSED='NO' AND LEGAL_ENTITY ='FY'");
					"SELECT * FROM IDVS_MASTER_TABLE WHERE EMPLOYEE_NUMBER ='2354579' AND PROCESS_TYPE='PROFILE_UPDATE' AND LEGAL_ENTITY ='FY'");
			// Fetching data for AD OU -- start
			Map<String, Set<String>> adOuDetailsMap = getADOUDetails();
			// Fetching data for AD OU -- End

			// fetching AD Station list- START
			List<ADStation> adStationList = getADStationList();

			// fetching AD station list-END

			while (rowSet.next()) {

				// Added in order to update the data in sync with Pulsera

				movementObj = new ADUpdateVO();

				// nik
				/*
				 * MALAYSIA =
				 * getJdbcTemplate().queryForRowSet("select MALAYSIA from AD_OU_DETAILS WHERE '"
				 * + rowSet.getString("LOCATION") + "' IN (MALAYSIA)"); INTLW =
				 * getJdbcTemplate().queryForRowSet( "select INTLW from AD_OU_DETAILS WHERE '" +
				 * rowSet.getString("LOCATION") + "' IN (INTLW)"); if (MALAYSIA.next()) {
				 * movementObj.setAdOU("MALAYSIA"); } else if (INTLW.next()) {
				 * movementObj.setAdOU("INTLW"); } else { movementObj.setAdOU("INTLE"); }
				 */

				// end

				// ADDRESS UPDATE Raji

				/*
				 * if (rowSet.getString("LOCATION").equalsIgnoreCase("KULAP")) {
				 * 
				 * String division = rowSet.getString("DIVISION").replaceAll("'", "''"); String
				 * department = rowSet.getString("DEPARTMENT").replaceAll("'", "''"); String
				 * query = "";
				 * 
				 * String queryForCount =
				 * "SELECT count(*) FROM AD_STATION_LIST WHERE DIVISION='" + division +
				 * "' AND DEPARTMENT='" + department + "' AND LOCATION_CODE='" +
				 * rowSet.getString("LOCATION") + "'";
				 * 
				 * int count1 = getJdbcTemplate().queryForInt(queryForCount);
				 * 
				 * if (count1 == 0) {
				 * 
				 * queryForCount = "SELECT count(*) FROM AD_STATION_LIST WHERE DIVISION='" +
				 * division + "' AND DEPARTMENT ='' AND LOCATION_CODE='" +
				 * rowSet.getString("LOCATION") + "'"; int count2 =
				 * getJdbcTemplate().queryForInt(queryForCount);
				 * 
				 * if (count2 == 0) { query =
				 * "SELECT STREET_ADDRESS,CITY,POSTAL_CODE,STATE,COUNTRY FROM AD_STATION_LIST WHERE DIVISION='' AND DEPARTMENT ='' AND LOCATION_CODE='"
				 * + rowSet.getString("LOCATION") + "'"; } else {
				 * 
				 * query =
				 * "SELECT STREET_ADDRESS,CITY,POSTAL_CODE,STATE,COUNTRY FROM AD_STATION_LIST WHERE DIVISION='"
				 * + division + "' AND DEPARTMENT ='' AND LOCATION_CODE='" +
				 * rowSet.getString("LOCATION") + "'"; } } else { query =
				 * "SELECT STREET_ADDRESS,POSTAL_CODE,CITY,STATE,COUNTRY FROM AD_STATION_LIST WHERE DIVISION='"
				 * + division + "' AND DEPARTMENT='" + department + "' AND LOCATION_CODE='" +
				 * rowSet.getString("LOCATION") + "'"; } SqlRowSet value =
				 * getJdbcTemplate().queryForRowSet(query); if (value.next()) {
				 * movementObj.setStreetAddress(value.getString("STREET_ADDRESS"));
				 * movementObj.setState(value.getString("STATE"));
				 * movementObj.setCity(value.getString("CITY"));
				 * 
				 * movementObj.setPostalCode(value.getString("POSTAL_CODE"));
				 * 
				 * movementObj.setCountry(value.getString("COUNTRY"));
				 * 
				 * } else { movementObj.setStreetAddress("null"); movementObj.setState("null");
				 * movementObj.setCity("null"); movementObj.setPostalCode("null");
				 * movementObj.setCountry("null");
				 * 
				 * }
				 * 
				 * }
				 * 
				 * else { SqlRowSet value = getJdbcTemplate().queryForRowSet(
				 * "SELECT * FROM AD_STATION_LIST WHERE LOCATION_CODE='" +
				 * rowSet.getString("LOCATION") + "'"); if (value.next()) { if
				 * (value.getString("STREET_ADDRESS").equalsIgnoreCase(null)) {
				 * movementObj.setStreetAddress("null"); } else {
				 * movementObj.setStreetAddress(value.getString("STREET_ADDRESS")); } if
				 * (value.getString("STATE").equalsIgnoreCase(null)) {
				 * movementObj.setState("null"); } else {
				 * movementObj.setState(value.getString("STATE")); } if
				 * (value.getString("CITY").equalsIgnoreCase(null)) {
				 * movementObj.setCity("null"); } else {
				 * movementObj.setCity(value.getString("CITY")); } if
				 * (value.getString("POSTAL_CODE").equalsIgnoreCase(null)) {
				 * movementObj.setPostalCode("null"); } else {
				 * 
				 * movementObj.setPostalCode(value.getString("POSTAL_CODE")); } if
				 * (value.getString("COUNTRY").equalsIgnoreCase(null)) {
				 * movementObj.setCountry("null"); } else {
				 * movementObj.setCountry(value.getString("COUNTRY")); } //
				 * log.info("Country"+newHireObj.getCountry()); } else {
				 * movementObj.setStreetAddress("null"); movementObj.setState("null");
				 * movementObj.setCity("null"); movementObj.setPostalCode("null");
				 * movementObj.setCountry("null");
				 * 
				 * } }
				 */

				// End

				String master_loc = rowSet.getString("LOCATION").toString().trim();
				String master_division = rowSet.getString("DIVISION").toString().trim();
				String master_department = rowSet.getString("DEPARTMENT").toString().trim();

				String adou = getADOU(adOuDetailsMap, master_loc);
				movementObj.setAdOU(adou);

				/// AD STATION LIST-- START

				ADStation adStationTemp = getADStation(master_loc, master_division, master_department, adStationList);

				if (adStationTemp != null) {
					movementObj.setCountry(adStationTemp.getCountry());
					movementObj.setStreetAddress(adStationTemp.getStreetAddress());
					movementObj.setPostalCode(adStationTemp.getPostalCode());
					movementObj.setCity(adStationTemp.getCity());
					movementObj.setState(adStationTemp.getState());
				}

				movementObj.setStaffId(rowSet.getString("EMPLOYEE_NUMBER"));

				movementObj.setFullName(rowSet.getString("Full_NAME"));

				movementObj.setBusinessEmail(rowSet.getString("BUSINESS_EMAIL_ADDRESS"));

				movementObj.setFirstName(rowSet.getString("FIRST_NAME"));

				movementObj.setLastName(rowSet.getString("LAST_NAME"));
				movementObj.setCostCenter(rowSet.getString("RC"));
				movementObj.setDivisionName(rowSet.getString("DIVISION"));
				movementObj.setDepartmentName(rowSet.getString("DEPARTMENT"));
				movementObj.setDesignation(rowSet.getString("DESIGNATION"));
				movementObj.setLegalEntity(rowSet.getString("LEGAL_ENTITY"));
				movementObj.setJobGrade(rowSet.getString("JOB_GRADE_ID"));
				movementObj.setPhoneNO(rowSet.getString("PRIMARY_PHONE_NO"));
				movementObj.setSecondryPhoneNO(rowSet.getString("SECONDARY_PHONE_NO"));
				movementObj.setSuperior(rowSet.getString("IMMEDIATE_SUPERVISOR_ID"));
				movementObj.setEmployeeCategory(rowSet.getString("EMPLOYEE_CATEGORY"));
				movementObj.setLocationCode(rowSet.getString("LOCATION"));

				movementList.add(movementObj);
			}
			log.info("Passing Movement Data to AD----" + movementList.size() +" ,"+ movementList);
			return movementList;
		} catch (InvalidResultSetAccessException e) {
			log.error(e.getMessage());
			log.info(e);
			throw e;
		} catch (DataAccessException e) {
			log.error(e.getMessage());
			log.info(e);
			throw e;
		}

	}

	
	public List<String[]> getParamValues(List<String> codes) throws IDMDBException, IDMException {

		List<String[]> paramsOut = new ArrayList<>();
		try {
			
			if(CollectionUtils.isEmpty(codes)) {
				return paramsOut;
			}
			
			StringBuffer codSb = new StringBuffer();
			for(String code : codes) {
				codSb.append("'").append(code).append("',");
			}
			
			String param = codSb.substring(0, codSb.length()-1);
			
		List<Map<String, Object>> params = getJdbcTemplate().queryForList(
				"SELECT PC_MODULE_NAME, PC_PARAM_VALUE FROM IDM_PARAM_CONFIG WHERE PC_MODULE_NAME in ("+param+")");
		
		for(Map<String, Object> map: params) {
			
			String module_name = (String)map.get("PC_MODULE_NAME");
			String param_value = (String)map.get("PC_PARAM_VALUE");
			paramsOut.add(new String[] {module_name,param_value });
			
		}
		} catch (DataAccessException dataAccessexception) {
			log.info(dataAccessexception);
			throw new IDMException(ErrorCodeEnums.DB_RUNTIME_ERROR.getErrorCode(), dataAccessexception);
		} catch (Exception exception) {
			log.info(exception);
			throw new IDMException(ErrorCodeEnums.INTERNAL_SERVER_ERROR.getErrorCode(), exception);
		}
		// log.debug("Fetching Security team Address completed...");
		return paramsOut;
	}

}
