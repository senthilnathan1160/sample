package com.polaris.cards.interfaces.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.orbitech.frameworks.mline.MLException;
import com.orbitech.frameworks.mline.MLFatalException;

import com.polaris.cards.common.utils.ConnectionWrapper;
import com.polaris.cards.interfaces.common.CommonTools;

import static com.intellectdesign.constants.GeneralValues.SYSTEMWIDE_ORGCODE;
import static com.intellectdesign.utilities.Quirks.show;
import static com.intellectdesign.utilities.Resources.CONNECTION_TO_CATH;
import static com.intellectdesign.utilities.Resources.closeDownSilently;
import static com.intellectdesign.utilities.Resources.getConnectionAndDrownPossibleErrors;
import static com.intellectdesign.utilities.Resources.logAndPrepareStatement;
import static com.intellectdesign.utilities.Strings.denull;
import static com.intellectdesign.utilities.Strings.isNotEmpty;
import static com.intellectdesign.utilities.Strings.isNotIn;
import static com.intellectdesign.utilities.Strings.isNotVain;

/*
 * COPYRIGHT NOTICE
 *
 * Copyright 2014 Polaris Financial Technologies Ltd. All rights reserved.
 *
 * These materials are confidential and proprietary to
 * Polaris Financial Technologies Ltd. and no part of these materials should
 * be reproduced, published, transmitted or distributed in any form or
 * by any means, electronic, mechanical, photocopying, recording or
 * otherwise, or stored in any information storage or retrieval system
 * of any nature nor should the materials be disclosed to third parties
 * or used in any other manner for which this is not authorized, without
 * the prior express written authorization of Polaris Financial Technologies Ltd.
 *
 */

/*********************************************************************
 *
 * Module Name : Online Interface
 *
 * File Name : QueryForTheInstallmentsPurchasesDAO.java
 *
 * Description : This class defines methods to perform to show unbilled trasanction for the given customer card number.
 *
 * Version Control Block
 *
 * Date Version Author Description
 * --------- -------- --------------- ---------------------------
 * 06-06-2006 1.1 Rahamth Updated CopyRight Notice and History
 * Framework Generated Code - DO NOT MODIFY
 *
 * FIX-05112008 - Ramesh.G - Added Unmatched and payment details in the response xml.
 * FIX-18022009 - Ramesh.G - Displaying payments in multiple records in the response xml.
 * Issue Number 1104
 * FIX-16092009 - Ramesh.G - 1. The online transaction is not displaying (TR310).
 * 2. The transactions with description CASA & IDEAS is not displaying. Problems with &.
 * 3. The new installment transactions not billed, have problem with tenor, because is displaying for example 05/03. It is not correct. is attached evidences.
 * 4. The description for installment is not correct, because should display the merchant descriptions. Currently is displaying the transaction description from tx_master. it is not correct. This exception is only for renegotiation and refinancing transactions. Issue Number 1673 FIX-16092009-01
 * FIX-18072011 - Ramesh G 1. Unbilled tranaction from TR310 are getting displayed twice if the card is gets swapped. Defect Id 1970.
 *
 * 20131029 1.0.1.40.1.4 David Lopez CR326: GAPS IN MC PROJECT
 * 20140326 # on top of David Lopez CR333: CHANGES IN TX00031 ONLINE SERVICE
 * 20140331 # on top of r18804 David Lopez CR333: New filter is added to avoid duplicate transactions.
 * 20140401 # on top of r18815 David Lopez CR333: A new query is made.
 * 20140910 David Lopez CR336: All queries are done on the ORBICATH.TR310 and ORBICATH.BC4NN tables were modified to only use the CLP amounts (using the table ORBIBILL.FCY_TXN_CODE where all USD transactions are maintained).
 * 20171031 Miguel Farah: major surgery on the full source, to unify the differing versions available, purify the resulting code and make the hardcoded SQL queries comply with the extant PCI requirements.
 * 20171031 Miguel Farah: CR385: the '&' character needs to be escaped in the generated XML sent for ALL possible responses.
 **********************************************************************/
public class QueryForTheInstallmentsPurchasesDAO {

    private static final String ME = QueryForTheInstallmentsPurchasesDAO.class.getName();

    static Logger log = Logger.getLogger(ME);

    private static final String SQL_SELECT_RE900_MEANING         = "SELECT MEANING FROM ORBIAFU.RE900 WHERE CNAME = 'INTERFACE-ONLINE' AND CVALUE = ? AND ORG_CODE = ?";
    private static final String SQL_SELECT_BILL_MONTH            = "SELECT BILL_MONTH FROM ORBIBILL.BC100 WHERE STATUS_CODE=? AND AC180A=? AND CYC_CODE=? AND BILL_MONTH=(SELECT BILL_MONTH FROM ORBIBILL.BC100 WHERE CYC_CODE=? AND BILL_MONTH=(SELECT CURRENT_YYMM FROM ORBIBILL.DATE_TAB) AND VERIFY_FLAG=? AND RECORD_STATUS=?) AND VERIFY_FLAG=? AND RECORD_STATUS=?";
    private static final String SQL_SELECT_DATA_FOR_THIS_MONTH   = "SELECT E.EM_CARD_NO, E.EM_TXN_CODE, M.ME_POPULAR_NAME, TO_CHAR(E.EM_BOOKED_DATE, 'YYYYMMDD'), E.EM_ORG_LOAN_AMT, (E.EM_TOTAL_INST + E.EM_GRACE_PERIOD) EM_TOTAL_INST, NVL(E.EM_TOT_INSTLS_PAID, '0') EM_TOT_INSTLS_PAID, C.CC_INST_AMT, N.USE_MENAME_SWITCH, TLD.ME_NAME, E.EM_AUTH_NO, E.EM_ARN_NO FROM ORBIBILL.EMITR E JOIN ORBIBILL.TX_MASTER T ON (T.ORG_CODE = ? AND T.TX_CODE = E.EM_TXN_CODE AND T.RECORD_STATUS = ? AND T.VERIFY_FLAG = ?) JOIN ORBIMERC.MEMST M ON (M.ORG_CODE = ? AND M.ME_CODE = E.EM_MERC_NO) JOIN ORBIBILL.CURPTSCH C ON (C.ORG_CODE = ? AND C.CC_SLNO = E.EM_SLNO AND C.CC_CLEAR_DATE IS NULL AND TO_CHAR(C.CC_DUE_DATE,'YYMM') = (SELECT BILL_MONTH FROM ORBIBILL.BC100 WHERE ORG_CODE = ? AND CYC_CODE = ? AND BILL_MONTH IN (SELECT TO_CHAR(ADD_MONTHS(TO_DATE(CURRENT_YYMM, 'YYMM'), 1), 'YYMM') FROM ORBIBILL.DATE_TAB) AND VERIFY_FLAG = ? AND RECORD_STATUS = ?)) JOIN ORBIBILL.RE900 R ON (R.ORG_CODE = ? AND R.CNAME = ? AND R.CVALUE = E.EM_CR_SEG_CODE AND R.RECORD_STATUS = ? AND R.VERIFY_FLAG = ?) LEFT JOIN ORBIMERC.NETWORK_MASTER N ON (N.ORG_CODE = ? AND M.ME_NETWORK_CODE = N.NETWORK_CODE AND N.VERIFY_FLAG = ?) LEFT JOIN ORBIBILL.TXN_LOC_DETAILS TLD ON (TLD.ORG_CODE = ? AND TLD.NETWORK_CODE = M.ME_NETWORK_CODE AND TLD.CUST_NO = ? AND E.EM_AUTH_NO = TLD.AUTH_NO) WHERE E.ORG_CODE = ? AND E.EM_CUST_NO = ? AND E.EM_STATUS_FLG = ?";
    private static final String SQL_SELECT_DATA_FOR_NEXT_MONTH   = "SELECT E.EM_CARD_NO, E.EM_TXN_CODE, M.ME_POPULAR_NAME, TO_CHAR(E.EM_BOOKED_DATE, 'YYYYMMDD'), E.EM_ORG_LOAN_AMT, (E.EM_TOTAL_INST + E.EM_GRACE_PERIOD) EM_TOTAL_INST, NVL(E.EM_TOT_INSTLS_PAID, '0') EM_TOT_INSTLS_PAID, C.CC_INST_AMT, N.USE_MENAME_SWITCH, TLD.ME_NAME, E.EM_AUTH_NO, E.EM_ARN_NO FROM ORBIBILL.EMITR E JOIN ORBIBILL.TX_MASTER T ON (T.ORG_CODE = ? AND T.TX_CODE = E.EM_TXN_CODE AND T.RECORD_STATUS = ? AND T.VERIFY_FLAG = ?) JOIN ORBIMERC.MEMST M ON (M.ORG_CODE = ? AND M.ME_CODE = E.EM_MERC_NO) JOIN ORBIBILL.CURPTSCH C ON (C.ORG_CODE = ? AND C.CC_SLNO = E.EM_SLNO AND C.CC_CLEAR_DATE IS NULL AND TO_CHAR(C.CC_DUE_DATE,'YYMM') = (SELECT BILL_MONTH FROM ORBIBILL.BC100 WHERE ORG_CODE = ? AND CYC_CODE = ? AND BILL_MONTH IN (SELECT TO_CHAR(           TO_DATE(CURRENT_YYMM, 'YYMM'),'YYMM')      FROM ORBIBILL.DATE_TAB) AND VERIFY_FLAG = ? AND RECORD_STATUS = ?)) JOIN ORBIBILL.RE900 R ON (R.ORG_CODE = ? AND R.CNAME = ? AND R.CVALUE = E.EM_CR_SEG_CODE AND R.RECORD_STATUS = ? AND R.VERIFY_FLAG = ?) LEFT JOIN ORBIMERC.NETWORK_MASTER N ON (N.ORG_CODE = ? AND M.ME_NETWORK_CODE = N.NETWORK_CODE AND N.VERIFY_FLAG = ?) LEFT JOIN ORBIBILL.TXN_LOC_DETAILS TLD ON (TLD.ORG_CODE = ? AND TLD.NETWORK_CODE = M.ME_NETWORK_CODE AND TLD.CUST_NO = ? AND E.EM_AUTH_NO = TLD.AUTH_NO) WHERE E.ORG_CODE = ? AND E.EM_CUST_NO = ? AND E.EM_STATUS_FLG = ?";
    private static final String SQL_SELECT_TR310_AND_MASTER_DATA = "SELECT NVL(AUTH_AMT, 0) AUTH_AMT, TO_CHAR(ENT_DTTM, 'YYYYMMDD') ENT_DTTM, ME_NAME, N.USE_MENAME_SWITCH FROM ORBICATH.TR310 B JOIN ORBIMERC.NETWORK_MASTER N ON (SUBSTR(B.MERC_NO, 1, 2) = N.NETWORK_CODE AND N.VERIFY_FLAG=?) WHERE B.ORG_CODE=? AND B.CUST_NO=? AND B.PROCSS_FLG IS NULL AND SUBSTR(B.AUTH_STAT, 2, 1)=? AND B.DR_CR_FLAG=? AND NVL(B.SYS_CODE1, 'Y') != ? AND B.SEGMENT_CODE NOT IN ('L3', 'L4', 'L5', 'L6', 'L7', 'L8', 'L9', 'L10', 'L11', 'L12', 'L13', 'L14', 'L15') AND B.TXN_CODE NOT IN (SELECT TXN_CODE FROM ORBIBILL.FCY_TXN_CODE WHERE ORG_CODE=? AND VERIFY_FLAG=? AND RECORD_STATUS=? AND CURR_CODE=?)";
    private static final String SQL_SELECT_BC4PYMT_DATA          = "SELECT NVL(AMOUNT_RS, '0') AMOUNT_RS, CARD_NUM CARD_NUM, POST_DATE POST_DATE FROM ORBIBILL.BC4_PYMT WHERE PRIMARY_CARD_NUM=? AND TX_CODE IN (?, ?) AND PROCESS_FLAG IS NULL AND (AUTH_FLAG<>? OR AUTH_FLAG IS NULL)";
    private static final String SQL_SELECT_TR310_DATA            = "SELECT NVL(AUTH_AMT, 0) AUTH_AMT, TO_CHAR(ENT_DTTM, 'YYYYMMDD') ENT_DTTM FROM ORBICATH.TR310 B WHERE ORG_CODE=? AND CUST_NO=? AND PROCSS_FLG IS NULL AND SUBSTR(AUTH_STAT, 2, 1)=? AND DR_CR_FLAG=? AND NVL(SYS_CODE1, 'Y')!=? AND SEGMENT_CODE NOT IN ('L3', 'L4', 'L5', 'L6', 'L7', 'L8', 'L9', 'L10', 'L11', 'L12', 'L13', 'L14', 'L15') AND B.TXN_CODE NOT IN (SELECT TXN_CODE FROM ORBIBILL.FCY_TXN_CODE WHERE ORG_CODE=? AND VERIFY_FLAG=? AND RECORD_STATUS=? AND CURR_CODE=?)";
    private static final String SQL_SELECT_CARD_NUMBER           = "SELECT NVL(PARENT_CARD_NUM, CARD_NUM) FROM CDMST WHERE CARD_NUM=? OR RECARD_NUM=? AND ORG_CODE=?";
    private static final String SQL_SELECT_CARD_DATA             = "SELECT CARD_TYPE, CARD_MEM_TYPE, CUST_NO FROM CDMST WHERE (CARD_NUM=? OR RECARD_NUM=?)";
    private static final String SQL_SELECT_MORE_CARD_DATA        = "SELECT CARD_TYPE, CARD_MEM_TYPE, CUST_NO, PRIMARY_CARD_NUM, CYC_CODE, SUBSTR(CDMST.LB_DATE, 1, 4) LB_DATE FROM ORBICATH.CDMST WHERE (CARD_NUM=? OR RECARD_NUM=?)";
    private static final String SQL_SELECT_PRIMARY_NUMBER        = "SELECT DISTINCT PRIMARY_CARD_NUM FROM ORBICATH.CDMST WHERE CUST_NO=?";

    private static final String TEMPLATE_SQL_SELECT_BCDATA = "SELECT B.PRIMARY_CARD_NUM, SUBSTR(B.TX_CODE, 1, 3), B.TX_DESC, B.ST_REF_DT, B.AMOUNT_RS FROM __table__name__ B WHERE B.DR_CR_CODE=? AND BILL_FLAG!=? AND B.PRIMARY_CARD_NUM=? AND SUBSTR(B.TX_CODE,0,3) NOT IN (SELECT TXN_CODE FROM ORBIBILL.FCY_TXN_CODE WHERE ORG_CODE=? AND VERIFY_FLAG=? AND RECORD_STATUS=? AND CURR_CODE=?)";

    private static final String ORG_CODE = SYSTEMWIDE_ORGCODE;

    private CommonTools commonTools;

    private static enum TypeMade {
        PURCHASES, PAYMENTS, CANCELLATION;

        private TypeMade() {}
    }


    public QueryForTheInstallmentsPurchasesDAO() {
        this.commonTools = new CommonTools();
    }

    private static final String PURCHASES_MADE    = getValueMade(TypeMade.PURCHASES);
    private static final String PAYMENTS_MADE     = getValueMade(TypeMade.PAYMENTS);
    private static final String CANCELLATION_MADE = getValueMade(TypeMade.CANCELLATION);


    public Map<String, String> getDBDetails(Map<String, String> pInMap, String jndi) throws MLException, MLFatalException {
        log.debug("Entering in getDBDetails method (pInMap): " + pInMap);
        log.debug("Entering in getDBDetails method (jndi): " + jndi);

        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        String account_no = "";
        String primary_card_num = "";
        String card_no = "";
        String fecha = "";
        String codTrx = "";
        String descripcion = "";
        String montoTrx = "";
        String numCuota = "";
        String numTotalCuota = "";
        String lv_cycCode = "";

        int sumOfInstallmentAmount = 0;
        int sumOfRevolvingAmount = 0;
        int sumofCancellationAmount = 0;
        int sumOfPaymentAmount = 0;
        int totalFacturar = 0;

        ArrayList amountList = new ArrayList();

        String installmentAmount = "";
        StringBuffer sb = new StringBuffer();
        StringBuffer lv_InstallmentDetails = new StringBuffer();

        try {
            conn = getConnectionAndDrownPossibleErrors(ME, CONNECTION_TO_CATH);

            String cardType = "";
            String cardAccountNum = "";

            Set set = pInMap.entrySet();
            Iterator it = set.iterator();
            while (it.hasNext()) {
                Map.Entry me = (Map.Entry)it.next();
                String keydetails = (String)me.getKey();
                if (keydetails.endsWith("numCta")) {
                    account_no = pInMap.get(keydetails);
                    log.debug("accountNumber: " + account_no);
                }
            }

            BasicResponse basicResponse = new BasicResponse();
            account_no = this.commonTools.getCardNumberFromInput(account_no, ConnectionWrapper.getConnection(CONNECTION_TO_CATH)); // BADCODE Why does this need a separate Connection object?
            basicResponse = isValidCardNumber(conn, account_no);

            int cardCount = Integer.parseInt(basicResponse.getResponseMap().get("CARD_COUNT"));
            if (cardCount == 0) {
                sb = getInvalidTransactionResponse();
                log.debug("CARD NUMBER / RECARD-NUMBER DOES NOT EXIST IN CDMST " + account_no);
            } else {
                List localList = basicResponse.getList();
                Iterator it2 = localList.iterator();
                cardType = basicResponse.getResponseMap().get("CARD_TYPE");
                cardAccountNum = basicResponse.getResponseMap().get("ACCCOUNT_NO");
                lv_cycCode = basicResponse.getResponseMap().get("CYC_CODE");
                if (lv_cycCode.length() < 2) {
                    lv_cycCode = "0" + lv_cycCode;
                }
                int i3 = 0; // BADCODE This clearly a binary flag and should be a boolean, not an int.

                closeDownSilently(ME, rs, ps);
                ps = logAndPrepareStatement(ME, conn, SQL_SELECT_BILL_MONTH, "-15", "5", lv_cycCode, lv_cycCode, "A", "A", "A", "A");
                rs = ps.executeQuery();

                if ((rs != null) && rs.next()) {
                    i3 = 1;
                    log.debug("Billing not happened for this card for this month");
                } else {
                    i3 = 0;
                    log.debug("Billing happened for this card number. Adding one month extra.");
                }

                String[] arguments = { ORG_CODE, "A", "A", ORG_CODE, ORG_CODE, ORG_CODE, lv_cycCode, "A", "A", ORG_CODE, "CREDIT_SEGMENT", "A", "A", ORG_CODE, "A", ORG_CODE, cardAccountNum, ORG_CODE, cardAccountNum, "P" };
                closeDownSilently(ME, rs, ps);
                ps = logAndPrepareStatement(ME, conn, ((i3 == 0) ? SQL_SELECT_DATA_FOR_THIS_MONTH : SQL_SELECT_DATA_FOR_NEXT_MONTH), arguments);
                rs = ps.executeQuery();

                while ((rs != null) && rs.next()) {
                    card_no = rs.getString(1);
                    codTrx = rs.getString(2);
                    descripcion = rs.getString(3);
                    fecha = rs.getString(4);
                    montoTrx = rs.getString(5);
                    numCuota = rs.getString(6);
                    numTotalCuota = rs.getString(7);
                    installmentAmount = rs.getString(8);
                    log.debug(show("card_no", card_no, "codTrx", codTrx, "descripcion", descripcion, "fecha", fecha));
                    log.debug(show("montoTrx", montoTrx, "numCuota", numCuota, "numTotalCuota", numTotalCuota, "installmentAmount", installmentAmount));
                    log.debug("ORBIMERC.NETWORK_MASTER.USE_MENAME_SWITCH = " + rs.getString(9));
                    log.debug("ORBIBILL.TXN_LOC_DETAILS.ME_NAME          = " + rs.getString(10));
                    log.debug("ORBIBILL.EMITR.EM_AUTH_NO                 = " + rs.getString(11));
                    log.debug("ORBIBILL.EMITR.EM_ARN_NO                  = " + rs.getString(12));
                    // BADCODE Why bother retrieving data that will not be used?

                    amountList.add(montoTrx);
                    if (("Y".equalsIgnoreCase(rs.getString(9))) && isNotVain(rs.getString(10))) {
                        descripcion = rs.getString(10);
                        log.debug("descripcion revisada: " + descripcion);
                    }
                    String correlativo = (numTotalCuota == null) ? "" : String.valueOf(1 + Integer.parseInt(numTotalCuota)) + " de " + numCuota;

                    lv_InstallmentDetails.append(movPorFacturarTag(fecha, "2", descripcion, montoTrx, correlativo, installmentAmount));

                    if (isNotEmpty(installmentAmount)) {
                        sumOfInstallmentAmount += Integer.parseInt(installmentAmount);
                    }
                }
                log.debug(show("sumOfInstallmentAmount", sumOfInstallmentAmount));

                closeDownSilently(ME, rs, ps);
                ps = logAndPrepareStatement(ME, conn, SQL_SELECT_TR310_AND_MASTER_DATA, "A", ORG_CODE, cardAccountNum, "A", "D", "R", ORG_CODE, "A", "A", "USD");
                rs = ps.executeQuery();

                while ((rs != null) && rs.next()) {
                    descripcion = ("Y".equalsIgnoreCase(rs.getString("USE_MENAME_SWITCH"))) ? PURCHASES_MADE : rs.getString("ME_NAME");
                    fecha = rs.getString("ENT_DTTM");
                    montoTrx = rs.getString("AUTH_AMT");
                    log.debug(show("fecha", fecha, "monthTrx", montoTrx));

                    lv_InstallmentDetails.append(movPorFacturarTag(fecha, "1", descripcion, montoTrx, null, null));

                    if (montoTrx != null) {
                        sumOfRevolvingAmount += Integer.parseInt(montoTrx);
                    }
                    log.debug(show("sumOfRevolvingAmount", sumOfRevolvingAmount));
                }

                sb.append("<movs>").append(lv_InstallmentDetails.toString());

                while (it2.hasNext()) {
                    log.debug(show("baseCardDetails.getResponseMap()", basicResponse.getResponseMap()));
                    primary_card_num = (String)it2.next();
                    log.debug(show("primaryCardNum", primary_card_num));

                    String tabla = "ORBIBILL.BC4" + lv_cycCode;

                    closeDownSilently(ME, rs, ps);
                    ps = logAndPrepareStatement(ME, conn, TEMPLATE_SQL_SELECT_BCDATA.replaceAll("__table__name__", tabla), "C", "U", primary_card_num, ORG_CODE, "A", "A", "USD");
                    rs = ps.executeQuery();

                    while ((rs != null) && rs.next()) {
                        log.debug("INSIDE PAYMENT CONDITION.. ENTERING");
                        card_no = rs.getString(1);
                        codTrx = rs.getString(2);
                        descripcion = rs.getString(3);
                        fecha = rs.getString(4);
                        montoTrx = rs.getString(5);
                        sb.append(movPorFacturarTag(fecha, "0", descripcion, montoTrx, null, null));
                        if (montoTrx != null) {
                            sumOfPaymentAmount += Integer.parseInt(montoTrx);
                        }
                        log.debug("INSIDE PAYMENT CONDITION.. LEAVING  " + sumOfPaymentAmount);
                    }

                    closeDownSilently(ME, rs, ps);
                    ps = logAndPrepareStatement(ME, conn, SQL_SELECT_BC4PYMT_DATA, primary_card_num, "062001", "570001", "R"); // HARDCODEDVALUE Meaningful constants are needed here.
                    rs = ps.executeQuery();

                    while (rs.next()) {
                        card_no = rs.getString("CARD_NUM");
                        descripcion = PAYMENTS_MADE;
                        fecha = rs.getString("POST_DATE");
                        montoTrx = rs.getString("AMOUNT_RS");
                        sb.append(movPorFacturarTag(fecha, "0", descripcion, montoTrx, null, null));
                        if (montoTrx != null) {
                            sumOfPaymentAmount += Integer.parseInt(montoTrx);
                        }
                        log.debug("INSIDE PAYMENT TODAY CONDITION.. LEAVING  " + sumOfPaymentAmount);
                    }

                    closeDownSilently(ME, rs, ps);
                    ps = logAndPrepareStatement(ME, conn, TEMPLATE_SQL_SELECT_BCDATA.replaceAll("__table__name__", tabla), "D", "U", primary_card_num, ORG_CODE, "A", "A", "USD");
                    rs = ps.executeQuery();

                    while (rs.next()) {
                        card_no = rs.getString(1);
                        codTrx = rs.getString(2);
                        descripcion = rs.getString(3);
                        fecha = rs.getString(4);
                        montoTrx = rs.getString(5);
                        sb.append(movPorFacturarTag(fecha, "1", descripcion, montoTrx, null, null));
                        if (montoTrx != null) {
                            sumOfRevolvingAmount += Integer.parseInt(montoTrx);
                        }
                        log.debug("INSIDE REVOLVING CONDITION.. LEAVING " + sumOfRevolvingAmount);
                    }

                    closeDownSilently(ME, rs, ps);
                    ps = logAndPrepareStatement(ME, conn, SQL_SELECT_TR310_DATA, ORG_CODE, cardAccountNum, "A", "C", "R", ORG_CODE, "A", "A", "USD");
                    rs = ps.executeQuery();

                    while ((rs != null) && rs.next()) {
                        descripcion = CANCELLATION_MADE;
                        fecha = rs.getString("ENT_DTTM");
                        montoTrx = rs.getString("AUTH_AMT");
                        log.debug(show("fecha", fecha, "monthTrx", montoTrx));
                        sb.append(movPorFacturarTag(fecha, "0", descripcion, montoTrx, null, null));
                        if (montoTrx != null) {
                            sumofCancellationAmount += Integer.parseInt(montoTrx);
                        }
                        log.debug("INSIDE REVOLVING UNMATCHED PURCHASES CONDITION.. LEAVING " + sumOfRevolvingAmount);
                    }
                    totalFacturar = sumOfRevolvingAmount + sumOfInstallmentAmount - (sumOfPaymentAmount + sumofCancellationAmount);
                }
                log.debug(show("totalFacturar", totalFacturar));

                sb.append("</movs>");
                sb.append("<totalAFacturar>").append(totalFacturar).append("</totalAFacturar>");
                sb.append("<tipoProdTarjeta>").append(denull(cardType)).append("</tipoProdTarjeta>");
            }
            log.debug("FINAL VALUE ==> " + sb.toString() + "\n\n");

            pInMap.put("RESPONSE_PARAM", sb.toString());
        } catch (SQLException e1) {
            log.debug("SQL Exception Occurred " + e1);
            sb = getInvalidTransactionResponse();
            pInMap.put("RESPONSE_PARAM", sb.toString());
        } catch (Exception e2) {
            log.debug("Exception Occurred " + e2);
            sb = getInvalidTransactionResponse();
            pInMap.put("RESPONSE_PARAM", sb.toString());
        } finally {
            closeDownSilently(ME, rs, ps, conn);
            log.debug("Leaving from getDBDetails(pInMap): " + pInMap);
        }
        return pInMap;
    }


    private static String getValueMade(TypeMade paramTypeMade) {
        log.debug("Entering getValueMade method (type): " + paramTypeMade);

        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        String meaning = null;

        try {
            conn = getConnectionAndDrownPossibleErrors(ME, CONNECTION_TO_CATH);

            String type = null;
            if (paramTypeMade == TypeMade.PURCHASES) {
                type = "TX00031_PURCHASES";
            } else if (paramTypeMade == TypeMade.PAYMENTS) {
                type = "TX00031_PAYMENTS";
            } else {
                type = "TX00031_CANCELLATION";
            }

            closeDownSilently(ME, rs, ps);
            ps = logAndPrepareStatement(ME, conn, SQL_SELECT_RE900_MEANING, type, ORG_CODE);
            rs = ps.executeQuery();

            if ((rs != null) && rs.next()) {
                meaning = rs.getString("MEANING");
            }

        } catch (SQLException e) {
            log.fatal("SQLException arise in getValueMade", e);
            return meaning;
        } finally {
            closeDownSilently(ME, rs, ps, conn);
        }

        return meaning;
    }


    public String getPrim(Connection conn, String cardNum, String cardType, String cardMemType, String orgCode) throws MLException, MLFatalException {
        if ((cardType.length() == 0) || (cardType.length() < 5)) {
            return "1";
        }
        if (cardNum.length() == 0) {
            return "2";
        }
        if (isNotIn(cardMemType, "C", "P", "A")) {
            return "3";
        }

        PreparedStatement ps = null;
        ResultSet rs = null;

        String parCardNum = "";

        try {
            String lastThree;
            if (cardNum.charAt(cardNum.length() - 2) > '0') {
                closeDownSilently(ME, rs, ps);
                ps = logAndPrepareStatement(ME, conn, SQL_SELECT_CARD_NUMBER, new Integer(cardNum), new Integer(cardNum), orgCode);
                rs = ps.executeQuery();

                if (rs.next()) {
                    parCardNum = rs.getString(1);
                } else {
                    return "4";
                }
            } else {
                parCardNum = cardNum;
            }
            log.debug("The par card num is " + parCardNum);

            if (parCardNum.charAt(parCardNum.length() - 3) > '0') {
                lastThree = parCardNum.substring(0, cardNum.length() - 3);
                int thirdLast = parCardNum.charAt(cardNum.length() - 3) - '0';
                int secLast = parCardNum.charAt(cardNum.length() - 2) - '0';
                int last = parCardNum.charAt(cardNum.length() - 1) - '0';
                log.debug("The primary card num is " + last);

                last = (thirdLast + last) % 10;
                thirdLast = 0;
                parCardNum = lastThree + thirdLast + secLast + last;
                log.debug("The primary card num is " + parCardNum);
            }

        } catch (Exception e) {
            throw new MLFatalException("ML9998", e);
        } finally {
            closeDownSilently(ME, rs, ps);
        }

        return parCardNum;
    }


    public Map<String, String> getCardType(Connection conn, String cardNum) throws MLException, MLFatalException {
        log.debug("Entering in getCardType method (connection): " + conn);
        log.debug("Entering in getCardType method (cardNumber): " + cardNum);

        PreparedStatement ps = null;
        ResultSet rs = null;

        HashMap cardTypeMap = new HashMap();

        try {
            closeDownSilently(ME, rs, ps);
            ps = logAndPrepareStatement(ME, conn, SQL_SELECT_CARD_DATA, cardNum, cardNum);
            rs = ps.executeQuery();

            int count = 0;
            while (rs.next()) {
                count++;
                cardTypeMap.put("CARD_TYPE", rs.getString(1));
                cardTypeMap.put("CARD_MEM_TYPE", rs.getString(2));
                cardTypeMap.put("ACCCOUNT_NO", rs.getString(3));
            }
            if (count == 0) {
                cardTypeMap.put("CARD_TYPE", "");
                cardTypeMap.put("CARD_MEM_TYPE", "");
                cardTypeMap.put("ACCCOUNT_NO", "");
            }
            log.debug("cardTypeMap: " + cardTypeMap);
        } catch (Exception e) {
            throw new MLFatalException("ML9998", e);
        } finally {
            closeDownSilently(ME, rs, ps);
        }
        return cardTypeMap;
    }


    private BasicResponse isValidCardNumber(Connection conn, String cardNum) throws MLException, MLFatalException {
        log.debug("Entering in isValidCardNumber method (connection): " + conn);
        log.debug("Entering in isValidCardNumber method (cardNumber): " + cardNum);

        PreparedStatement ps = null;
        ResultSet rs = null;

        BasicResponse br = new BasicResponse();
        HashMap resMap = new HashMap();

        try {
            closeDownSilently(ME, rs, ps);
            ps = logAndPrepareStatement(ME, conn, SQL_SELECT_MORE_CARD_DATA, cardNum, cardNum);
            rs = ps.executeQuery();

            if ((rs != null) && rs.next()) {
                resMap.put("CARD_COUNT", "1");
                resMap.put("CARD_TYPE", rs.getString("CARD_TYPE"));
                resMap.put("CARD_MEM_TYPE", rs.getString("CARD_MEM_TYPE"));
                resMap.put("ACCCOUNT_NO", rs.getString("CUST_NO"));
                resMap.put("CYC_CODE", rs.getString("CYC_CODE"));
                resMap.put("LB_DATE", rs.getString("LB_DATE"));
                br.setResponseMap(resMap);
            } else {
                resMap.put("CARD_COUNT", "0");
                br.setResponseMap(resMap);
                return br;
            }
            String cust_no = rs.getString("CUST_NO");

            closeDownSilently(ME, rs, ps);
            ps = logAndPrepareStatement(ME, conn, SQL_SELECT_PRIMARY_NUMBER, cust_no);
            rs = ps.executeQuery();

            ArrayList<String> lo1 = new ArrayList<String>();
            while ((rs != null) && rs.next()) {
                lo1.add(rs.getString("PRIMARY_CARD_NUM"));
            }
            br.setList(lo1);

        } catch (SQLException e) {
            resMap.put("CARD_COUNT", "0");
            log.fatal("SQLException: ", e);
            MLException localMLException = new MLException();
            localMLException.addError("ML0003", "");
            throw localMLException;
        } catch (Exception e1) {
            resMap.put("CARD_COUNT", "0");
            log.fatal("Exception: ", e1);
            throw new MLFatalException("ML9998", e1);

        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (ps != null) {
                    ps.close();
                }
            } catch (Exception e2) {
                resMap.put("CARD_COUNT", "0");
                throw new MLFatalException("ML9998", e2);
            }
            log.debug("Leaving from isValidCardNumber (response): " + br);
        }
        return br;
    }


    private StringBuffer getInvalidTransactionResponse() {
        StringBuffer responseBuffer = new StringBuffer();
        responseBuffer.append("<movs>");
        responseBuffer.append(movPorFacturarTag(null, null, null, null, null, null));
        responseBuffer.append("</movs><totalAFacturar></totalAFacturar><tipoProdTarjeta></tipoProdTarjeta>");
        return responseBuffer;
    }


    private static String movPorFacturarTag(final String fecha, final String codTrx, final String descripcion, final String montoTrx, final String numCuota, final String montoCuota) {
        String descripcionSaneada = denull(descripcion).replaceAll("&", "&amp;");
        return "<movPorFacturar><fecha>" + denull(fecha) + "</fecha><codTrx>" + denull(codTrx) + "</codTrx><descripcion>" + descripcionSaneada + "</descripcion><montoTrx>" + denull(montoTrx) + "</montoTrx><numCuota>" + denull(numCuota) + "</numCuota><montoCuota>" + denull(montoCuota) + "</montoCuota></movPorFacturar>";
    }

}
