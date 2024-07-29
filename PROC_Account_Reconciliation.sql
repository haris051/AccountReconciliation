drop procedure if Exists PROC_ACCOUNT_RECONCILIATION;
DELIMITER $$
CREATE PROCEDURE `PROC_ACCOUNT_RECONCILIATION`( P_ACCOUNT_ID TEXT,
											   P_ENTRY_DATE_FROM TEXT,
											   P_ENTRY_DATE_TO TEXT,
                                               P_COMPANY_ID INT,
                                               P_YEAR TEXT )
BEGIN

	SET @QRY = CONCAT('SELECT ACC_ID, 
							  DESCRIPTION, 
							  FORM_DATE, 
							  FORM_REFERENCE, 
							  FORM_TYPE,
							  DEBIT,
							  CREDIT,
							  BALANCE,
							  GL_FLAG,
							  FORM_ID,
                              IFNULL(ROUND(((SUM(DEBIT) OVER()) / 2), 2), 0) AS TOTAL_DEBIT,
                              IFNULL(ROUND(((SUM(CREDIT) OVER()) / 2), 2), 0) AS TOTAL_CREDIT,
                              BEG_BAL,
                              IS_Conflicted,
                              COUNT(*) OVER() AS TOTAL_ROWS
					 FROM (SELECT ACC_ID, 
								  DESCRIPTION, 
								  FORM_DATE, 
								  FORM_REFERENCE, 
								  FORM_TYPE,
								  CASE 
									WHEN DEBIT < 0 THEN SUM(DEBIT * -1) 
									ELSE SUM(DEBIT)
								  END AS DEBIT,
								  CASE 
									WHEN CREDIT < 0 THEN SUM(CREDIT * -1)
									ELSE SUM(CREDIT)
								  END AS CREDIT, 
								  BALANCE as BALANCE,
								  GL_FLAG,
								  FORM_ID,
                                  "" as BEG_BAL,
                                  IS_Conflicted
							FROM ( SELECT ACC_ID,
										  DESCRIPTION,
										  FORM_DATE,
										  FORM_REFERENCE,
										  CASE 
												WHEN ee.GL_FLAG = 15 OR ee.GL_FLAG = 16 OR ee.GL_FLAG = 510 OR ee.GL_FLAG = 511 THEN ''Payment Sent''
												WHEN ee.GL_FLAG = 19 OR ee.GL_FLAG = 20 OR ee.GL_FLAG = 512 OR ee.GL_FLAG = 513 THEN ''Receive Money''
												WHEN ee.GL_FLAG = 101 OR ee.GL_FLAG = 26 OR ee.GL_FLAG = 23 OR ee.GL_FLAG = 201 OR ee.GL_FLAG = 102 OR ee.GL_FLAG = 203 OR ee.GL_FLAG = 103 OR ee.GL_FLAG = 104 OR ee.GL_FLAG = 105 OR ee.GL_FLAG = 106 OR ee.GL_FLAG = 5553 OR ee.GL_FLAG=5554 THEN ''Payments''
												WHEN ee.GL_FLAG = 107 OR ee.GL_FLAG = 29 OR ee.GL_FLAG = 28 OR ee.GL_FLAG = 204 OR ee.GL_FLAG = 205 OR ee.GL_FLAG = 108 OR ee.GL_FLAG = 109 OR ee.GL_FLAG = 110 OR ee.GL_FLAG = 111 OR ee.GL_FLAG = 113 OR ee.GL_FLAG = 114 OR ee.GL_FLAG = 112 OR ee.GL_FLAG = 5552 OR ee.GL_FLAG =5551 THEN ''Receipts''
												WHEN ee.GL_FLAG = 115 OR ee.GL_FLAG = 89 OR ee.GL_FLAG = 90 OR ee.GL_FLAG = 116 OR ee.GL_FLAG = 117 THEN ''CHARGES''
												WHEN ee.GL_FLAG = 31 OR ee.GL_FLAG = 32 OR ee.GL_FLAG = 33 OR ee.GL_FLAG = 34 THEN ''Partial Credit''
												WHEN ee.GL_FLAG = 37 OR ee.GL_FLAG = 38 THEN ''Receive Order''
												WHEN ee.GL_FLAG = 39 OR ee.GL_FLAG = 40 THEN ''Vendor Credit Memo''
												WHEN ee.GL_FLAG = 41 OR ee.GL_FLAG = 42 OR ee.GL_FLAG = 43 OR ee.GL_FLAG = 44 OR ee.GL_FLAG = 79 OR ee.GL_FLAG = 80 OR ee.GL_FLAG = 81 THEN ''Sale Invoice''
												WHEN ee.GL_FLAG = 45 OR ee.GL_FLAG = 46 OR ee.GL_FLAG = 47 OR ee.GL_FLAG = 48  OR ee.GL_FLAG = 82 OR ee.GL_FLAG = 83 OR ee.GL_FLAG = 84 THEN ''Sale Return''
												WHEN ee.GL_FLAG = 49 OR ee.GL_FLAG = 50 OR ee.GL_FLAG = 51 OR ee.GL_FLAG = 52 OR ee.GL_FLAG = 53 OR ee.GL_FLAG = 54 OR ee.GL_FLAG = 55 OR ee.GL_FLAG = 56  OR ee.GL_FLAG = 85 OR ee.GL_FLAG = 86 OR ee.GL_FLAG = 87 OR ee.GL_FLAG = 100 THEN ''Replacement''
												WHEN ee.GL_FLAG = 57 OR ee.GL_FLAG = 58 OR ee.GL_FLAG = 59 OR ee.GL_FLAG = 60 OR ee.GL_FLAG = 150 OR ee.GL_FLAG = 151 THEN ''Stock Transfer''
												WHEN ee.GL_FLAG = 62  OR ee.GL_FLAG = 64 THEN ''Stock In''
												WHEN ee.GL_FLAG = 65 OR ee.GL_FLAG = 66 OR ee.GL_FLAG = 67 OR ee.GL_FLAG = 68 OR ee.GL_FLAG = 69 OR ee.GL_FLAG = 70 THEN ''Adjustment''
												WHEN ee.GL_FLAG = 71 OR ee.GL_FLAG = 72 THEN ''General Journal''
												WHEN ee.GL_FLAG = 75 OR ee.GL_FLAG = 76 or ee.GL_FLAG = 77 OR ee.GL_FLAG = 78 THEN ''Repair IN''
												WHEN ee.GL_FLAG = 73 OR ee.GL_FLAG = 74 THEN ''Repair Out''
												WHEN ee.GL_FLAG = 00 THEN ''BEGNING BALANCE''
												ELSE ''''
										  END AS FORM_TYPE,
										  DEBIT,
										  CREDIT,
                                          CASE
											 WHEN FF.ACCOUNT_ID = 3 OR FF.ACCOUNT_ID = 2 OR FF.ACCOUNT_ID = 5 THEN SUM(IFNULL(DEBIT, 0) - IFNULL(CREDIT, 0)) OVER( PARTITION BY ACC_ID 
																																									   ORDER BY ACC_ID, DESCRIPTION, FORM_DATE, GL_FLAG, FORM_REFERENCE, FORM_ID, DETAIL_ID) 
											 ELSE SUM(IFNULL(CREDIT, 0) - IFNULL(DEBIT, 0)) OVER( PARTITION BY ACC_ID 
																									  ORDER BY ACC_ID, DESCRIPTION, FORM_DATE, GL_FLAG, FORM_REFERENCE, FORM_ID, DETAIL_ID) 
										  END AS BALANCE,
										  GL_FLAG,
										  FORM_ID,
                                          IS_CONFLICTED,
                                          DETAIL_ID
									 FROM (  
											 -- PAYMENT SENT --
											 SELECT CASE
													   WHEN (A.GL_FLAG = 16) THEN 
                                                        A.AMOUNT
													   when (A.GL_FLAG = 510) then A.Amount
													END AS DEBIT,
													CASE
													   WHEN (A.GL_FLAG = 15) THEN A.AMOUNT 
													   when (A.GL_FLAG = 511) then A.Amount
													END AS CREDIT,
													A.GL_ACC_ID AS ACCOUNT_ID,
													C.ACC_ID AS ACC_ID,
													 C.DESCRIPTION AS DESCRIPTION,
													 C.ACCOUNT_TYPE_ID AS ACCOUNT_TYPE_ID,
													A.GL_FLAG,
													A.FORM_DATE,
													A.FORM_REFERENCE,
													A.ID AS FORM_ID,
                                                    A.IS_CONFLICTED,
                                                    A.Form_DETAIL_ID AS DETAIL_ID
											   FROM (SELECT * FROM Payments_Accounting 
										 WHERE FORM_FLAG ="PaymentSent" AND CASE
													  WHEN \'',P_COMPANY_ID,'\' <> "" THEN COMPANY_ID = \'',P_COMPANY_ID,'\'
															  ELSE TRUE
																   END
									         AND ((CASE
											WHEN \'',P_ENTRY_DATE_FROM,'\' <> "" THEN FORM_DATE >= \'',P_ENTRY_DATE_FROM,'\'
											ELSE TRUE
										  END
									  AND CASE
											WHEN \'',P_ENTRY_DATE_TO,'\' <> "" THEN FORM_DATE <= \'',P_ENTRY_DATE_TO,'\'
											ELSE TRUE
										  END)
										    OR ((CASE
											WHEN \'',P_ENTRY_DATE_TO,'\' <> "" THEN FORM_DATE <= \'',P_ENTRY_DATE_TO,'\'
											ELSE TRUE
                                            END)
											AND IS_Conflicted=''N'') OR (CASE
																			WHEN \'',P_ENTRY_DATE_FROM,'\' <> "" THEN RECONCILE_DATE > \'',P_ENTRY_DATE_FROM,'\'
																			ELSE TRUE
																		  END
																	  AND CASE
																			WHEN \'',P_ENTRY_DATE_TO,'\' <> "" THEN RECONCILE_DATE <= \'',P_ENTRY_DATE_TO,'\'
																			ELSE TRUE
																		  END) 
                                                                          AND IS_Conflicted=''Y'')
											   )A JOIN (  SELECT ID, 
																								   ACC_ID, 
																								   DESCRIPTION, 
																								   ACCOUNT_TYPE_ID
																								 FROM ACCOUNTS_ID
																								WHERE CASE
																								 WHEN \'',P_COMPANY_ID,'\' <> "" THEN COMPANY_ID = \'',P_COMPANY_ID,'\'
																								   ELSE TRUE
																									  END ) C ON (A.GL_ACC_ID = C.ID) 
											 -- PAYMENT SENT --
									  
											 UNION ALL
									  
											 -- RECEIVE_MONEY -- 								  
											 SELECT CASE
													   WHEN (A.GL_FLAG = 19) THEN A.AMOUNT
													   when (A.GL_FLAG = 513) Then A.Amount
													END AS DEBIT,
													CASE
													   WHEN (A.GL_FLAG = 20) THEN A.AMOUNT 
													   when (A.GL_FLAG  = 512) Then A.Amount
													END AS CREDIT,
													A.GL_ACC_ID AS ACCOUNT_ID,
													C.ACC_ID AS ACC_ID,
													 C.DESCRIPTION AS DESCRIPTION,
													 C.ACCOUNT_TYPE_ID AS ACCOUNT_TYPE_ID,
													A.GL_FLAG,
													A.FORM_DATE,
													A.FORM_REFERENCE,
													A.ID AS FORM_ID,
                                                    A.IS_CONFLICTED,
                                                    A.Form_Detail_ID AS DETAIL_ID
											   FROM (SELECT * FROM Payments_Accounting 
											 WHERE FORM_FLAG = "ReceiveMoney" AND CASE
													  WHEN \'',P_COMPANY_ID,'\' <> "" THEN COMPANY_ID = \'',P_COMPANY_ID,'\'
															  ELSE TRUE
																   END
									         AND ((CASE
											WHEN \'',P_ENTRY_DATE_FROM,'\' <> "" THEN FORM_DATE >= \'',P_ENTRY_DATE_FROM,'\'
											ELSE TRUE
										  END
									  AND CASE
											WHEN \'',P_ENTRY_DATE_TO,'\' <> "" THEN FORM_DATE <= \'',P_ENTRY_DATE_TO,'\'
											ELSE TRUE
										  END)
										    OR ((CASE
											WHEN \'',P_ENTRY_DATE_TO,'\' <> "" THEN FORM_DATE <= \'',P_ENTRY_DATE_TO,'\'
											ELSE TRUE
                                            END)
											AND IS_Conflicted=''N'') OR (CASE
																			WHEN \'',P_ENTRY_DATE_FROM,'\' <> "" THEN RECONCILE_DATE > \'',P_ENTRY_DATE_FROM,'\'
																			ELSE TRUE
																		  END
																	  AND CASE
																			WHEN \'',P_ENTRY_DATE_TO,'\' <> "" THEN RECONCILE_DATE <= \'',P_ENTRY_DATE_TO,'\'
																			ELSE TRUE
																		  END) 
                                                                          AND IS_Conflicted=''Y'')
											  )A JOIN  (  SELECT ID, 
																								   ACC_ID, 
																								   DESCRIPTION, 
																								   ACCOUNT_TYPE_ID
																								 FROM ACCOUNTS_ID
																								WHERE CASE
																								 WHEN \'',P_COMPANY_ID,'\' <> "" THEN COMPANY_ID = \'',P_COMPANY_ID,'\'
																								   ELSE TRUE
																									  END ) C ON (A.GL_ACC_ID = C.ID) 
											 -- RECEIVE_MONEY --
											  
											 UNION ALL
											  
											 -- PAYMENTS --											
											 SELECT CASE
													   When (A.GL_FLAG = 26) then A.Amount 
													   When (A.GL_FLAG = 201) then A.Amount 
													   When (A.GL_FLAG = 203) then A.Amount 
													   When (A.GL_FLAG = 103) then A.Amount 
													   When (A.GL_FLAG = 105) then A.Amount 
													   When (A.GL_FLAG = 5553) then A.Amount
													END AS DEBIT,
													CASE
													   When (A.GL_FLAG = 101) then A.Amount 
													   When (A.GL_FLAG = 23)  then A.Amount 
													   When (A.GL_FLAG = 102) then A.Amount 
													   When (A.GL_FLAG = 104) then A.Amount 
													   When (A.GL_FLAG = 106) then A.Amount 
													   When (A.GL_FLAG = 5554) then A.Amount													   
													END AS CREDIT,
													A.GL_ACC_ID AS ACCOUNT_ID,
													C.ACC_ID AS ACC_ID,
													 C.DESCRIPTION AS DESCRIPTION,
													 C.ACCOUNT_TYPE_ID AS ACCOUNT_TYPE_ID,
													A.GL_FLAG,
													A.FORM_DATE,
													A.FORM_REFERENCE,
													A.ID AS FORM_ID,
                                                     A.IS_CONFLICTED,
                                                     A.Form_DETAIL_ID AS DETAIL_ID
											   FROM  (SELECT * FROM Payments_Accounting 
								 WHERE FORM_FLAG = "Payments" and CASE
													  WHEN \'',P_COMPANY_ID,'\' <> "" THEN COMPANY_ID = \'',P_COMPANY_ID,'\'
															  ELSE TRUE
																   END
									         AND ((CASE
											WHEN \'',P_ENTRY_DATE_FROM,'\' <> "" THEN FORM_DATE >= \'',P_ENTRY_DATE_FROM,'\'
											ELSE TRUE
										  END
									  AND CASE
											WHEN \'',P_ENTRY_DATE_TO,'\' <> "" THEN FORM_DATE <= \'',P_ENTRY_DATE_TO,'\'
											ELSE TRUE
										  END)
										    OR ((CASE
											WHEN \'',P_ENTRY_DATE_TO,'\' <> "" THEN FORM_DATE <= \'',P_ENTRY_DATE_TO,'\'
											ELSE TRUE
                                            END)
											AND IS_Conflicted=''N'') OR (CASE
																			WHEN \'',P_ENTRY_DATE_FROM,'\' <> "" THEN RECONCILE_DATE > \'',P_ENTRY_DATE_FROM,'\'
																			ELSE TRUE
																		  END
																	  AND CASE
																			WHEN \'',P_ENTRY_DATE_TO,'\' <> "" THEN RECONCILE_DATE <= \'',P_ENTRY_DATE_TO,'\'
																			ELSE TRUE
																		  END) 
                                                                          AND IS_Conflicted=''Y'')
											   )A JOIN  (  SELECT ID, 
																								   ACC_ID, 
																								   DESCRIPTION, 
																								   ACCOUNT_TYPE_ID
																								 FROM ACCOUNTS_ID
																								WHERE CASE
																								 WHEN \'',P_COMPANY_ID,'\' <> "" THEN COMPANY_ID = \'',P_COMPANY_ID,'\'
																								   ELSE TRUE
																									  END ) C ON (A.GL_ACC_ID = C.ID) 
											 -- PAYMENTS --  
											   
											 UNION ALL
												
											 -- RECEIPTS --											
											 SELECT CASE
														When (A.GL_FLAG = 107) then A.Amount 
														When (A.GL_FLAG = 204) then A.Amount 
														When (A.GL_FLAG = 205) then A.Amount 
														When (A.GL_FLAG = 110) then A.Amount 
														When (A.GL_FLAG = 113) then A.Amount 
														When (A.GL_FLAG = 112) then A.Amount 
														When (A.GL_FLAG = 5551) then A.Amount											 
													END AS DEBIT,
													CASE
													   When (A.GL_FLAG = 29) then A.Amount 
													   When (A.GL_FLAG = 28) then A.Amount 
													   When (A.GL_FLAG = 108) then A.Amount 
													   When (A.GL_FLAG = 109) then A.Amount 
													   When (A.GL_FLAG = 111) then A.Amount 
													   When (A.GL_FLAG = 114) then A.Amount 
													   When (A.GL_FLAG = 5552) then A.Amount												
													END AS CREDIT,
													A.GL_ACC_ID AS ACCOUNT_ID,
													C.ACC_ID AS ACC_ID,
													 C.DESCRIPTION AS DESCRIPTION,
													 C.ACCOUNT_TYPE_ID AS ACCOUNT_TYPE_ID,
													A.GL_FLAG,
													A.FORM_DATE,
													A.FORM_REFERENCE,
													A.ID AS FORM_ID,
													A.IS_CONFLICTED,
                                                    A.Form_DETAIL_ID AS DETAIL_ID
											   FROM (SELECT * FROM  Payments_Accounting 
									 WHERE FORM_FLAG = "RECEIPTS" AND CASE
													  WHEN \'',P_COMPANY_ID,'\' <> "" THEN COMPANY_ID = \'',P_COMPANY_ID,'\'
															  ELSE TRUE
																   END
									         AND ((CASE
											WHEN \'',P_ENTRY_DATE_FROM,'\' <> "" THEN FORM_DATE >= \'',P_ENTRY_DATE_FROM,'\'
											ELSE TRUE
										  END
									  AND CASE
											WHEN \'',P_ENTRY_DATE_TO,'\' <> "" THEN FORM_DATE <= \'',P_ENTRY_DATE_TO,'\'
											ELSE TRUE
										  END)
										    OR ((CASE
											WHEN \'',P_ENTRY_DATE_TO,'\' <> "" THEN FORM_DATE <= \'',P_ENTRY_DATE_TO,'\'
											ELSE TRUE
                                            END)
											AND IS_Conflicted=''N'') OR (CASE
																			WHEN \'',P_ENTRY_DATE_FROM,'\' <> "" THEN RECONCILE_DATE > \'',P_ENTRY_DATE_FROM,'\'
																			ELSE TRUE
																		  END
																	  AND CASE
																			WHEN \'',P_ENTRY_DATE_TO,'\' <> "" THEN RECONCILE_DATE <= \'',P_ENTRY_DATE_TO,'\'
																			ELSE TRUE
																		  END) 
                                                                          AND IS_Conflicted=''Y'')
											   )A JOIN  (  SELECT ID, 
																								   ACC_ID, 
																								   DESCRIPTION, 
																								   ACCOUNT_TYPE_ID
																								 FROM ACCOUNTS_ID
																								WHERE CASE
																								 WHEN \'',P_COMPANY_ID,'\' <> "" THEN COMPANY_ID = \'',P_COMPANY_ID,'\'
																								   ELSE TRUE
																									  END ) C ON (A.GL_ACC_ID = C.ID) 
												
											 UNION ALL
												
											 -- PARTIAL_CREDIT --
											 SELECT CASE
													   WHEN (A.GL_FLAG = 32) THEN A.AMOUNT
													   WHEN (A.GL_FLAG = 33) THEN A.AMOUNT
													END AS DEBIT,
													CASE
													   WHEN (A.GL_FLAG = 31) THEN A.AMOUNT 
													   WHEN (A.GL_FLAG = 34) THEN A.AMOUNT 
													END AS CREDIT,
													  A.GL_ACC_ID AS ACCOUNT_ID,
													C.ACC_ID AS ACC_ID,
													 C.DESCRIPTION AS DESCRIPTION,
													 C.ACCOUNT_TYPE_ID AS ACCOUNT_TYPE_ID,
													A.GL_FLAG,
													A.FORM_DATE,
													A.FORM_REFERENCE,
													A.ID AS FORM_ID,
													A.IS_CONFLICTED,
                                                    A.Form_DETAIL_ID AS DETAIL_ID
											   FROM  (SELECT * FROM Purchase_Accounting 
						 WHERE FORM_FLAG = "PartialCredit" and CASE
													  WHEN \'',P_COMPANY_ID,'\' <> "" THEN COMPANY_ID = \'',P_COMPANY_ID,'\'
															  ELSE TRUE
																   END
									         AND ((CASE
											WHEN \'',P_ENTRY_DATE_FROM,'\' <> "" THEN FORM_DATE >= \'',P_ENTRY_DATE_FROM,'\'
											ELSE TRUE
										  END
									  AND CASE
											WHEN \'',P_ENTRY_DATE_TO,'\' <> "" THEN FORM_DATE <= \'',P_ENTRY_DATE_TO,'\'
											ELSE TRUE
										  END)
										    OR ((CASE
											WHEN \'',P_ENTRY_DATE_TO,'\' <> "" THEN FORM_DATE <= \'',P_ENTRY_DATE_TO,'\'
											ELSE TRUE
                                            END)
											AND IS_Conflicted=''N'') OR (CASE
																			WHEN \'',P_ENTRY_DATE_FROM,'\' <> "" THEN RECONCILE_DATE > \'',P_ENTRY_DATE_FROM,'\'
																			ELSE TRUE
																		  END
																	  AND CASE
																			WHEN \'',P_ENTRY_DATE_TO,'\' <> "" THEN RECONCILE_DATE <= \'',P_ENTRY_DATE_TO,'\'
																			ELSE TRUE
																		  END) 
                                                                          AND IS_Conflicted=''Y'')
											   )A JOIN  (  SELECT ID, 
																								   ACC_ID, 
																								   DESCRIPTION, 
																								   ACCOUNT_TYPE_ID
																								 FROM ACCOUNTS_ID
																								WHERE CASE
																								 WHEN \'',P_COMPANY_ID,'\' <> "" THEN COMPANY_ID = \'',P_COMPANY_ID,'\'
																								   ELSE TRUE
																									  END ) C ON (A.GL_ACC_ID = C.ID) 
											 -- PARTIAL_CREDIT --
												
											 UNION ALL
												
											 -- RECEIVE_ORDER --											
											 SELECT CASE
													   WHEN (A.GL_FLAG = 37) THEN A.AMOUNT
													END AS DEBIT,
													CASE
													   WHEN (A.GL_FLAG = 38) THEN A.AMOUNT 
													END AS CREDIT,
													  A.GL_ACC_ID AS ACCOUNT_ID,
													C.ACC_ID AS ACC_ID,
													 C.DESCRIPTION AS DESCRIPTION,
													 C.ACCOUNT_TYPE_ID AS ACCOUNT_TYPE_ID,
													A.GL_FLAG,
													A.FORM_DATE,
													A.FORM_REFERENCE,
													A.ID AS FORM_ID,
													A.IS_CONFLICTED,
                                                    A.Form_DETAIL_ID AS DETAIL_ID
											   FROM ( SELECT * FROM Purchase_Accounting 
									 WHERE FORM_FLAG = "ReceiveOrder" and CASE
													  WHEN \'',P_COMPANY_ID,'\' <> "" THEN COMPANY_ID = \'',P_COMPANY_ID,'\'
															  ELSE TRUE
																   END
									         AND ((CASE
											WHEN \'',P_ENTRY_DATE_FROM,'\' <> "" THEN FORM_DATE >= \'',P_ENTRY_DATE_FROM,'\'
											ELSE TRUE
										  END
									  AND CASE
											WHEN \'',P_ENTRY_DATE_TO,'\' <> "" THEN FORM_DATE <= \'',P_ENTRY_DATE_TO,'\'
											ELSE TRUE
										  END)
										    OR ((CASE
											WHEN \'',P_ENTRY_DATE_TO,'\' <> "" THEN FORM_DATE <= \'',P_ENTRY_DATE_TO,'\'
											ELSE TRUE
                                            END)
											AND IS_Conflicted=''N'') OR (CASE
																			WHEN \'',P_ENTRY_DATE_FROM,'\' <> "" THEN RECONCILE_DATE > \'',P_ENTRY_DATE_FROM,'\'
																			ELSE TRUE
																		  END
																	  AND CASE
																			WHEN \'',P_ENTRY_DATE_TO,'\' <> "" THEN RECONCILE_DATE <= \'',P_ENTRY_DATE_TO,'\'
																			ELSE TRUE
																		  END) 
                                                                          AND IS_Conflicted=''Y'')
											   )A JOIN  (  SELECT ID, 
																								   ACC_ID, 
																								   DESCRIPTION, 
																								   ACCOUNT_TYPE_ID
																								 FROM ACCOUNTS_ID
																								WHERE CASE
																								 WHEN \'',P_COMPANY_ID,'\' <> "" THEN COMPANY_ID = \'',P_COMPANY_ID,'\'
																								   ELSE TRUE
																									  END ) C ON (A.GL_ACC_ID = C.ID) 
											 -- RECEIVE_ORDER --
												
											 UNION ALL
												
											 -- VENDOR_CREDIT_MEMO --											
											 SELECT CASE
													   WHEN (A.GL_FLAG = 39) THEN A.AMOUNT
													END AS DEBIT,
													CASE
													   WHEN (A.GL_FLAG = 40) THEN A.AMOUNT 
													END AS CREDIT,
													A.GL_ACC_ID AS ACCOUNT_ID,
													C.ACC_ID AS ACC_ID,
													 C.DESCRIPTION AS DESCRIPTION,
													 C.ACCOUNT_TYPE_ID AS ACCOUNT_TYPE_ID,
													A.GL_FLAG,
													A.FORM_DATE,
													A.FORM_REFERENCE,
													A.ID AS FORM_ID,
													A.IS_CONFLICTED,
                                                    A.Form_DETAIL_ID AS DETAIL_ID
											   FROM  (SELECT * FROM Payments_Accounting
									 WHERE FORM_FLAG="VendorCreditMemo" and CASE
													  WHEN \'',P_COMPANY_ID,'\' <> "" THEN COMPANY_ID = \'',P_COMPANY_ID,'\'
															  ELSE TRUE
																   END
									         AND ((CASE
											WHEN \'',P_ENTRY_DATE_FROM,'\' <> "" THEN FORM_DATE >= \'',P_ENTRY_DATE_FROM,'\'
											ELSE TRUE
										  END
									  AND CASE
											WHEN \'',P_ENTRY_DATE_TO,'\' <> "" THEN FORM_DATE <= \'',P_ENTRY_DATE_TO,'\'
											ELSE TRUE
										  END)
										    OR ((CASE
											WHEN \'',P_ENTRY_DATE_TO,'\' <> "" THEN FORM_DATE <= \'',P_ENTRY_DATE_TO,'\'
											ELSE TRUE
                                            END)
											AND IS_Conflicted=''N'') OR (CASE
																			WHEN \'',P_ENTRY_DATE_FROM,'\' <> "" THEN RECONCILE_DATE > \'',P_ENTRY_DATE_FROM,'\'
																			ELSE TRUE
																		  END
																	  AND CASE
																			WHEN \'',P_ENTRY_DATE_TO,'\' <> "" THEN RECONCILE_DATE <= \'',P_ENTRY_DATE_TO,'\'
																			ELSE TRUE
																		  END) 
                                                                          AND IS_Conflicted=''Y'')
											   )A JOIN  (  SELECT ID, 
																								   ACC_ID, 
																								   DESCRIPTION, 
																								   ACCOUNT_TYPE_ID
																								 FROM ACCOUNTS_ID
																								WHERE CASE
																								 WHEN \'',P_COMPANY_ID,'\' <> "" THEN COMPANY_ID = \'',P_COMPANY_ID,'\'
																								   ELSE TRUE
																									  END ) C ON (A.GL_ACC_ID = C.ID) 
											 -- VENDOR_CREDIT_MEMO --
												
											 UNION ALL
											 
											 -- SALE INVOICE --
											 SELECT CASE
													   WHEN (A.GL_FLAG = 41) THEN A.AMOUNT
													   WHEN (A.GL_FLAG = 43) THEN A.AMOUNT
													END AS DEBIT,
													CASE
													   WHEN (A.GL_FLAG = 42) THEN A.AMOUNT 
													   WHEN (A.GL_FLAG = 44) THEN A.AMOUNT 
													   WHEN (A.GL_FLAG = 79) THEN A.AMOUNT 
													   WHEN (A.GL_FLAG = 80) THEN A.AMOUNT 
													   WHEN (A.GL_FLAG = 81) THEN A.AMOUNT 
													END AS CREDIT,
												A.GL_ACC_ID AS ACCOUNT_ID,
													C.ACC_ID AS ACC_ID,
													 C.DESCRIPTION AS DESCRIPTION,
													 C.ACCOUNT_TYPE_ID AS ACCOUNT_TYPE_ID,
													A.GL_FLAG,
													A.FORM_DATE,
													A.FORM_REFERENCE,
													A.ID AS FORM_ID,
													A.IS_CONFLICTED,
                                                    A.Form_DETAIL_ID AS DETAIL_ID
											   FROM  (SELECT * FROM Sales_Accounting 
												 WHERE Form_Flag = "SaleInvoice" and CASE
													  WHEN \'',P_COMPANY_ID,'\' <> "" THEN COMPANY_ID = \'',P_COMPANY_ID,'\'
															  ELSE TRUE
																   END
									         AND ((CASE
											WHEN \'',P_ENTRY_DATE_FROM,'\' <> "" THEN FORM_DATE >= \'',P_ENTRY_DATE_FROM,'\'
											ELSE TRUE
										  END
									  AND CASE
											WHEN \'',P_ENTRY_DATE_TO,'\' <> "" THEN FORM_DATE <= \'',P_ENTRY_DATE_TO,'\'
											ELSE TRUE
										  END)
										    OR ((CASE
											WHEN \'',P_ENTRY_DATE_TO,'\' <> "" THEN FORM_DATE <= \'',P_ENTRY_DATE_TO,'\'
											ELSE TRUE
                                            END)
											AND IS_Conflicted=''N'') OR (CASE
																			WHEN \'',P_ENTRY_DATE_FROM,'\' <> "" THEN RECONCILE_DATE > \'',P_ENTRY_DATE_FROM,'\'
																			ELSE TRUE
																		  END
																	  AND CASE
																			WHEN \'',P_ENTRY_DATE_TO,'\' <> "" THEN RECONCILE_DATE <= \'',P_ENTRY_DATE_TO,'\'
																			ELSE TRUE
																		  END) 
                                                                          AND IS_Conflicted=''Y'')
											   )A JOIN  (  SELECT ID, 
																								   ACC_ID, 
																								   DESCRIPTION, 
																								   ACCOUNT_TYPE_ID
																								 FROM ACCOUNTS_ID
																								WHERE CASE
																								 WHEN \'',P_COMPANY_ID,'\' <> "" THEN COMPANY_ID = \'',P_COMPANY_ID,'\'
																								   ELSE TRUE
																									  END ) C ON (A.GL_ACC_ID = C.ID)            
											 -- SALE INVOICE --
												
											 UNION ALL
												
											 -- SALE_RETURN --		
											 
											 SELECT CASE
														
													  WHEN (A.GL_FLAG = 45) THEN A.AMOUNT
													  WHEN (A.GL_FLAG = 48) THEN A.AMOUNT
													   WHEN (A.GL_FLAG = 82) THEN A.AMOUNT 
													   WHEN (A.GL_FLAG = 83) THEN A.AMOUNT 
														WHEN (A.GL_FLAG = 84) THEN A.AMOUNT 
													END AS DEBIT,
													CASE
													  WHEN (A.GL_FLAG = 46) THEN A.AMOUNT 
													   WHEN (A.GL_FLAG = 47) THEN A.AMOUNT 
													END AS CREDIT,
													A.GL_ACC_ID AS ACCOUNT_ID,
													C.ACC_ID AS ACC_ID,
													 C.DESCRIPTION AS DESCRIPTION,
													 C.ACCOUNT_TYPE_ID AS ACCOUNT_TYPE_ID,
													A.GL_FLAG,
													A.FORM_DATE,
													A.FORM_REFERENCE,
													A.ID AS FORM_ID,
                                                    A.IS_CONFLICTED,
                                                    A.Form_DETAIL_ID AS DETAIL_ID
											   FROM  (SELECT * FROM Sales_Accounting 
		                                  WHERE FORM_FLAG = "SaleReturn" and CASE
													  WHEN \'',P_COMPANY_ID,'\' <> "" THEN COMPANY_ID = \'',P_COMPANY_ID,'\'
															  ELSE TRUE
																   END
									         AND ((CASE
											WHEN \'',P_ENTRY_DATE_FROM,'\' <> "" THEN FORM_DATE >= \'',P_ENTRY_DATE_FROM,'\'
											ELSE TRUE
										  END
									  AND CASE
											WHEN \'',P_ENTRY_DATE_TO,'\' <> "" THEN FORM_DATE <= \'',P_ENTRY_DATE_TO,'\'
											ELSE TRUE
										  END)
										    OR ((CASE
											WHEN \'',P_ENTRY_DATE_TO,'\' <> "" THEN FORM_DATE <= \'',P_ENTRY_DATE_TO,'\'
											ELSE TRUE
                                            END)
											AND IS_Conflicted=''N'') OR (CASE
																			WHEN \'',P_ENTRY_DATE_FROM,'\' <> "" THEN RECONCILE_DATE > \'',P_ENTRY_DATE_FROM,'\'
																			ELSE TRUE
																		  END
																	  AND CASE
																			WHEN \'',P_ENTRY_DATE_TO,'\' <> "" THEN RECONCILE_DATE <= \'',P_ENTRY_DATE_TO,'\'
																			ELSE TRUE
																		  END) 
                                                                          AND IS_Conflicted=''Y'')
											   )A JOIN  (  SELECT ID, 
																								   ACC_ID, 
																								   DESCRIPTION, 
																								   ACCOUNT_TYPE_ID
																								 FROM ACCOUNTS_ID
																								WHERE CASE
																								 WHEN \'',P_COMPANY_ID,'\' <> "" THEN COMPANY_ID = \'',P_COMPANY_ID,'\'
																								   ELSE TRUE
																									  END ) C ON (A.GL_ACC_ID = C.ID)        
																									  
																						  
																									  
											 -- SALE_RETURN --
											 UNION ALL
												
											-- REPLACEMENT --
											 SELECT CASE
													   WHEN (A.GL_FLAG = 49) THEN A.AMOUNT
													   WHEN (A.GL_FLAG = 52) THEN A.AMOUNT
													   WHEN (A.GL_FLAG = 100) THEN A.AMOUNT
													   WHEN (A.GL_FLAG = 53) THEN A.AMOUNT
													   WHEN (A.GL_FLAG = 55) THEN A.AMOUNT
													END AS DEBIT,
													CASE
														WHEN (A.GL_FLAG = 50) THEN A.AMOUNT
														WHEN (A.GL_FLAG = 51) THEN A.AMOUNT 
														WHEN (A.GL_FLAG = 54) THEN A.AMOUNT 
														WHEN (A.GL_FLAG = 56) THEN A.AMOUNT 
														WHEN (A.GL_FLAG = 86) THEN A.AMOUNT
														WHEN (A.GL_FLAG = 87) THEN A.AMOUNT 
															WHEN (A.GL_FLAG = 85) THEN CASE 
																		 WHEN A.AMOUNT < 0 THEN (A.AMOUNT * -1) 
																		 ELSE A.AMOUNT
																	   END
													END AS CREDIT,
													A.GL_ACC_ID AS ACCOUNT_ID,
													C.ACC_ID AS ACC_ID,
													 C.DESCRIPTION AS DESCRIPTION,
													 C.ACCOUNT_TYPE_ID AS ACCOUNT_TYPE_ID,
													A.GL_FLAG,
													A.FORM_DATE,
													A.FORM_REFERENCE,
													A.ID AS FORM_ID,
													A.IS_CONFLICTED,
                                                    A.Form_DETAIL_ID AS DETAIL_ID
											   FROM  (SELECT * FROM Sales_Accounting 
												 WHERE FORM_FLAG="Replacement" and CASE
													  WHEN \'',P_COMPANY_ID,'\' <> "" THEN COMPANY_ID = \'',P_COMPANY_ID,'\'
															  ELSE TRUE
																   END
									         AND ((CASE
											WHEN \'',P_ENTRY_DATE_FROM,'\' <> "" THEN FORM_DATE >= \'',P_ENTRY_DATE_FROM,'\'
											ELSE TRUE
										  END
									  AND CASE
											WHEN \'',P_ENTRY_DATE_TO,'\' <> "" THEN FORM_DATE <= \'',P_ENTRY_DATE_TO,'\'
											ELSE TRUE
										  END)
										    OR ((CASE
											WHEN \'',P_ENTRY_DATE_TO,'\' <> "" THEN FORM_DATE <= \'',P_ENTRY_DATE_TO,'\'
											ELSE TRUE
                                            END)
											AND IS_Conflicted=''N'') OR (CASE
																			WHEN \'',P_ENTRY_DATE_FROM,'\' <> "" THEN RECONCILE_DATE > \'',P_ENTRY_DATE_FROM,'\'
																			ELSE TRUE
																		  END
																	  AND CASE
																			WHEN \'',P_ENTRY_DATE_TO,'\' <> "" THEN RECONCILE_DATE <= \'',P_ENTRY_DATE_TO,'\'
																			ELSE TRUE
																		  END) 
                                                                          AND IS_Conflicted=''Y'')
											   )A JOIN  (  SELECT ID, 
																								   ACC_ID, 
																								   DESCRIPTION, 
																								   ACCOUNT_TYPE_ID
																								 FROM ACCOUNTS_ID
																								WHERE CASE
																								 WHEN \'',P_COMPANY_ID,'\' <> "" THEN COMPANY_ID = \'',P_COMPANY_ID,'\'
																								   ELSE TRUE
																									  END ) C ON (A.GL_ACC_ID = C.ID)                                                   
											 -- REPLACEMENT --
												
											 UNION ALL
												
											 -- STOCK_TRANSFER --											
											 SELECT CASE
													   WHEN (A.GL_FLAG = 57) THEN A.AMOUNT
													   WHEN (A.GL_FLAG = 59) THEN A.AMOUNT
													END AS DEBIT,
													CASE
													   WHEN (A.GL_FLAG = 58) THEN A.AMOUNT 
													   WHEN (A.GL_FLAG = 60) THEN A.AMOUNT
													   WHEN (A.GL_FLAG = 150) THEN A.AMOUNT
													   WHEN (A.GL_FLAG = 151) THEN A.AMOUNT										
													END AS CREDIT,
														A.GL_ACC_ID AS ACCOUNT_ID,
													C.ACC_ID AS ACC_ID,
													 C.DESCRIPTION AS DESCRIPTION,
													 C.ACCOUNT_TYPE_ID AS ACCOUNT_TYPE_ID,
													A.GL_FLAG,
													A.FORM_DATE,
													A.FORM_REFERENCE,
													A.ID AS FORM_ID,
													A.IS_CONFLICTED,
                                                    A.Form_DETAIL_ID AS DETAIL_ID
											   FROM  (SELECT * FROM Stock_Accounting 
										 WHERE FORM_FLAG = "StockTransfer" and CASE
													  WHEN \'',P_COMPANY_ID,'\' <> "" THEN COMPANY_ID = \'',P_COMPANY_ID,'\'
															  ELSE TRUE
																   END
									         AND ((CASE
											WHEN \'',P_ENTRY_DATE_FROM,'\' <> "" THEN FORM_DATE >= \'',P_ENTRY_DATE_FROM,'\'
											ELSE TRUE
										  END
									  AND CASE
											WHEN \'',P_ENTRY_DATE_TO,'\' <> "" THEN FORM_DATE <= \'',P_ENTRY_DATE_TO,'\'
											ELSE TRUE
										  END)
										    OR ((CASE
											WHEN \'',P_ENTRY_DATE_TO,'\' <> "" THEN FORM_DATE <= \'',P_ENTRY_DATE_TO,'\'
											ELSE TRUE
                                            END)
											AND IS_Conflicted=''N'') OR (CASE
																			WHEN \'',P_ENTRY_DATE_FROM,'\' <> "" THEN RECONCILE_DATE > \'',P_ENTRY_DATE_FROM,'\'
																			ELSE TRUE
																		  END
																	  AND CASE
																			WHEN \'',P_ENTRY_DATE_TO,'\' <> "" THEN RECONCILE_DATE <= \'',P_ENTRY_DATE_TO,'\'
																			ELSE TRUE
																		  END) 
                                                                          AND IS_Conflicted=''Y'')
											   )A JOIN  (  SELECT ID, 
																								   ACC_ID, 
																								   DESCRIPTION, 
																								   ACCOUNT_TYPE_ID
																								 FROM ACCOUNTS_ID
																								WHERE CASE
																								 WHEN \'',P_COMPANY_ID,'\' <> "" THEN COMPANY_ID = \'',P_COMPANY_ID,'\'
																								   ELSE TRUE
																									  END ) C ON (A.GL_ACC_ID = C.ID)       
											 -- STOCK_TRANSFER --
												
											 UNION ALL
												
											 -- STOCK_IN --
											 SELECT CASE
														  WHEN (A.GL_FLAG = 64) THEN A.AMOUNT
													END AS DEBIT,
													CASE
													   WHEN (A.GL_FLAG = 62) THEN A.AMOUNT 
													END AS CREDIT,
													A.GL_ACC_ID AS ACCOUNT_ID,
													C.ACC_ID AS ACC_ID,
													 C.DESCRIPTION AS DESCRIPTION,
													 C.ACCOUNT_TYPE_ID AS ACCOUNT_TYPE_ID,
													A.GL_FLAG,
													A.FORM_DATE,
													A.FORM_REFERENCE,
													A.ID AS FORM_ID,
													A.IS_CONFLICTED,
                                                    A.Form_DETAIL_ID AS DETAIL_ID
											   FROM  (SELECT * FROM Stock_Accounting 
										 WHERE FORM_FLAG = "StockIn" and CASE
													  WHEN \'',P_COMPANY_ID,'\' <> "" THEN COMPANY_ID = \'',P_COMPANY_ID,'\'
															  ELSE TRUE
																   END
									         AND ((CASE
											WHEN \'',P_ENTRY_DATE_FROM,'\' <> "" THEN FORM_DATE >= \'',P_ENTRY_DATE_FROM,'\'
											ELSE TRUE
										  END
									  AND CASE
											WHEN \'',P_ENTRY_DATE_TO,'\' <> "" THEN FORM_DATE <= \'',P_ENTRY_DATE_TO,'\'
											ELSE TRUE
										  END)
										    OR ((CASE
											WHEN \'',P_ENTRY_DATE_TO,'\' <> "" THEN FORM_DATE <= \'',P_ENTRY_DATE_TO,'\'
											ELSE TRUE
                                            END)
											AND IS_Conflicted=''N'') OR (CASE
																			WHEN \'',P_ENTRY_DATE_FROM,'\' <> "" THEN RECONCILE_DATE > \'',P_ENTRY_DATE_FROM,'\'
																			ELSE TRUE
																		  END
																	  AND CASE
																			WHEN \'',P_ENTRY_DATE_TO,'\' <> "" THEN RECONCILE_DATE <= \'',P_ENTRY_DATE_TO,'\'
																			ELSE TRUE
																		  END) 
                                                                          AND IS_Conflicted=''Y'')
											   )A JOIN  (  SELECT ID, 
																								   ACC_ID, 
																								   DESCRIPTION, 
																								   ACCOUNT_TYPE_ID
																								 FROM ACCOUNTS_ID
																								WHERE CASE
																								 WHEN \'',P_COMPANY_ID,'\' <> "" THEN COMPANY_ID = \'',P_COMPANY_ID,'\'
																								   ELSE TRUE
																									  END ) C ON (A.GL_ACC_ID = C.ID)    
											 -- STOCK_IN --
												
											 UNION ALL
												
											 -- ADJUSTMENT --											
											 SELECT CASE
													   WHEN (A.GL_FLAG = 66) THEN A.AMOUNT
													   WHEN (A.GL_FLAG = 67) THEN A.AMOUNT
													   WHEN (A.GL_FLAG = 69) THEN A.AMOUNT
													END AS DEBIT,
													CASE
													   WHEN (A.GL_FLAG = 65) THEN A.AMOUNT 
													   WHEN (A.GL_FLAG = 68) THEN A.AMOUNT 
													   WHEN (A.GL_FLAG = 70) THEN A.AMOUNT 
													END AS CREDIT,
													A.GL_ACC_ID AS ACCOUNT_ID,
													C.ACC_ID AS ACC_ID,
													 C.DESCRIPTION AS DESCRIPTION,
													 C.ACCOUNT_TYPE_ID AS ACCOUNT_TYPE_ID,
													A.GL_FLAG,
													A.FORM_DATE,
													A.FORM_REFERENCE,
													A.ID AS FORM_ID,
													A.IS_CONFLICTED,
                                                    A.Form_DETAIL_ID AS DETAIL_ID
											   FROM  (SELECT * FROM Adjustment_Accounting 
									 WHERE Form_Flag = "Adjustment" and CASE
													  WHEN \'',P_COMPANY_ID,'\' <> "" THEN COMPANY_ID = \'',P_COMPANY_ID,'\'
															  ELSE TRUE
																   END
									         AND ((CASE
											WHEN \'',P_ENTRY_DATE_FROM,'\' <> "" THEN FORM_DATE >= \'',P_ENTRY_DATE_FROM,'\'
											ELSE TRUE
										  END
									  AND CASE
											WHEN \'',P_ENTRY_DATE_TO,'\' <> "" THEN FORM_DATE <= \'',P_ENTRY_DATE_TO,'\'
											ELSE TRUE
										  END)
										    OR ((CASE
											WHEN \'',P_ENTRY_DATE_TO,'\' <> "" THEN FORM_DATE <= \'',P_ENTRY_DATE_TO,'\'
											ELSE TRUE
                                            END)
											AND IS_Conflicted=''N'') OR (CASE
																			WHEN \'',P_ENTRY_DATE_FROM,'\' <> "" THEN RECONCILE_DATE > \'',P_ENTRY_DATE_FROM,'\'
																			ELSE TRUE
																		  END
																	  AND CASE
																			WHEN \'',P_ENTRY_DATE_TO,'\' <> "" THEN RECONCILE_DATE <= \'',P_ENTRY_DATE_TO,'\'
																			ELSE TRUE
																		  END) 
                                                                          AND IS_Conflicted=''Y'')
											   )A JOIN  (  SELECT ID, 
																								   ACC_ID, 
																								   DESCRIPTION, 
																								   ACCOUNT_TYPE_ID
																								 FROM ACCOUNTS_ID
																								WHERE CASE
																								 WHEN \'',P_COMPANY_ID,'\' <> "" THEN COMPANY_ID = \'',P_COMPANY_ID,'\'
																								   ELSE TRUE
																									  END ) C ON (A.GL_ACC_ID = C.ID)  
											 -- ADJUSTMENT --
												
											 UNION ALL
												
											 -- GENERAL_JOURNAL --										 
											 SELECT CASE
													   WHEN (A.GL_FLAG = 71) THEN A.AMOUNT
													END AS DEBIT,
													CASE
													   WHEN (A.GL_FLAG = 72) THEN A.AMOUNT 
													END AS CREDIT,
													A.GL_ACC_ID AS ACCOUNT_ID,
													C.ACC_ID AS ACC_ID,
													 C.DESCRIPTION AS DESCRIPTION,
													 C.ACCOUNT_TYPE_ID AS ACCOUNT_TYPE_ID,
													A.GL_FLAG,
													A.FORM_DATE,
													A.FORM_REFERENCE,
													A.ID AS FORM_ID,
													A.IS_CONFLICTED,
                                                    A.Form_DETAIL_ID AS DETAIL_ID
											   FROM  (SELECT * FROM Adjustment_Accounting 
											   
									 WHERE Form_Flag = "GeneralJournal" and CASE
													  WHEN \'',P_COMPANY_ID,'\' <> "" THEN COMPANY_ID = \'',P_COMPANY_ID,'\'
															  ELSE TRUE
																   END
									         AND ((CASE
											WHEN \'',P_ENTRY_DATE_FROM,'\' <> "" THEN FORM_DATE >= \'',P_ENTRY_DATE_FROM,'\'
											ELSE TRUE
										  END
									  AND CASE
											WHEN \'',P_ENTRY_DATE_TO,'\' <> "" THEN FORM_DATE <= \'',P_ENTRY_DATE_TO,'\'
											ELSE TRUE
										  END)
										    OR ((CASE
											WHEN \'',P_ENTRY_DATE_TO,'\' <> "" THEN FORM_DATE <= \'',P_ENTRY_DATE_TO,'\'
											ELSE TRUE
                                            END)
											AND IS_Conflicted=''N'') OR (CASE
																			WHEN \'',P_ENTRY_DATE_FROM,'\' <> "" THEN RECONCILE_DATE > \'',P_ENTRY_DATE_FROM,'\'
																			ELSE TRUE
																		  END
																	  AND CASE
																			WHEN \'',P_ENTRY_DATE_TO,'\' <> "" THEN RECONCILE_DATE <= \'',P_ENTRY_DATE_TO,'\'
																			ELSE TRUE
																		  END) 
                                                                          AND IS_Conflicted=''Y'')
											  )A JOIN  (  SELECT ID, 
																								   ACC_ID, 
																								   DESCRIPTION, 
																								   ACCOUNT_TYPE_ID
																								 FROM ACCOUNTS_ID
																								WHERE CASE
																								 WHEN \'',P_COMPANY_ID,'\' <> "" THEN COMPANY_ID = \'',P_COMPANY_ID,'\'
																								   ELSE TRUE
																									  END ) C ON (A.GL_ACC_ID = C.ID)  
											 -- GENERAL_JOURNAL --
											 
											   UNION ALL
												
											 -- Repair Out --										 
											 SELECT CASE
													   WHEN (A.GL_FLAG = 74) THEN A.AMOUNT
													END AS DEBIT,
													CASE
													   WHEN (A.GL_FLAG = 73) THEN A.AMOUNT 
													END AS CREDIT,
													A.GL_ACC_ID AS ACCOUNT_ID,
													C.ACC_ID AS ACC_ID,
													 C.DESCRIPTION AS DESCRIPTION,
													 C.ACCOUNT_TYPE_ID AS ACCOUNT_TYPE_ID,
													A.GL_FLAG,
													A.FORM_DATE,
													A.FORM_REFERENCE,
													A.ID AS FORM_ID,
													A.IS_CONFLICTED,
                                                    A.Form_DETAIL_ID AS DETAIL_ID
											   FROM  (SELECT * FROM Repair_Accounting 
											 WHERE Form_Flag="RepairOut" and CASE
													  WHEN \'',P_COMPANY_ID,'\' <> "" THEN COMPANY_ID = \'',P_COMPANY_ID,'\'
															  ELSE TRUE
																   END
									         AND ((CASE
											WHEN \'',P_ENTRY_DATE_FROM,'\' <> "" THEN FORM_DATE >= \'',P_ENTRY_DATE_FROM,'\'
											ELSE TRUE
										  END
									  AND CASE
											WHEN \'',P_ENTRY_DATE_TO,'\' <> "" THEN FORM_DATE <= \'',P_ENTRY_DATE_TO,'\'
											ELSE TRUE
										  END)
										    OR ((CASE
											WHEN \'',P_ENTRY_DATE_TO,'\' <> "" THEN FORM_DATE <= \'',P_ENTRY_DATE_TO,'\'
											ELSE TRUE
                                            END)
											AND IS_Conflicted=''N'') OR (CASE
																			WHEN \'',P_ENTRY_DATE_FROM,'\' <> "" THEN RECONCILE_DATE > \'',P_ENTRY_DATE_FROM,'\'
																			ELSE TRUE
																		  END
																	  AND CASE
																			WHEN \'',P_ENTRY_DATE_TO,'\' <> "" THEN RECONCILE_DATE <= \'',P_ENTRY_DATE_TO,'\'
																			ELSE TRUE
																		  END) 
                                                                          AND IS_Conflicted=''Y'')
											   )A JOIN  (  SELECT ID, 
																								   ACC_ID, 
																								   DESCRIPTION, 
																								   ACCOUNT_TYPE_ID
																								 FROM ACCOUNTS_ID
																								WHERE CASE
																								 WHEN \'',P_COMPANY_ID,'\' <> "" THEN COMPANY_ID = \'',P_COMPANY_ID,'\'
																								   ELSE TRUE
																									  END ) C ON (A.GL_ACC_ID = C.ID)          
											 -- Repair Out --
												  UNION ALL
											  -- Repair IN --	
											  
											 SELECT CASE
													   WHEN (A.GL_FLAG = 75) THEN A.AMOUNT
													END AS DEBIT,
													CASE
													 WHEN (A.GL_FLAG = 76) THEN A.AMOUNT 
													   WHEN (A.GL_FLAG = 77) THEN A.AMOUNT 
													   WHEN (A.GL_FLAG = 78) THEN A.AMOUNT 
													END AS CREDIT,
													A.GL_ACC_ID AS ACCOUNT_ID,
													C.ACC_ID AS ACC_ID,
													 C.DESCRIPTION AS DESCRIPTION,
													 C.ACCOUNT_TYPE_ID AS ACCOUNT_TYPE_ID,
													A.GL_FLAG,
													A.FORM_DATE,
													A.FORM_REFERENCE,
													A.ID AS FORM_ID,
													A.IS_CONFLICTED,
                                                    A.Form_DETAIL_ID AS DETAIL_ID
											   FROM  (SELECT * FROM Repair_Accounting 
												 WHERE Form_Flag = "RepairIn" and CASE
													  WHEN \'',P_COMPANY_ID,'\' <> "" THEN COMPANY_ID = \'',P_COMPANY_ID,'\'
															  ELSE TRUE
																   END
									         AND ((CASE
											WHEN \'',P_ENTRY_DATE_FROM,'\' <> "" THEN FORM_DATE >= \'',P_ENTRY_DATE_FROM,'\'
											ELSE TRUE
										  END
									  AND CASE
											WHEN \'',P_ENTRY_DATE_TO,'\' <> "" THEN FORM_DATE <= \'',P_ENTRY_DATE_TO,'\'
											ELSE TRUE
										  END)
										    OR ((CASE
											WHEN \'',P_ENTRY_DATE_TO,'\' <> "" THEN FORM_DATE <= \'',P_ENTRY_DATE_TO,'\'
											ELSE TRUE
                                            END)
											AND IS_Conflicted=''N'') OR (CASE
																			WHEN \'',P_ENTRY_DATE_FROM,'\' <> "" THEN RECONCILE_DATE > \'',P_ENTRY_DATE_FROM,'\'
																			ELSE TRUE
																		  END
																	  AND CASE
																			WHEN \'',P_ENTRY_DATE_TO,'\' <> "" THEN RECONCILE_DATE <= \'',P_ENTRY_DATE_TO,'\'
																			ELSE TRUE
																		  END) 
                                                                          AND IS_Conflicted=''Y'')
											   )A JOIN  (  SELECT ID, 
																								   ACC_ID, 
																								   DESCRIPTION, 
																								   ACCOUNT_TYPE_ID
																								 FROM ACCOUNTS_ID
																								WHERE CASE
																								 WHEN \'',P_COMPANY_ID,'\' <> "" THEN COMPANY_ID = \'',P_COMPANY_ID,'\'
																								   ELSE TRUE
																									  END ) C ON (A.GL_ACC_ID = C.ID)        
											 -- Repair IN --
											   
										  ) ee JOIN ACCOUNT_TYPE FF ON (EE.ACCOUNT_TYPE_ID = FF.ID)
									WHERE CASE
											WHEN \'',P_ACCOUNT_ID,'\' <> "" THEN ee.ACCOUNT_ID = \'',P_ACCOUNT_ID,'\'
											ELSE TRUE
										  END
										  
								 ORDER BY ACC_ID, DESCRIPTION, FORM_DATE, GL_FLAG, FORM_REFERENCE, FORM_ID, DETAIL_ID ) E
							   
						GROUP BY ACC_ID, DESCRIPTION, FORM_DATE, GL_FLAG, FORM_REFERENCE, FORM_ID, DETAIL_ID, FORM_TYPE WITH ROLLUP
						 HAVING FORM_TYPE IS NOT NULL OR ACC_ID IS NULL) T
                       ;');
    PREPARE STMP FROM @QRY;
    EXECUTE STMP ;
    DEALLOCATE PREPARE STMP;
END $$
DELIMITER ;
