
  CREATE OR REPLACE VIEW "IDEAS"."PR_LINE_ITEM_SUMMARY" ("PR_LINE_ITEM_DETAIL_ID", "PURCHASE_REQUEST_ID", "FAMILY_ID", "PARENT_ID", "LINE_ITEM", "DESCRIPTION", "TYPE", "PRODUCT_OR_SERVICE", "UNIT_OF_MEASURE", "QUANTITY", "UNIT_PRICE", "PRICING_ARRANGEMENT", "AMOUNT", "COST_CONSTRAINT", "COMMITTED_AMOUNT", "COMMITMENT_ID_NUMBER", "OPTION_LINE_ITEM", "SEVERABLE_INDICATOR", "ITEM_UUID_REQUIRED", "POP_START", "POP_END", "FOB_POINT_TYPE", "PAYMENT_METHOD", "UNITED_NATIONS_CODE", "REFERENCE_DESCRIPTION", "REFERENCE_VALUE", "OPTION_EXERCISE_DATE") AS 
  SELECT lineItemDetail."id"           AS PR_LINE_ITEM_DETAIL_ID,
    lineItemDetail."PurchaseRequestId" AS PURCHASE_REQUEST_ID,
    CASE
      WHEN lineItemDetail."ParentId" IS NULL
      THEN lineItemDetail."id"
      ELSE lineItemDetail."ParentId"
    END                       AS FAMILY_ID,
    lineItemDetail."ParentId" AS PARENT_ID,
    itemsType."lineItemBase"
    || extensionType."infoSlinExtension"
    || extensionType."slinExtension"            AS LINE_ITEM,
    psQuantityType."productServiceDescription"  AS DESCRIPTION,
    itemsType."lineItemType"                    AS TYPE,
    psQuantityType."productOrService"           AS PRODUCT_OR_SERVICE,
    psQuantityType."unitOfMeasure"              AS UNIT_OF_MEASURE,
    psQuantityType."quantity"                   AS QUANTITY,
    psQuantityType."unitPrice"                  AS UNIT_PRICE,
    pricingArrangement."pricingArrangementBase" AS PRICING_ARRANGEMENT,
    CASE
      WHEN pricingArrangement."pricingArrangementBase" IS NOT NULL
      THEN
        CASE
          WHEN pricingArrangement."pricingArrangementBase" IN ('Fixed Price Re-determination Prospective', 'Fixed Price with Economic Price Adjustment - Actual Costs', 'Fixed Price with Economic Price Adjustment - Cost Indexes', 'Fixed Price with Economic Price Adjustment - Established Prices')
          THEN amountBasePrice."amount"
          WHEN pricingArrangement."pricingArrangementBase" IN ('Fixed Price Incentive (Successive Targets)', 'Fixed Price Incentive (Cost Based)', 'Fixed Price Re-determination Retrospective')
          THEN amountCeilingPrice."amount"
          WHEN pricingArrangement."pricingArrangementBase" IN ('Cost No Fee', 'Cost Sharing', 'Cost Plus Award Fee', 'Cost Plus Fixed Fee')
          THEN amountEstimatedCost."amount"
          WHEN pricingArrangement."pricingArrangementBase" IN ('Firm Fixed Price', 'Fixed Price Level of Effort', 'Labor Hour', 'Time and Materials', 'Cost Plus Incentive Fee (Cost Based)')
          THEN
            CASE
              WHEN psQuantityType."unitPrice" IS NULL
              OR psQuantityType."quantity"    IS NULL
              THEN NULL
              ELSE psQuantityType."unitPrice" * psQuantityType."quantity"
            END
          ELSE NULL
        END
      ELSE NULL
    END                                         AS AMOUNT,
    priceBasis."priceBasis"                     AS COST_CONSTRAINT,
    commitmentAmount."committedAmount"          AS COMMITTED_AMOUNT,
    accountingRef."cmmitmentIdentificatinNmber" AS COMMITMENT_ID_NUMBER,
    basicInformationType."optionLineItem"       AS OPTION_LINE_ITEM,
    basicInformationType."severableIndicator"   AS SEVERABLE_INDICATOR,
    psQuantityType."itemUIDRequired"            AS ITEM_UUID_REQUIRED,
    popStart."dateElement"                      AS POP_START,
    popEnd."dateElement"                        AS POP_END,
    fob."foBPoint"                              AS FOB_POINT_TYPE,
    fob."paymentMethod"                         AS PAYMENT_METHOD,
    fob."unitedNationsCode"                     AS UNITED_NATIONS_CODE,
    referenceDescription."referenceDescription" AS REFERENCE_DESCRIPTION,  
    referenceDescription."referenceValue"       AS REFERENCE_VALUE, 
    optionExercise."dateElement"                AS OPTION_EXERCISE_DATE
  FROM "PR_PRLineItemDetailType" lineItemDetail
  LEFT JOIN "PR_LineItemIdentifierType" identifier
  ON identifier."id" = lineItemDetail.lineItemIdentifier
  LEFT JOIN "PR_DFARSLineItemType" DFARS
  ON DFARS."id" = identifier.DFARS
  LEFT JOIN "PR_LineItemsType" itemsType
  ON itemsType."id" = DFARS.LINEITEM
  LEFT JOIN "PR_LineItemExtensionType" extensionType
  ON extensionType."id" = itemsType.LINEITEMEXTENSION
  LEFT JOIN "PR_LinItmBsicInformtionType" basicInformationType
  ON basicInformationType."id" = lineItemDetail.LINEITEMBASICINFORMATION
  LEFT JOIN "PR_PricingArrangementType" pricingArrangement
  ON pricingArrangement."id" = basicInformationType.RECOMMENDEDPRICINGARRANGEMENT
  LEFT JOIN "PR_PrdctServiceQuantityType" psQuantityType
  ON psQuantityType."id" = basicInformationType.PRODUCTSERVICESORDERED
  LEFT JOIN "PR_PriceBasisType" priceBasis
  ON priceBasis."PrdctSrvcQnttyTyp_prcBss_id" = psQuantityType."id"
  LEFT JOIN "PR_LineItemAmountsType" itemAmounts
  ON itemAmounts."id" = lineItemDetail.LINEITEMAMOUNTS
  LEFT JOIN "PR_ItemCommitmentAmountType" commitmentAmount
  ON commitmentAmount."id" = itemAmounts.ITEMCOMMITTEDAMOUNT
  LEFT JOIN "PR_ItemOtherAmountsType" amountBasePrice
  ON amountBasePrice."LinItmmntsTyp_itmthrmnts_id" = itemAmounts."id"
  AND amountBasePrice."amountDescription"          = 'Base Price'
  LEFT JOIN "PR_ItemOtherAmountsType" amountCeilingPrice
  ON amountCeilingPrice."LinItmmntsTyp_itmthrmnts_id" = itemAmounts."id"
  AND amountCeilingPrice."amountDescription"          = 'Ceiling Price'
  LEFT JOIN "PR_ItemOtherAmountsType" amountEstimatedCost
  ON amountEstimatedCost."LinItmmntsTyp_itmthrmnts_id" = itemAmounts."id"
  AND amountEstimatedCost."amountDescription"          = 'Estimated Cost'
  LEFT JOIN "PR_ItemAcntingRfrnceNmbrTyp" accountingRef
  ON accountingRef."id" = commitmentAmount.ACCOUNTINGREFERENCENUMBER
  LEFT JOIN "PR_LineItemDatesType" lineItemPopDate
  ON lineItemPopDate."PRLnItmDtilTyp_linItmDts_id" = lineItemDetail."id"
  AND lineItemPopDate."lineItemDateDescription"    = 'Period of Performance'
  LEFT JOIN "PR_DatePeriodType" popDate
  ON popDate."id" = lineItemPopDate.LINEITEMPERIOD
  LEFT JOIN "PR_DateTimeType" popStart
  ON popStart."id" = popDate.PERIODSTART
  LEFT JOIN "PR_DateTimeType" popEnd
  ON popEnd."id" = popDate.PERIODEND
  LEFT JOIN "PR_LineItemDatesType" lineItemOptExDate
  ON lineItemOptExDate."PRLnItmDtilTyp_linItmDts_id" = lineItemDetail."id"
  AND lineItemOptExDate."lineItemDateDescription"    = 'Option Exercise Date'
  LEFT JOIN "PR_DateTimeType" optionExercise
  ON optionExercise."id" = lineItemOptExDate.LINEITEMDATE
  LEFT JOIN "PR_ShippingType" ship
  ON ship."id" = lineItemDetail.SHIPPING
  LEFT JOIN "PR_FoBDetailsType" fob
  ON fob."id" = ship.FOBDETAILS
LEFT JOIN "PR_DocumentReferenceNumType" referenceDescription
ON referenceDescription."id" = lineItemDetail.lineItemIdentifier;
