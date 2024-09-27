/**
 * API used to update customer order header with pretext from OIS035
 *
 * The method in this extension class (AddPreText) updates customer order header
 * with pretext from OIS035.
 *
 *  Date          Changed By            Version      Description
 *  2024-05-29    Frank Herman Wik      1.0          Initial Release
 *  2024-09-19    Frank Herman Wik      1.0          Updated according to review comments
 *  2024-09-26    Frank Herman Wik      1.0          Correction
 *  2024-09-27    Frank Herman Wik      1.0          Added Javadoc to method deleteTxtBlockLines
 *
 */
import java.time.*
import java.time.format.*

public class AddPreText extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final UtilityAPI utility
  private final ProgramAPI program
  private final LoggerAPI logger
  private final MICallerAPI miCaller

  private String inOrderNumber, customer, businessChainLevel1, customerOrderLanguage, newTextID, permanentTextIdentity, permanentTextLanguage, pretext
  boolean textHeadCreated, textLineCreated, textLineDeleted
  int company, nrOfRecords, orderStatus

  public AddPreText(MIAPI mi, DatabaseAPI databaseAPI, ProgramAPI program, UtilityAPI util, LoggerAPI logger, MICallerAPI miCaller) {
    this.mi = mi
    this.database = databaseAPI
    this.utility = util
    this.program = program
    this.logger = logger
    this.miCaller = miCaller
  }

  public void main() {
    // The company number retrieved from the program's LDAZD.CONO
    company = program.LDAZD.CONO as int

    // The number of records, limited to a maximum of 10000
    nrOfRecords = mi.getMaxRecords() <= 0 || mi.getMaxRecords() >= 10000? 10000: mi.getMaxRecords()

    // Retrieve the order number from the input
    inOrderNumber = mi.in.get("ORNO") == null ? "" : mi.in.get("ORNO")
    logger.debug("Input CONO/ORNO: " + company + "/" + inOrderNumber)

    // If the order number is not provided, an error is returned and the execution is stopped
    if (inOrderNumber == "") {
      mi.error("Order number must be entered")
      return
    }

    // Check if the customer order exists
    if (!getOOHEAD(company, inOrderNumber)) {
      if (orderStatus == 90) {
        mi.error("Order is deleted, add pretext is not permitted")
      } else {
        mi.error("Customer order number ${inOrderNumber} does not exists")
      }
      return
    }

    // Check if CO delivery status is valid
    if (!deliveryLines(company, inOrderNumber)) {
      mi.error("WARNING - Changes will not affect existing deliveries")
      return
    }

    // Retrieve the permanent text for the given company, customer, and customer order language
    retrievePermanentText(company, customer, customerOrderLanguage)

    // If the permanent text identity is valid and the customer order language matches the permanent text language
    if (Integer.parseInt(permanentTextIdentity) > 0 && customerOrderLanguage == permanentTextLanguage) {
      // Check if the pretext already exists
      if (pretextExists(company, inOrderNumber)) {
        logger.debug("pretextExists --> true")
        // Delete pretext
        deleteTxtBlockLines(company.toString(), "", pretext, "CO02", "", "OSYTXH", "")
      }
      // Get a new text identity
      newTextID = getNewTextID("OSYTXH")
      logger.debug("newTextID = " + newTextID)

      // Add a new text block header and lines to it
      addTextBlock(company, inOrderNumber, permanentTextIdentity, newTextID)

      // If the text block header and lines were successfully created, update the customer order head with the new text identity
      if (textHeadCreated && textLineCreated) {
        updatePretext(company, inOrderNumber, newTextID)
      }
    }
  }

  /**
   * Checks if the pretext already exists for the given order number.
   *
   * @param company The company number.
   * @param orderNumber The order number.
   * @return {@code true} if the pretext exists, {@code false} otherwise.
   */
  private boolean pretextExists(int company, String orderNumber) {
    DBAction query = database.table("OOHEAD").index("00").selection("OAPRTX").build()
    DBContainer container = query.getContainer()

    container.set("OACONO", company)
    container.set("OAORNO", orderNumber)

    if (query.read(container)) {
      pretext = container.get("OAPRTX").toString()
      logger.debug("pretext = " + pretext)
      return pretext != null && !pretext.trim().isEmpty()
    }
    return false
  }

  /**
   * Updates the existing pretext for the given order number.
   *
   * @param company The company number.
   * @param orderNumber The order number.
   * @param textIdentity The identity of the text to be updated.
   */
  private void updatePretext(int company, String orderNumber, String textIdentity) {
    DBAction query = database.table("OOHEAD").index("00").selection("OAPRTX").build()
    DBContainer container = query.getContainer()

    container.set("OACONO", company)
    container.set("OAORNO", orderNumber)

    Closure<?> updateCallBack = { LockedResult lockedResult ->
      lockedResult.set("OAPRTX", Long.parseLong(textIdentity))
      lockedResult.set("OALMDT", LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInteger())
      lockedResult.set("OACHNO", ((int) lockedResult.get("OACHNO")) + 1)
      lockedResult.set("OACHID", program.getUser())
      lockedResult.update()
    }

    if (!query.readLock(container, updateCallBack)) {
      mi.error("Could not update OOHEAD with new values")
      return
    }
    logger.debug("OOHEAD record updated successfully. New pretext value = " + textIdentity)
  }

  /**
   * Adds a new text block header and lines to it.
   *
   * @param company The company number.
   * @param inOrderNumber The order number.
   * @param permanentTextIdentity The identity of the permanent text.
   * @param newTextID The new Text ID.
   */
  private void addTextBlock(int company, String inOrderNumber, String permanentTextIdentity, String newTextID) {
    // Add new header
    addTxtBlockHead("OOHEAD00", newTextID, company.toString(), "OSYTXH", inOrderNumber, "", program.getUser(), "CO02", "", "", "2")

    if (textHeadCreated) {
      logger.debug("Text header created")
      DBAction dbaOSYTXL = database.table("OSYTXL").index("00").selection("TLTX60").build()
      DBContainer conOSYTXL = dbaOSYTXL.getContainer()

      conOSYTXL.set("TLCONO", company)
      conOSYTXL.set("TLDIVI", "")
      conOSYTXL.set("TLTXID", Long.parseLong(permanentTextIdentity))
      conOSYTXL.set("TLTXVR", "")
      conOSYTXL.set("TLLNCD", "")

      Closure<?> readOSYTXL  = { DBContainer getOSYTXL ->
        // Add text block line
        addTxtBlockLine("OOHEAD00", newTextID, "OSYTXH", getOSYTXL.getString("TLTX60"), company.toString(), "", "", "CO02")
      }

      dbaOSYTXL.readAll(conOSYTXL, 5, nrOfRecords, readOSYTXL)
    }
  }

  /**
   * This method retrieves the permanent text for a given company, customer, and language.
   * It uses the ODTXTH table to find the relevant records and updates the permanentTextIdentity and permanentTextLanguage variables.
   *
   * @param company The company number.
   * @param customer The customer number.
   * @param language The language code.
   */
  private void retrievePermanentText(int company, String customer, String language) {
    int today = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInteger()
    permanentTextIdentity = "0"
    permanentTextLanguage = ""

    DBAction dbaODTXTH = database.table("ODTXTH").index("00").selection("UVTXID", "UVTXDO", "UVTXPR", "UVENDT", "UVLNCD").build()
    DBContainer conODTXTH = dbaODTXTH.getContainer()

    conODTXTH.set("UVCONO", company)
    conODTXTH.set("UVTXCD", 3)
    conODTXTH.set("UVTXKY", customer)
    conODTXTH.set("UVLNCD", language)

    Closure<?> readODTXTH  = { DBContainer getODTXTH ->
      logger.debug("ODTXTH record found, input customer: " + customer)
      logger.debug("ODTXTH UVTXDO/UVTXPR/UVENDT: " + getODTXTH.getInt("UVTXDO") + "/" + getODTXTH.getInt("UVTXPR") + "/" + getODTXTH.getInt("UVENDT"))
      if (getODTXTH.getInt("UVTXDO") == 3 && getODTXTH.getInt("UVTXPR") == 1 && today <= getODTXTH.getInt("UVENDT")) {
        permanentTextIdentity = getODTXTH.get("UVTXID").toString().trim()
        permanentTextLanguage = getODTXTH.getString("UVLNCD").trim()
        logger.debug("permanentTextIdentity/permanentTextLanguage = " + permanentTextIdentity + "/" + permanentTextLanguage)
      }
    }

    // Execute the query and process the results
    if (!dbaODTXTH.readAll(conODTXTH, 4, nrOfRecords, readODTXTH)) {
      // If record not found, try with business chain level 1
      logger.debug("ODTXTH record not found, try with business chain level 1 - " + businessChainLevel1)
      conODTXTH.set("UVTXKY", businessChainLevel1)
      dbaODTXTH.readAll(conODTXTH, 4, nrOfRecords, readODTXTH)
    }

    logger.debug("permanentTextIdentity / permanentTextLanguage / customerOrderLanguage = " + permanentTextIdentity + " / " + permanentTextLanguage + " / " + customerOrderLanguage)
  }

  /**
   * Updates the OOHEAD record with new values.
   *
   * @param company The company number.
   * @param orderNumber The order number.
   */
  private void updateOOHEAD(int company, String orderNumber) {
    DBAction query = database.table("OOHEAD").index("00").selection("OAPRTX", "OACONO").build()
    DBContainer container = query.getContainer()

    container.set("OACONO", company)
    container.set("OAORNO", orderNumber)

    Closure<?> updateCallBack = { LockedResult lockedResult ->
      lockedResult.set("OAPRTX", Long.parseLong(newTextID))
      lockedResult.set("OALMDT", LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInteger())
      lockedResult.set("OACHNO", ((int)lockedResult.get("OACHNO"))+1)
      lockedResult.set("OACHID", program.getUser())
      lockedResult.update()
    }

    if (!query.readLock(container, updateCallBack)) {
      mi.error("Could not update OOHEAD with new values")
      return
    }
    logger.debug("OOHEAD record updated successfully")
  }

  /**
   * Retrieves information about a specific customer order from the database.
   *
   * @param cono The company number.
   * @param orno The order number.
   * @return {@code true} if the order exists in the database, {@code false} otherwise.
   */
  private boolean getOOHEAD(int cono, String orno) {
    DBAction query = database.table("OOHEAD").index("00").selection("OACUNO", "OALNCD", "OACHL1", "OAORST").build()
    DBContainer container = query.getContainer()

    container.set("OACONO", cono)
    container.set("OAORNO", orno)

    if (query.read(container)) {
      orderStatus = Integer.parseInt(container.get("OAORST").toString())
      if (orderStatus == 90) {
        return false
      }
      customer = container.getString("OACUNO").trim()
      customerOrderLanguage = container.getString("OALNCD").trim()
      businessChainLevel1 = container.getString("OACHL1").trim()
      return true
    }
    return false
  }

  /**
   * Checks if the delivery lines for a given company and order number meet specific criteria.
   *
   * @param cono The company number.
   * @param orno The order number.
   * @return {@code true} if the order status is less than 68, {@code false} otherwise.
   */
  public boolean deliveryLines(int cono, String orno) {
    boolean result = false
    DBAction query = database.table("ODHEAD").index("35").selection("UAORST").build()
    DBContainer container = query.getContainer()

    container.set("UACONO", cono)
    container.set("UAORNO", orno)

    Closure<?> readODHEAD  = { DBContainer getODHEAD ->
      if (Integer.parseInt(getODHEAD.get("UAORST").toString()) < 68) {
        result = true
      }
    }

    if (!query.readAll(container, 2, nrOfRecords, readODHEAD)) {
      return true
    }

    return result
  }

  /**
   * Retrieves a new Text ID from the specified file using CRS980MI RtvNewTextID method.
   *
   * @param file The name of the file for which a new Text ID is to be retrieved.
   * @return The new Text ID obtained from CRS980MI RtvNewTextID, or empty string if an error occurs.
   */
  private String getNewTextID(String file) {
    final HashMap<String, String> inputRtvNewTextID = new HashMap<>()
    inputRtvNewTextID.put("FILE", file)
    String out = ""
    miCaller.call("CRS980MI", "RtvNewTextID", inputRtvNewTextID, { final output ->
      if(output["errorMessage"] != null && output["errorMessage"].toString() != "" ){
        logger.debug("CRS980MI - RtvNewTextID, errror message: " + output["errorMessage"].toString())
        out = ""
      } else {
        out = output["TXID"].toString().trim()
      }
    })
    return out
  }

  /**
   * Adds a text block header using the CRS980MI AddTxtBlockHead method.
   *
   * @param file          The file associated with the text block.
   * @param textIdentity  The identity of the text block.
   * @param company       The company code.
   * @param transferFile  The transfer file.
   * @param fileKey       The file key.
   * @param language      The language code.
   * @param userID        The user ID.
   * @param textBlock     The text block version.
   * @param division      The division code.
   * @param description   The description of the text block.
   * @param extIntText    The external/internal text indicator.
   */
  private String addTxtBlockHead(String file, String textIdentity, String company, String transferFile, String fileKey, String language, String userID,
                                 String textBlock, String division, String description, String extIntText) {
    final HashMap<String, String> inputAddTxtBlockHead = new HashMap<>()
    inputAddTxtBlockHead.put("FILE", file)
    inputAddTxtBlockHead.put("TXID", textIdentity)
    inputAddTxtBlockHead.put("CONO", company)
    inputAddTxtBlockHead.put("TFIL", transferFile)
    inputAddTxtBlockHead.put("KFLD", fileKey)
    inputAddTxtBlockHead.put("LNCD", language)
    inputAddTxtBlockHead.put("USID", userID)
    inputAddTxtBlockHead.put("TXVR", textBlock)
    inputAddTxtBlockHead.put("DIVI", division)
    inputAddTxtBlockHead.put("TX40", description)
    inputAddTxtBlockHead.put("TXEI", extIntText)

    logger.debug("run CRS980MI.AddTxtBlockHead")

    miCaller.call("CRS980MI", "AddTxtBlockHead", inputAddTxtBlockHead, { final output ->
      if(output["errorMessage"] != null && output["errorMessage"].toString() != "" ){
        logger.debug("CRS980MI - AddTxtBlockHead, errror message: " + output["errorMessage"].toString())
        textHeadCreated = false
        return
      }
    })
    textHeadCreated = true
  }

  /**
   * Adds a text block line using the CRS980MI AddTxtBlockLine method.
   *
   * @param file          The file associated with the text block.
   * @param textIdentity  The identity of the text block.
   * @param transferFile  The transfer file.
   * @param text          The text to be added to the text block.
   * @param company       The company code.
   * @param language      The language code.
   * @param division      The division code.
   * @param textBlock     The text block version.
   */
  private String addTxtBlockLine(String file, String textIdentity, String transferFile, String text, String company, String language, String division, String textBlock) {
    if (text.length() > 60) {
      logger.debug("addTxtBlockLine - text length = " + text.length())
      text = text.toString().substring(0, 60)
      logger.debug("addTxtBlockLine - text, substring 0-60 = " + text)
    }

    final HashMap<String, String> inputAddTxtBlockLine = new HashMap<>()
    inputAddTxtBlockLine.put("FILE", file)
    inputAddTxtBlockLine.put("TXID", textIdentity)
    inputAddTxtBlockLine.put("TFIL", transferFile)
    inputAddTxtBlockLine.put("TX60", text)
    inputAddTxtBlockLine.put("CONO", company)
    inputAddTxtBlockLine.put("LNCD", language)
    inputAddTxtBlockLine.put("DIVI", division)
    inputAddTxtBlockLine.put("TXVR", textBlock)

    logger.debug("run CRS980MI.AddTxtBlockLine")

    miCaller.call("CRS980MI", "AddTxtBlockLine", inputAddTxtBlockLine, { final output ->
      if(output["errorMessage"] != null && output["errorMessage"].toString() != "" ){
        logger.debug("CRS980MI - AddTxtBlockLine, errror message: " + output["errorMessage"].toString())
        textLineCreated = false
        return
      }
    })
    textLineCreated = true
  }

  /**
   * Deletes text block lines using the CRS980MI DltTxtBlockLins method.
   *
   * @param company       The company code.
   * @param division      The division code.
   * @param textIdentity  The identity of the text block.
   * @param textBlock     The text block version.
   * @param language      The language code.
   * @param transferFile  The transfer file.
   * @param file          The file associated with the text block.
   */
  private String deleteTxtBlockLines(String company, String division, String textIdentity, String textBlock, String language, String transferFile, String file) {
    final HashMap<String, String> inputDltTxtBlockLins = new HashMap<>()
    inputDltTxtBlockLins.put("CONO", company)
    inputDltTxtBlockLins.put("DIVI", division)
    inputDltTxtBlockLins.put("TXID", textIdentity)
    inputDltTxtBlockLins.put("TXVR", textBlock)
    inputDltTxtBlockLins.put("LNCD", language)
    inputDltTxtBlockLins.put("TFIL", transferFile)
    inputDltTxtBlockLins.put("FILE", file)

    logger.debug("run CRS980MI.DltTxtBlockLins")

    miCaller.call("CRS980MI", "DltTxtBlockLins", inputDltTxtBlockLins, { final output ->
      if(output["errorMessage"] != null && output["errorMessage"].toString() != "" ){
        logger.debug("CRS980MI - DltTxtBlockLins, errror message: " + output["errorMessage"].toString())
        textLineCreated = false
        return
      }
    })
    textLineDeleted = true
  }
}
