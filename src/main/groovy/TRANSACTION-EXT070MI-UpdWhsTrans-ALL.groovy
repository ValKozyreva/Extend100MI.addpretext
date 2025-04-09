/**
 * API used to update warehouse transaction with serial number and CoC
 *
 * The method in this extension class (UpdWhsTrans) updates transaction history
 * with serial number and CoC (Country of Origin).
 *
 *  Date          Changed By            Version      Description
 *  2025-02-06    Frank Herman Wik      1.0          Initial Release
 *
 */
public class UpdWhsTrans extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final ProgramAPI program
  private final LoggerAPI logger

  String warehouse, location, orderNumber, orderLine, deliveryNumber, itemNumber, serialNumber, countryOfOrigin, messageType
  int company

  public UpdWhsTrans(MIAPI mi, DatabaseAPI databaseAPI, ProgramAPI program, LoggerAPI logger) {
    this.mi = mi
    this.database = databaseAPI
    this.program = program
    this.logger = logger
  }

  public void main() {
    // Get the company number
    company = program.LDAZD.CONO as int

    // Get input data from MI
    warehouse = mi.inData.get("WHLO") == null ? "" : mi.inData.get("WHLO").trim()
    location = mi.inData.get("WHSL") == null ? "" : mi.inData.get("WHSL").trim()
    orderNumber = mi.inData.get("RIDN") == null ? "" : mi.inData.get("RIDN").trim()
    orderLine = mi.inData.get("RIDL") == null ? "" : mi.inData.get("RIDL").trim()
    deliveryNumber = mi.inData.get("DLIX") == null ? "" : mi.inData.get("DLIX").trim()
    itemNumber = mi.inData.get("ITNO") == null ? "" : mi.inData.get("ITNO").trim()
    serialNumber = mi.inData.get("USD1") == null ? "" : mi.inData.get("USD1").trim()
    countryOfOrigin = mi.inData.get("USD2") == null ? "" : mi.inData.get("USD2").trim()

    logger.debug("Input Data: CONO= ${company}, WHLO=${warehouse}, WHSL=${location}, RIDN=${orderNumber}, RIDL=${orderLine}, DLIX=${deliveryNumber}, ITNO=${itemNumber}, USD1=${serialNumber}, USD2=${countryOfOrigin}")

    // Check if all required fields are filled
    if (warehouse.isEmpty() || location.isEmpty() || orderNumber.isEmpty() || orderLine.isEmpty()
      || deliveryNumber.isEmpty() || itemNumber.isEmpty() || serialNumber.isEmpty()) {
      mi.error("All required fields must be filled")
      return
    }

    // Update stock transaction with serial number and CoC
    updateStockTransaction()
  }

  /**
   * Updates the warehouse transaction with serial number and Country of Origin (CoC).
   *
   * This method constructs an expression to match records in the MITTRA table based on
   * delivery number, order number, order line, and location. It then updates the matched
   * records with the serial number and CoC.
   */
  private void updateStockTransaction() {
    // Check if the serial number already exists in the MITTRA table
    ExpressionFactory checkExpression = database.getExpressionFactory("MITTRA")
    checkExpression = checkExpression.eq("MTBREF", serialNumber)
    checkExpression = checkExpression.and(checkExpression.eq("MTRIDN", orderNumber))
    checkExpression = checkExpression.and(checkExpression.eq("MTRIDL", orderLine))

    DBAction checkQuery = database.table("MITTRA").index("00").matching(checkExpression).build()
    DBContainer checkContainer = checkQuery.getContainer()
    checkContainer.setInt("MTCONO", company)
    checkContainer.setString("MTWHLO", warehouse)
    checkContainer.setString("MTITNO", itemNumber)

    boolean recordExists = false

    Closure<?> readCallback = { DBContainer readResult ->
      recordExists = true
    }

    // Read the record to check if it exists
    checkQuery.readAll(checkContainer, 3, readCallback)

    if (recordExists) {
      mi.error("Serial number ${serialNumber} already exists in MITTRA for order number ${orderNumber} and order line ${orderLine}")
      return
    }

    // Update the warehouse transaction with serial number and CoC
    ExpressionFactory expression = database.getExpressionFactory("MITTRA")
    expression = expression.eq("MTRIDI", deliveryNumber)
    expression = expression.and(expression.eq("MTRIDN", orderNumber))
    expression = expression.and(expression.eq("MTRIDL", orderLine))
    expression = expression.and(expression.eq("MTWHSL", location))

    DBAction query = database.table("MITTRA").index("00").matching(expression).build()
    DBContainer container = query.getContainer()

    container.setInt("MTCONO", company)
    container.setString("MTWHLO", warehouse)
    container.setString("MTITNO", itemNumber)

    boolean recordUpdated = false

    Closure<?> updateCallBack = { LockedResult lockedResult ->
      if (lockedResult.get("MTBREF").toString().trim().isEmpty() && !recordUpdated) {
        logger.debug("Updating warehouse transaction with serial number: ${serialNumber} and CoC: ${countryOfOrigin}")
        recordUpdated = true
        lockedResult.set("MTBREF", serialNumber)
        if (countryOfOrigin != "") {
          lockedResult.set("MTBRE2", countryOfOrigin)
        }
        lockedResult.update()
      }
    }

    if (!query.readAllLock(container, 3, updateCallBack)) {
      mi.error("Failed to lock and update the record in MITTRA")
    }
  }
}
