import { randomUUID } from "crypto"
import {
  createAndFillUpTable,
  deleteEntities,
  shouldFindEverySingleRecord,
} from "./tests/createAndFillUpTable"
import { createAndDropTable } from "./tests/createAndDropTable"

// Your Azure Storage Account details
const accountName = process.env.AZURE_ACCOUNT_NAME ?? ""
const accountKey = process.env.AZURE_ACCOUNT_KEY ?? ""

// Constants
const TABLE_NAME = "CustomersTable"
const TABLE_TO_BE_DELETED = `DeleteTable${randomUUID().replace(/-/g, "")}`
const MAX_RETRIES = 3
const RETRY_DELAY_IN_MS = 1000

console.log("Settings: ", {
  accountName,
  accountKey,
  CrudTable: TABLE_NAME,
  DeleteTable: TABLE_TO_BE_DELETED,
})

// Create a TableClient
async function main() {
  const insertTable = `InsertTable${randomUUID().replace(/-/g, "")}`

  // 1. Creating table, appending records and finding them
  await createAndFillUpTable(
    accountName,
    accountKey,
    insertTable,
    MAX_RETRIES,
    RETRY_DELAY_IN_MS,
  )
  await shouldFindEverySingleRecord(
    accountName,
    accountKey,
    insertTable,
    MAX_RETRIES,
    RETRY_DELAY_IN_MS,
  )
  await deleteEntities(
    accountName,
    accountKey,
    insertTable,
    MAX_RETRIES,
    RETRY_DELAY_IN_MS,
  )

  // 2. Creating and Dropping table
  await createAndDropTable(
    accountName,
    accountKey,
    MAX_RETRIES,
    RETRY_DELAY_IN_MS,
  )

  console.log("Done")
}

// Run the main function
main().catch((err) => console.error("Error:", err))
