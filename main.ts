import { randomUUID } from "crypto"
import {
  createAndFillUpTable,
  deleteEntities,
  shouldFindEverySingleRecord,
} from "./tests/createAndFillUpTable"
import { createAndDropTable } from "./tests/createAndDropTable"
import { IBaseEntity } from "./interfaces/entity.interface"
import { createLock } from "./tests/lockTable"
import { sleep } from "./libs"

// Your Azure Storage Account details
const accountName = process.env.AZURE_ACCOUNT_NAME ?? ""
const accountKey = process.env.AZURE_ACCOUNT_KEY ?? ""

// Constants
const LOCK_TABLE = "LockTable"
const MAX_RETRIES = 3
const RETRY_DELAY_IN_MS = 1000

console.log("Settings: ", {
  accountName,
  accountKey,
})

// Create a TableClient
async function main() {
  const insertTable = `InsertTable${randomUUID().replace(/-/g, "")}`

  // 1. Creating table, appending records and finding them
  // await createAndFillUpTable(
  //   accountName,
  //   accountKey,
  //   insertTable,
  //   MAX_RETRIES,
  //   RETRY_DELAY_IN_MS,
  // )
  // await shouldFindEverySingleRecord(
  //   accountName,
  //   accountKey,
  //   insertTable,
  //   MAX_RETRIES,
  //   RETRY_DELAY_IN_MS,
  // )
  // await deleteEntities(
  //   accountName,
  //   accountKey,
  //   insertTable,
  //   MAX_RETRIES,
  //   RETRY_DELAY_IN_MS,
  // )

  // 2. Creating and Dropping table
  // await createAndDropTable(
  //   accountName,
  //   accountKey,
  //   MAX_RETRIES,
  //   RETRY_DELAY_IN_MS,
  // )

  // 3. Adding locks
  const resource: IBaseEntity = { partitionKey: "KEY", rowKey: "1234" }
  await Promise.all([
    createLock(
      accountName,
      accountKey,
      LOCK_TABLE,
      resource,
      "ProcessA",
      10,
      RETRY_DELAY_IN_MS,
    ),

    createLock(
      accountName,
      accountKey,
      LOCK_TABLE,
      resource,
      "ProcessB",
      10,
      RETRY_DELAY_IN_MS,
    ),

    createLock(
      accountName,
      accountKey,
      LOCK_TABLE,
      resource,
      "ProcessC",
      10,
      RETRY_DELAY_IN_MS,
    ),
  ])

  console.log("Done")
}

// Run the main function
main().catch((err) => console.error("Error:", err))
