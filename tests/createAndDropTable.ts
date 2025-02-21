// Entity to be inserted
import {
  createTable,
  dropTable,
  getTables,
  getTableServiceClient,
  tableExists,
} from "../libs"

import { randomUUID } from "crypto"

export const createAndDropTable = async (
  accountName: string,
  accountKey: string,
  maxRetries: number,
  retryDelayInMillis: number,
): Promise<void> => {
  const tableName = `TmpTable${randomUUID().replace(/-/g, "")}`

  console.log(`Creating and Dropping table ${tableName}`)

  const tableSvcClient = getTableServiceClient(accountName, accountKey)

  if (
    !(await tableExists(
      tableSvcClient,
      tableName,
      maxRetries,
      retryDelayInMillis,
    ))
  ) {
    console.info(`Table not found ${tableName}`)
    await createTable(tableSvcClient, tableName, maxRetries, retryDelayInMillis)
    console.info(`Table ${tableName} created`)
  } else {
    console.info(`Table already exists ${tableName}`)
  }

  await dropTable(tableSvcClient, tableName, maxRetries, retryDelayInMillis)
  console.info(`Table ${tableName} dropped`)

  const isDropped = await tableExists(
    tableSvcClient,
    tableName,
    maxRetries,
    retryDelayInMillis,
  )

  console.info(`Table ${tableName} exists: ${isDropped}`)

  console.info("TABLES:")
  const tables = await getTables(tableSvcClient, maxRetries, retryDelayInMillis)
  tables.forEach((table: string) => console.log(`  ${table}`))
}
