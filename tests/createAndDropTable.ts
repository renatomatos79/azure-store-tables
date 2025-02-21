// Entity to be inserted
import {
  createEntity,
  createTable,
  dropTable,
  getEntity,
  getTableClient,
  getTableServiceClient,
  isObjectSubset,
  tableExists,
} from "../lib"

import { IUserEntity } from "../interfaces/entity.interface"
import { USERS } from "../constants/mock-data.constants"
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
}
