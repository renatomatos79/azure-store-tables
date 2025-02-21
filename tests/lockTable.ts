// Entity to be inserted
import {
  addLog,
  createEntity,
  createTable,
  deleteEntity,
  getTableClient,
  getTableServiceClient,
  sleep,
} from "../libs"

import { IBaseEntity } from "../interfaces/entity.interface"

export const createLock = async (
  accountName: string,
  accountKey: string,
  tableName: string,
  resource: IBaseEntity,
  description: string,
  maxRetries: number,
  retryDelayInMillis: number,
): Promise<void> => {
  addLog("LOCK", `Creating lock for ${description} on ${tableName}`)

  const tableSvcClient = getTableServiceClient(accountName, accountKey)
  const tableClient = getTableClient(accountName, accountKey, tableName)

  await createTable(tableSvcClient, tableName, maxRetries, retryDelayInMillis)

  // create a lock
  await createEntity(
    tableClient,
    resource,
    maxRetries,
    retryDelayInMillis,
  ).catch((error: Error) => {
    console.error("createLock::createEntity::ERROR => ", error)
  })

  addLog("LOCK", `Lock ${description} created on ${tableName}`)

  await sleep(3000)

  // removes the lock
  await deleteEntity(
    tableClient,
    resource,
    maxRetries,
    retryDelayInMillis,
  ).catch((error: Error) => {
    console.error("createLock::deleteEntity::ERROR => ", error)
  })

  addLog("LOCK", `Lock ${description} removed from ${tableName}`)
}
