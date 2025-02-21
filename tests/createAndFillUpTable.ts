// Entity to be inserted
import {
  createEntity,
  createTable,
  deleteEntity,
  getEntity,
  getTableClient,
  getTableServiceClient,
  isObjectSubset,
} from "../libs"

import { IUserEntity } from "../interfaces/entity.interface"
import { USERS } from "../constants/mock-data.constants"

export const createAndFillUpTable = async (
  accountName: string,
  accountKey: string,
  tableName: string,
  maxRetries: number,
  retryDelayInMillis: number,
): Promise<void> => {
  console.log(`Creating and filling up table ${tableName}`)

  const tableSvcClient = getTableServiceClient(accountName, accountKey)
  const tableClient = getTableClient(accountName, accountKey, tableName)

  await createTable(tableSvcClient, tableName, maxRetries, retryDelayInMillis)

  await Promise.all(
    USERS.map(async (user) => {
      await createEntity(
        tableClient,
        user,
        maxRetries,
        retryDelayInMillis,
      ).catch((error: Error) => {
        console.error("createAndFillUpTable::ERROR => ", error)
      })
    }),
  )
}

export const shouldFindEverySingleRecord = async (
  accountName: string,
  accountKey: string,
  tableName: string,
  maxRetries: number,
  retryDelayInMillis: number,
): Promise<void> => {
  console.log(`Validating appended records ${tableName}`)

  const tableClient = getTableClient(accountName, accountKey, tableName)

  await Promise.all(
    USERS.map(async (user) => {
      await getEntity<IUserEntity>(
        tableClient,
        { partitionKey: user.partitionKey, rowKey: user.rowKey },
        maxRetries,
        retryDelayInMillis,
      )
        .then((result: IUserEntity) => {
          const found = isObjectSubset(user, result)
          if (!found) {
            const json1 = JSON.stringify(user)
            const json2 = JSON.stringify(result)

            console.log(`record not found: ${json1} on result ${json2}`)
          }
        })
        .catch((error: Error) => {
          console.error("shouldFindEverySingleRecord::ERROR => ", error)
        })
    }),
  )
}

export const deleteEntities = async (
  accountName: string,
  accountKey: string,
  tableName: string,
  maxRetries: number,
  retryDelayInMillis: number,
): Promise<void> => {
  console.log(`Deleting entities from ${tableName}`)

  const tableClient = getTableClient(accountName, accountKey, tableName)

  await Promise.all(
    USERS.map(async (user) => {
      await deleteEntity(
        tableClient,
        user,
        maxRetries,
        retryDelayInMillis,
      ).catch((error: Error) => {
        console.error("deleteEntities::ERROR => ", error)
      })
    }),
  )
}
