/* eslint-disable no-console */
import {
  AzureNamedKeyCredential,
  RestError,
  TableClient,
  TableServiceClient,
} from "@azure/data-tables"
import { IBaseEntity, IUserEntity } from "./interfaces/entity.interface"

export const createTable = (
  serviceClient: TableServiceClient,
  tableName: string,
  maxRetries: number,
  retryDelayInMs: number,
) => {
  return executeWithRetry(
    () => {
      return serviceClient.createTable(tableName, {
        onResponse: (response) => {
          if (response.status === 409) {
            console.log(`Table ${tableName} already exists`)
          }
        },
      })
    },
    maxRetries,
    retryDelayInMs,
  )
}

export const dropTable = (
  serviceClient: TableServiceClient,
  tableName: string,
  maxRetries: number,
  retryDelayInMs: number,
) => {
  return executeWithRetry(
    () => {
      return serviceClient.deleteTable(tableName, {
        onResponse: (response) => {
          console.log(`Table ${tableName} deleted with status ${response}`)

          if (response.status === 404) {
            console.log(`Table ${tableName} not found`)
          }
        },
      })
    },
    maxRetries,
    retryDelayInMs,
  )
}

export const getTables = (
  serviceClient: TableServiceClient,
  maxRetries: number,
  retryDelayInMs: number,
) => {
  return executeWithRetry(
    async () => {
      const tables: string[] = []
      const tableItems = serviceClient.listTables()
      for await (const table of serviceClient.listTables()) {
        tables.push(table.name)
      }
      return tables
    },
    maxRetries,
    retryDelayInMs,
  )
}

export const tableExists = async (
  serviceClient: TableServiceClient,
  tableName: string,
  maxRetries: number,
  retryDelayInMs: number,
) => {
  return (await getTables(serviceClient, maxRetries, retryDelayInMs)).includes(
    tableName,
  )
}

export const executeWithRetry = async (
  func: () => Promise<any>,
  maxRetries: number,
  retryDelayInMs: number,
  noRetryOnResourceNotFound: boolean = false,
) => {
  let numRetries = 0
  let errorToReturn: any = null
  while (numRetries < maxRetries) {
    try {
      return await func()
    } catch (error) {
      errorToReturn = error
      if (noRetryOnResourceNotFound && isResourceNotFoundError(error)) {
        break
      }
      console.error(
        `Error executing function ${func.name}: ${
          error instanceof RestError
        } ${error.message}`,
      )
      numRetries++
      await sleep(retryDelayInMs)
    }
  }
  console.error(
    `executeWithRetry: Function failed after ${numRetries + 1} retries.`,
  )
  throw (
    errorToReturn ??
    new Error(`Function failed after ${numRetries + 1} retries.`)
  )
}

export const getEntity = <T extends IBaseEntity>(
  tableClient: TableClient,
  { partitionKey, rowKey }: IBaseEntity,
  maxRetries: number = 0,
  retryDelayInMs: number = 0,
) => {
  return executeWithRetry(
    () => tableClient.getEntity<T>(partitionKey, rowKey),
    maxRetries,
    retryDelayInMs,
  )
}

export const createEntity = <
  T extends { partitionKey: string; rowKey: string },
>(
  tableClient: TableClient,
  entity: T,
  maxRetries: number,
  retryDelayInMs: number,
) => {
  return executeWithRetry(
    () => tableClient.createEntity(entity),
    maxRetries,
    retryDelayInMs,
  )
}

export const deleteEntity = <
  T extends { partitionKey: string; rowKey: string },
>(
  tableClient: TableClient,
  entity: T,
  maxRetries: number,
  retryDelayInMs: number,
) => {
  const { partitionKey, rowKey } = entity

  return executeWithRetry(
    () => tableClient.deleteEntity(partitionKey, rowKey),
    maxRetries,
    retryDelayInMs,
    true,
  )
}

export const updateEntity = <
  T extends { partitionKey: string; rowKey: string },
>(
  tableClient: TableClient,
  entity: T,
  maxRetries: number,
  retryDelayInMs: number,
) => {
  return executeWithRetry(
    () => tableClient.updateEntity(entity, "Merge"),
    maxRetries,
    retryDelayInMs,
  )
}

export const isStatusCodeError = (error: Error, statusCode: number) => {
  return error instanceof RestError && error.statusCode === statusCode
}

export const isResourceNotFoundError = (error: Error) => {
  return isStatusCodeError(error, 404)
}

export const sleep = (ms: number) => {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

export const getTableClient = (
  accountName: string,
  accountKey: string,
  tableName: string,
) => {
  const credential = new AzureNamedKeyCredential(accountName, accountKey)
  return new TableClient(
    `https://${accountName}.table.core.windows.net`,
    tableName,
    credential,
  )
}

export const getTableServiceClient = (
  accountName: string,
  accountKey: string,
) => {
  const credential = new AzureNamedKeyCredential(accountName, accountKey)
  return new TableServiceClient(
    `https://${accountName}.table.core.windows.net`,
    credential,
  )
}

export const isObjectSubset = <T extends Record<string, any>>(
  obj1: T,
  obj2: T,
): boolean => {
  return Object.entries(obj1).every(([key, value]) => obj2[key] === value)
}
