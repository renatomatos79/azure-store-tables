/* eslint-disable no-console */
import { TableServiceClient } from "@azure/data-tables"
import { executeWithRetry } from "./util"

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
          console.log(`Table ${tableName} dropped`)

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
