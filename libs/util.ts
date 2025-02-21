import {
  AzureNamedKeyCredential,
  RestError,
  TableClient,
  TableServiceClient,
} from "@azure/data-tables"

export const isStatusCodeError = (error: Error, statusCode: number) => {
  return error instanceof RestError && error.statusCode === statusCode
}

export const isResourceNotFoundError = (error: Error) => {
  return isStatusCodeError(error, 404)
}

export const sleep = (ms: number) => {
  return new Promise((resolve) => setTimeout(resolve, ms))
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
