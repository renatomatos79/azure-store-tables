/* eslint-disable no-console */
import {RestError, TableClient, TableServiceClient} from '@azure/data-tables'

export const createTable = (
    serviceClient: TableServiceClient,
    tableName: string,
    maxRetries: number,
    retryDelayInMs: number
) => {
    return executeWithRetry(
        () => {
            return serviceClient.createTable(tableName, {
                onResponse: (response) => {
                    if (response.status === 409) {
                        console.log(`Table ${tableName} already exists`);
                    }
                }
            })
        },
        maxRetries,
        retryDelayInMs
    )
}

export const dropTable = (
    serviceClient: TableServiceClient,
    tableName: string,
    maxRetries: number,
    retryDelayInMs: number
) => {
    return executeWithRetry(
        () => {
            return serviceClient.deleteTable(tableName, {
                onResponse: (response) => {
                    console.log(`Table ${tableName} deleted with status ${response}`);

                    if (response.status === 404) {
                        console.log(`Table ${tableName} not found`);
                    }
                }
            })
        },
        maxRetries,
        retryDelayInMs
    )
}

export const executeWithRetry = async (
    func: () => Promise<any>,
    maxRetries: number,
    retryDelayInMs: number,
    noRetryOnResourceNotFound: boolean = false
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
                } ${error.message}`
            )
            numRetries++
            await new Promise(resolve => setTimeout(resolve, retryDelayInMs))
        }
    }
    console.error(
        `executeWithRetry: Function failed after ${numRetries + 1} retries.`
    )
    throw (
        errorToReturn ??
        new Error(`Function failed after ${numRetries + 1} retries.`)
    )
}

export const getEntity = (
    tableClient: TableClient,
    {partitionKey, rowKey}: { partitionKey: string, rowKey: string },
    maxRetries: number,
    retryDelayInMs: number
) => {
    return executeWithRetry(
        () => tableClient.getEntity(partitionKey, rowKey),
        maxRetries,
        retryDelayInMs,
        true
    )
}

export const createEntity = <T extends { partitionKey: string, rowKey: string }>(
    tableClient: TableClient,
    entity: T,
    maxRetries: number,
    retryDelayInMs: number
) => {
    return executeWithRetry(
        () => tableClient.createEntity(entity),
        maxRetries,
        retryDelayInMs
    )
}


export const deleteEntity = <T extends { partitionKey: string, rowKey: string }>(
    tableClient: TableClient,
    entity: T,
    maxRetries: number,
    retryDelayInMs: number
) => {
    const {partitionKey, rowKey} = entity

    return executeWithRetry(
        () => tableClient.deleteEntity(partitionKey, rowKey),
        maxRetries,
        retryDelayInMs,
        true
    )
}

export const updateEntity = <T extends { partitionKey: string, rowKey: string }>(
    tableClient: TableClient,
    entity: T,
    maxRetries: number,
    retryDelayInMs: number
) => {
    return executeWithRetry(
        () => tableClient.updateEntity(entity, "Merge"),
        maxRetries,
        retryDelayInMs
    )
}

export const isStatusCodeError = (error: Error, statusCode: number) => {
    return error instanceof RestError && error.statusCode === statusCode
}

export const isResourceNotFoundError = (error: Error) => {
    return isStatusCodeError(error, 404)
}