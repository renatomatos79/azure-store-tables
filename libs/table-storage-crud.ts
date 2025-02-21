/* eslint-disable no-console */
import { TableClient } from "@azure/data-tables"
import { IBaseEntity } from "../interfaces/entity.interface"
import { executeWithRetry } from "./util"

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
