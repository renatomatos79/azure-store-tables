export interface IUserEntity extends IBaseEntity {
  name: string
  email: string
  age: number
  ttl: number
}

export interface IBaseEntity {
  partitionKey: string
  rowKey: string
}
