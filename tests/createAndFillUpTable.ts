// Entity to be inserted
import {
  createEntity,
  createTable,
  getEntity,
  getTableClient,
  getTableServiceClient,
  isObjectSubset,
} from "../lib"

import { IUserEntity } from "../interfaces/entity.interface"

const USERS: IUserEntity[] = [
  {
    partitionKey: "Users",
    rowKey: "001",
    name: "Alice Smith",
    email: "alice.smith@example.com",
    age: 25,
    ttl: 5,
  },
  {
    partitionKey: "Users",
    rowKey: "002",
    name: "Bob Johnson",
    email: "bob.johnson@example.com",
    age: 32,
    ttl: 5,
  },
  {
    partitionKey: "Users",
    rowKey: "003",
    name: "Charlie Brown",
    email: "charlie.brown@example.com",
    age: 28,
    ttl: 5,
  },
  {
    partitionKey: "Users",
    rowKey: "004",
    name: "David Wilson",
    email: "david.wilson@example.com",
    age: 40,
    ttl: 5,
  },
  {
    partitionKey: "Users",
    rowKey: "005",
    name: "Emma Davis",
    email: "emma.davis@example.com",
    age: 27,
    ttl: 5,
  },
  {
    partitionKey: "Users",
    rowKey: "006",
    name: "Frank Miller",
    email: "frank.miller@example.com",
    age: 35,
    ttl: 5,
  },
  {
    partitionKey: "Users",
    rowKey: "007",
    name: "Grace Harris",
    email: "grace.harris@example.com",
    age: 29,
    ttl: 5,
  },
  {
    partitionKey: "Users",
    rowKey: "008",
    name: "Henry Clark",
    email: "henry.clark@example.com",
    age: 31,
    ttl: 5,
  },
  {
    partitionKey: "Users",
    rowKey: "009",
    name: "Isabella Lewis",
    email: "isabella.lewis@example.com",
    age: 26,
    ttl: 5,
  },
  {
    partitionKey: "Users",
    rowKey: "010",
    name: "Jack Hall",
    email: "jack.hall@example.com",
    age: 38,
    ttl: 5,
  },
  {
    partitionKey: "Users",
    rowKey: "011",
    name: "Karen Allen",
    email: "karen.allen@example.com",
    age: 33,
    ttl: 5,
  },
  {
    partitionKey: "Users",
    rowKey: "012",
    name: "Liam Young",
    email: "liam.young@example.com",
    age: 24,
    ttl: 5,
  },
  {
    partitionKey: "Users",
    rowKey: "013",
    name: "Mia King",
    email: "mia.king@example.com",
    age: 37,
    ttl: 5,
  },
  {
    partitionKey: "Users",
    rowKey: "014",
    name: "Noah Scott",
    email: "noah.scott@example.com",
    age: 30,
    ttl: 5,
  },
  {
    partitionKey: "Users",
    rowKey: "015",
    name: "Olivia Green",
    email: "olivia.green@example.com",
    age: 28,
    ttl: 5,
  },
  {
    partitionKey: "Users",
    rowKey: "016",
    name: "Paul Adams",
    email: "paul.adams@example.com",
    age: 41,
    ttl: 5,
  },
  {
    partitionKey: "Users",
    rowKey: "017",
    name: "Quinn Baker",
    email: "quinn.baker@example.com",
    age: 23,
    ttl: 5,
  },
  {
    partitionKey: "Users",
    rowKey: "018",
    name: "Ryan Carter",
    email: "ryan.carter@example.com",
    age: 36,
    ttl: 5,
  },
  {
    partitionKey: "Users",
    rowKey: "019",
    name: "Sophia Nelson",
    email: "sophia.nelson@example.com",
    age: 29,
    ttl: 5,
  },
  {
    partitionKey: "Users",
    rowKey: "020",
    name: "Thomas Wright",
    email: "thomas.wright@example.com",
    age: 34,
    ttl: 5,
  },
  {
    partitionKey: "Users",
    rowKey: "021",
    name: "Ursula Mitchell",
    email: "ursula.mitchell@example.com",
    age: 39,
    ttl: 5,
  },
  {
    partitionKey: "Users",
    rowKey: "022",
    name: "Victor Perez",
    email: "victor.perez@example.com",
    age: 26,
    ttl: 5,
  },
  {
    partitionKey: "Users",
    rowKey: "023",
    name: "Wendy Roberts",
    email: "wendy.roberts@example.com",
    age: 32,
    ttl: 5,
  },
  {
    partitionKey: "Users",
    rowKey: "024",
    name: "Xavier Evans",
    email: "xavier.evans@example.com",
    age: 27,
    ttl: 5,
  },
  {
    partitionKey: "Users",
    rowKey: "025",
    name: "Yvonne Edwards",
    email: "yvonne.edwards@example.com",
    age: 31,
    ttl: 5,
  },
  {
    partitionKey: "Users",
    rowKey: "026",
    name: "Zachary Collins",
    email: "zachary.collins@example.com",
    age: 30,
    ttl: 5,
  },
  {
    partitionKey: "Users",
    rowKey: "027",
    name: "Amber Stewart",
    email: "amber.stewart@example.com",
    age: 29,
    ttl: 5,
  },
  {
    partitionKey: "Users",
    rowKey: "028",
    name: "Brandon Morris",
    email: "brandon.morris@example.com",
    age: 28,
    ttl: 5,
  },
  {
    partitionKey: "Users",
    rowKey: "029",
    name: "Catherine Rogers",
    email: "catherine.rogers@example.com",
    age: 37,
    ttl: 5,
  },
  {
    partitionKey: "Users",
    rowKey: "030",
    name: "Daniel Bailey",
    email: "daniel.bailey@example.com",
    age: 35,
    ttl: 5,
  },
]

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
        .then((result) => {
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
