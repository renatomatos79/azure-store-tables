import {
  TableClient,
  AzureNamedKeyCredential,
  TableServiceClient,
} from "@azure/data-tables";
import {
  createEntity,
  createTable,
  deleteEntity,
  dropTable,
  getEntity,
  isResourceNotFoundError,
  sleep,
  updateEntity,
} from "./lib";
import { randomUUID } from "crypto";

// Your Azure Storage Account details
const accountName = process.env.AZURE_ACCOUNT_NAME ?? "";
const accountKey = process.env.AZURE_ACCOUNT_KEY ?? "";

// Constants
const TABLE_NAME = "CustomersTable";
const TABLE_TO_BE_DELETED = `DeleteTable${randomUUID().replace(/-/g, "")}`;
const MAX_RETRIES = 3;
const RETRY_DELAY_IN_MS = 1000;

// Entity to be inserted
const USERS = [
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
];

console.log("Settings: ", {
  accountName,
  accountKey,
  CrudTable: TABLE_NAME,
  DeleteTable: TABLE_TO_BE_DELETED,
});

// Create a TableClient
const credential = new AzureNamedKeyCredential(accountName, accountKey);
const tableClient = new TableClient(
  `https://${accountName}.table.core.windows.net`,
  TABLE_NAME,
  credential,
);
const serviceClient = new TableServiceClient(
  `https://${accountName}.table.core.windows.net`,
  credential,
);

async function updateBlock() {
  // 4. Should not update when TTL is valid
  const user = USERS[0];
  try {
    console.log("Starting update block for: ", user);
    const row = await getEntity(
      tableClient,
      user,
      MAX_RETRIES,
      RETRY_DELAY_IN_MS,
    );

    if (!row) {
      console.log("Entity not found.");
      return;
    }

    const timestamp = new Date(row.timestamp);
    timestamp.setSeconds(timestamp.getSeconds() + row.ttl);

    const now = new Date();

    console.log("Current time: ", now.toISOString());
    console.log("Entity expiration time: ", timestamp.toISOString());

    if (now < timestamp) {
      console.log("TTL is still valid, not updating the entity: ", row);
    } else {
      console.log("TTL is expired, updating the entity");
      await updateEntity(
        tableClient,
        { ...row, age: row.age * 2 },
        MAX_RETRIES,
        RETRY_DELAY_IN_MS,
      );
    }
  } catch (error) {
    if (isResourceNotFoundError(error)) {
      console.log("Entity not found.");
    } else {
      console.log("updateBlock: ", error);
      throw error;
    }
  }
}

async function testingCRUD() {
  console.log("Testing CRUD operations...");
  await Promise.all(
    USERS.map(async (user) => {
      try {
        const row = await getEntity(tableClient, user, 1, RETRY_DELAY_IN_MS);
        if (!row) {
          console.log("Creating entity: ", user);
          await createEntity(tableClient, user, MAX_RETRIES, RETRY_DELAY_IN_MS);
        } else {
          console.log("Updating entity: ", user);
          await updateEntity(
            tableClient,
            { ...user, age: user.age + 1 },
            MAX_RETRIES,
            RETRY_DELAY_IN_MS,
          );
        }
      } catch (error) {
        if (isResourceNotFoundError(error)) {
          console.log("Entity not found.");
        } else {
          console.log("Testing CRUD operation: ERROR => ", error);
          throw error;
        }
      }
    }),
  );
}

async function main() {
  // 1. Validating create and delete table functions
  await createTable(
    serviceClient,
    TABLE_TO_BE_DELETED,
    MAX_RETRIES,
    RETRY_DELAY_IN_MS,
  );
  await dropTable(
    serviceClient,
    TABLE_TO_BE_DELETED,
    MAX_RETRIES,
    RETRY_DELAY_IN_MS,
  );

  // trying to delete a non-existing table
  await dropTable(
    serviceClient,
    TABLE_TO_BE_DELETED,
    MAX_RETRIES,
    RETRY_DELAY_IN_MS,
  );

  // 2. Creating table for CRUD operations
  await createTable(serviceClient, TABLE_NAME, MAX_RETRIES, RETRY_DELAY_IN_MS);

  // 3. Testing CRUD operations
  await testingCRUD();

  console.log("Waiting for 5 seconds before starting updating...");
  await sleep(5000);

  // 4. Should not update when TTL is valid
  console.log("Checking TTL for the first entity");
  await updateBlock();

  // 5. Should update when TTL is expired (wait for 6 seconds)
  console.log("Waiting for 6 seconds to update the first entity...");
  await sleep(6000);

  console.log("Checking TTL for the second block of entities");
  await updateBlock();

  // 4. Testing DELETE operations for the first 5 entities
  console.log("Deleting entities...");
  const toBeDeleted = USERS.slice(0, 5);
  await Promise.all(
    toBeDeleted.map(async (user) => {
      try {
        await deleteEntity(tableClient, user, MAX_RETRIES, RETRY_DELAY_IN_MS);
      } catch (error) {
        if (isResourceNotFoundError(error)) {
          console.log("Entity not found.");
        } else {
          console.log("Deleting entities: ERROR => ", error);
          throw error;
        }
      }
    }),
  );

  console.log("Done");
}

// Run the main function
main().catch((err) => console.error("Error:", err));
