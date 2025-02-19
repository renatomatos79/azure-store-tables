import { TableClient, AzureNamedKeyCredential, TableServiceClient } from "@azure/data-tables";

// Your Azure Storage Account details
const accountName = process.env.AZURE_ACCOUNT_NAME ?? "";
const accountKey = process.env.AZURE_ACCOUNT_KEY ?? "";
const tableName = "CustomersTable";

console.log('Settings: ', {  accountName, accountKey, tableName });

// Create a TableClient
const credential = new AzureNamedKeyCredential(accountName, accountKey);
const tableClient = new TableClient(`https://${accountName}.table.core.windows.net`, tableName, credential);
const serviceClient = new TableServiceClient(
  `https://${accountName}.table.core.windows.net`,
  credential
);
async function createTableIfNeeded() {
  console.log(`Creating table  ${tableName} ...`);
  await serviceClient.createTable(tableName, {
    onResponse: (response) => {
      console.log('createTableIfNeeded response: ', response.status);
      if (response.status === 409) {
        console.log(`Table ${tableName} already exists`);
      }
    }
  });
}

async function getEntity(partitionKey, rowKey) {
    try {
        const entity = await tableClient.getEntity(partitionKey, rowKey);
        console.log(`Entity with partitionKey "${partitionKey}" and rowKey "${rowKey}" exists.`);
        return entity;
    } catch (error) {
        if (error?.statusCode === 404) {
          console.log(`Entity with partitionKey "${partitionKey}" and rowKey "${rowKey}" does not exist.`);
          return null;
        }
        throw error;
    }
}

async function main() {
  // 1. Create the table if it does not exist
  await createTableIfNeeded()

  // 2. Insert an entity
  const entity = {
    partitionKey: "Users",
    rowKey: "321",
    name: "John Doe",
    email: "john.doe@example.com",
    age: 30,
    ttl: 5, // Entity will expire in 20 seconds
  };

  // 3. Retrieve an entity
  console.log("Retrieving entity...");
  const fetchedEntity = await getEntity(entity.partitionKey, entity.rowKey);
  console.log("Fetched Entity:", fetchedEntity);

  if (!fetchedEntity) {
    console.log("Inserting entity...");

    await tableClient.createEntity(entity);
    console.log("Entity inserted:", entity);
  
  }

  // 4. Update an entity (Merge mode)
  console.log("Updating entity...");
  await tableClient.updateEntity(
    { partitionKey: entity.partitionKey, rowKey: entity.rowKey, age: 55 }, // Partial update
    "Merge"
  );
  console.log("Entity updated.");

  // 5. Update an entity (Replace mode)
  const updContent = await getEntity(entity.partitionKey, entity.rowKey);
  
  const timestamp = new Date(updContent.timestamp);
  timestamp.setSeconds(timestamp.getSeconds() + updContent.ttl)
  
  console.log("updContent: ", updContent.timestamp, updContent.ttl, 'expires: ', timestamp);


  // 6. Delete an entity
  //console.log("Deleting entity...");
  //await tableClient.deleteEntity("Users", "123");
  //console.log("Entity deleted.");

  // 7. Delete the table
  //console.log("Deleting table...");
  //await tableClient.deleteTable();
  //console.log(`Table "${tableName}" deleted.`);
}

// Run the main function
main().catch(err => console.error("Error:", err));
