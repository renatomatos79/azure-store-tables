import { TableClient, AzureNamedKeyCredential, odata } from "@azure/data-tables";

// Your Azure Storage Account details
const accountName = process.env.AZURE_ACCOUNT_NAME ?? "";
const accountKey = process.env.AZURE_ACCOUNT_KEY ?? "";
const tableName = "CustomersTableV4";

console.log('Settings: ', {  accountName, accountKey, tableName });

// Create a TableClient
const credential = new AzureNamedKeyCredential(accountName, accountKey);
const tableClient = new TableClient(`https://${accountName}.table.core.windows.net`, tableName, credential);

async function tableExists() {
    try {
        await tableClient.getAccessPolicy();
        return true;
    } catch (error) {
        console.log('tableExists status code: ', error.statusCode);
        if (error.statusCode === 404) {
        return false;
        }
        throw error;
    }
}

async function getEntity(partitionKey, rowKey) {
    try {
        const entity = await tableClient.getEntity(partitionKey, rowKey);
        console.log(`Entity with partitionKey "${partitionKey}" and rowKey "${rowKey}" exists.`);
        return entity;
    } catch (error) {
        if (error.statusCode === 404) {
        console.log(`Entity with partitionKey "${partitionKey}" and rowKey "${rowKey}" does not exist.`);
        return null;
        }
        throw error;
    }
}

async function main() {
  // 1. Create a table if it does not exist
  const tableExistsResult = await tableExists();
  if (tableExistsResult) {
    console.log(`Table "${tableName}" already exists.`);
  } else {
    console.log("Creating table...");
    await tableClient.createTable();
    console.log(`Table "${tableName}" created.`);
  }

  // 2. Insert an entity
  const entity = {
    partitionKey: "Users",
    rowKey: "321",
    name: "John Doe",
    email: "john.doe@example.com",
    age: 30
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
    { partitionKey: entity.partitionKey, rowKey: entities.rowKey, age: 31 }, // Partial update
    "Merge"
  );
  console.log("Entity updated.");

  // 5. Query entities (filtering by age)
  console.log("Querying entities...");
  const entities = tableClient.listEntities({
    queryOptions: { filter: odata`age eq 31` }
  });

  for await (const entity of entities) {
    console.log("Queried Entity:", entity);
  }

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
