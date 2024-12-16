import fs from "fs/promises";

const remoteDbUrl = "https://replicate.npmjs.com/registry/_all_docs";
const localDbUrl = "http://localhost:5984/registry2";
const authHeader = "Basic " + Buffer.from("admin:admin").toString("base64");
const checkpointFile = "checkpoint2.json"; // File to store the last processed startKey
const includeDocs = false;

async function getCheckpoint() {
  try {
    const data = await fs.readFile(checkpointFile, "utf8");
    const { startKey } = JSON.parse(data);
    return { startKey: startKey || "" };
  } catch {
    return { startKey: "" }; // Start from the beginning if no checkpoint exists
  }
}

async function saveCheckpoint(startKey) {
  const checkpointData = JSON.stringify({ startKey }, null, 2);
  await fs.writeFile(checkpointFile, checkpointData, "utf8");
}

async function fetchPage(startKey, batchSize, skip) {
  const url = `${remoteDbUrl}?${
    includeDocs ? "include_docs=true&" : ""
  }limit=${batchSize}&startkey=${encodeURIComponent(
    JSON.stringify(startKey)
  )}&skip=${skip}`;
  const response = await fetch(url);
  const data = await response.json();
  return data;
}

async function* fetchDocuments(batchSize = 100, parallelFetchCount = 5) {
  let { startKey: lastKey } = await getCheckpoint();
  let hasMore = true;
  let totalDocs = null;

  let totalFetchTime = 0;
  let fetchCount = 0;

  while (hasMore) {
    const fetchStartTime = Date.now();
    const fetchPromises = [];
    for (let i = 0; i < parallelFetchCount; i++) {
      const skip = i * batchSize; // Skip records for the current page
      fetchPromises.push(fetchPage(lastKey, batchSize, skip));
    }

    const results = await Promise.all(fetchPromises);

    const fetchEndTime = Date.now();

    const fetchTime = (fetchEndTime - fetchStartTime) / 1000; // Time in seconds
    totalFetchTime += fetchTime;
    fetchCount++;
    const averageFetchTime = (totalFetchTime / fetchCount).toFixed(2);

    const fetchedCount = results[0].offset + batchSize * parallelFetchCount;

    console.log(
      `Fetched ${fetchedCount} / ${totalDocs} documents - ${fetchTime.toFixed(
        2
      )}s (avg: ${averageFetchTime}s)`
    );

    for (const data of results) {
      if (!totalDocs) totalDocs = data.total_rows;
      if (data.rows.length === 0) {
        hasMore = false;
        break;
      }

      const docs = includeDocs
        ? data.rows.map((row) => row.doc)
        : data.rows.map((row) => {
            delete row.value;
            return row;
          });

      lastKey = data.rows[data.rows.length - 1].id;

      yield {
        docs,
        lastKey,
      };
    }

    // Stop fetching if the last fetch returned fewer rows than expected
    if (results.some((data) => data.rows.length < batchSize)) {
      hasMore = false;
    }
  }
}

async function replicateDocuments(batchSize = 100, parallelFetchCount = 5) {
  console.log("Starting replication...");
  for await (const { docs, lastKey } of fetchDocuments(
    batchSize,
    parallelFetchCount
  )) {
    const bulkDocsPayload = {
      docs: docs.map((doc) => {
        return { _id: doc.id };
      }),
    };

    const response = await fetch(`${localDbUrl}/_bulk_docs`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: authHeader,
      },
      body: JSON.stringify(bulkDocsPayload),
    });

    if (!response.ok) {
      console.error("Failed to replicate documents:", await response.text());
    } else {
      console.log(`Replicated ${docs.length} documents.`, "Last key:", lastKey);
      await saveCheckpoint(lastKey);
    }
  }
  console.log("Replication complete!");
}

replicateDocuments(100, 5).catch(console.error);
