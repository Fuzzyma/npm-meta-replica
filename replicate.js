import fs from "fs/promises";
import { setTimeout } from "timers/promises";

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

async function* fetchDocuments(batchSize = 100) {
  let { startKey: lastKey } = await getCheckpoint();
  let hasMore = true;
  let totalDocs = null;

  let totalFetchTime = 0;
  let fetchCount = 0;

  while (hasMore) {
    const fetchStartTime = Date.now();
    const url = `${remoteDbUrl}?${
      includeDocs ? "include_docs=true&" : ""
    }limit=${batchSize}&startkey=${encodeURIComponent(
      JSON.stringify(lastKey)
    )}`;

    let data;
    while (true) {
      const response = await fetch(url);

      try {
        data = await response.json();
        if (!data) throw new Error("Empty response");
        break;
        // eslint-disable-next-line
      } catch (e) {
        console.warn(
          "Failed to parse JSON or empty response. Waiting 10sec and trying again"
        );
        await setTimeout(10000);
      }
    }

    const fetchEndTime = Date.now();

    const fetchTime = (fetchEndTime - fetchStartTime) / 1000; // Time in seconds
    totalFetchTime += fetchTime;
    fetchCount++;
    const averageFetchTime = (totalFetchTime / fetchCount).toFixed(2);

    // Initialize total document count
    if (totalDocs === null) totalDocs = data.total_rows;

    const fetchedCount = data.offset + data.rows.length;

    if (data.rows.length === 0) {
      hasMore = false;
    } else {
      console.log(
        `Fetched ${fetchedCount} / ${totalDocs} documents - ${fetchTime.toFixed(
          2
        )}s (avg: ${averageFetchTime}s)`
      );

      if (includeDocs) {
        lastKey = data.rows[data.rows.length - 1].id;
        yield {
          docs: data.rows.map((row) => row.doc),
          lastKey: data.rows[data.rows.length - 1].id,
        };
      } else {
        lastKey = data.rows[data.rows.length - 1].id;
        yield {
          docs: data.rows.map((d) => {
            delete d.value;
            return d;
          }),
          lastKey: lastKey,
        };
      }
    }
  }
}

async function replicateDocuments(batchSize = 100) {
  console.log("Starting replication...");
  for await (const { docs, lastKey } of fetchDocuments(batchSize)) {
    const bulkDocsPayload = {
      docs: docs.map((doc) => {
        return { _id: doc.id };
      }),
    };

    while (true) {
      try {
        const response = await fetch(`${localDbUrl}/_bulk_docs`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Authorization: authHeader,
          },
          body: JSON.stringify(bulkDocsPayload),
        });

        if (!response.ok) {
          console.error(
            "Failed to replicate documents:",
            await response.text()
          );
          await setTimeout(2000);
        } else {
          console.log(
            `Replicated ${docs.length} documents.`,
            "Last key:",
            lastKey
          );
          await saveCheckpoint(lastKey); // + "\u0000" Save the checkpoint after each successful batch
          break;
        }
      } catch (e) {
        console.log(e);
        await setTimeout(2000);
      }
    }
  }
  console.log("Replication complete!");
}

replicateDocuments(100).catch(console.error);
