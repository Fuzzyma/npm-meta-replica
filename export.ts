import fs from "fs";
import fsp from "fs/promises";
import { setTimeout } from "timers/promises";

const localDbUrl = "http://localhost:5984/registry2";
const authHeader = "Basic " + Buffer.from("admin:admin").toString("base64");
const checkpointFile = "checkpoint5.json"; // File to store the last processed startKey

async function getCheckpoint() {
  try {
    const data = await fsp.readFile(checkpointFile, "utf8");
    const { startKey } = JSON.parse(data);
    return { startKey: startKey || "" };
  } catch {
    return { startKey: "" }; // Start from the beginning if no checkpoint exists
  }
}

async function saveCheckpoint(startKey) {
  const checkpointData = JSON.stringify({ startKey }, null, 2);
  await fsp.writeFile(checkpointFile, checkpointData, "utf8");
}

let totalDocs = null;
let fetchedCount = 0;
async function* fetchDocuments(batchSize = 10000) {
  let { startKey: lastKey } = await getCheckpoint();
  let hasMore = true;

  while (hasMore) {
    const url = `${localDbUrl}/_all_docs?limit=${batchSize}${
      lastKey ? "&skip=1" : ""
    }&startkey=${encodeURIComponent(JSON.stringify(lastKey))}`;

    let data;
    while (true) {
      try {
        const response = await fetch(url, {
          headers: {
            Authorization: authHeader,
          },
        });
        data = await response.json();
        if (!data || data.error)
          throw new Error((data && data.error) || "Empty response");
        break;
        // eslint-disable-next-line
      } catch (e) {
        console.warn(e.message);
        await setTimeout(10000);
      }
    }

    // Initialize total document count
    if (totalDocs === null) totalDocs = data.total_rows;

    fetchedCount = data.offset + data.rows.length;

    if (data.rows.length === 0) {
      hasMore = false;
    } else {
      const rows = data.rows as { id: string }[];

      console.log(`Fetched ${fetchedCount} / ${totalDocs} documents`);

      lastKey = data.rows[data.rows.length - 1].id;
      yield {
        docs: rows.map((row) => '"' + row.id + '"'),
        lastKey: lastKey,
      };
    }
  }
}

let stop = false;
async function main() {
  let writableStream = fs.createWriteStream("packages/packages1.json", {
    flags: "a",
  });

  // const { startKey } = await getCheckpoint();
  // if (!startKey) {
  writableStream.write("[");
  // }

  let total = 0;
  let runs = 1;

  for await (const { docs, lastKey } of fetchDocuments()) {
    if (stop) {
      writableStream.close(() => {
        process.exit();
      });
      return;
    }
    const str = docs.join(",");
    writableStream.write((total === 0 ? "" : ",") + str);

    total += docs.length;

    if (total === 200000) {
      total = 0;
      runs++;
      writableStream.write("]");
      writableStream.close();
      writableStream = fs.createWriteStream(`packages/packages${runs}.json`);
      writableStream.write("[");
    }

    // await saveCheckpoint(lastKey);
    if (stop) {
      writableStream.close(() => {
        process.exit();
      });
      return;
    }

    await setTimeout(200);
  }

  writableStream.write("]");

  writableStream.close();
}

main();

process.on("SIGINT", async () => {
  stop = true;
});
