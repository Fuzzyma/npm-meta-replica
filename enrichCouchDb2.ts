import fs from "fs/promises";
import pc from "picocolors";
import * as readline from "readline";
import { setTimeout } from "timers/promises";
import { client } from "./api-client";

export type PackageDetails =
  | {
      name: string;
      error: Error | 404;
    }
  | {
      error: null;
      name: string;
      latestVersion: string;
      downloads: number | null;
      dependencies: Record<string, string>;
      devDependencies: Record<string, string>;
      unpackedSize: number;
      fileCount: number;
    };

// const localDbUrl = "http://localhost:5984/registry2";
const localDbUrl = "https://npm.devminer.xyz/registry2";
// const authHeader = "Basic " + Buffer.from("admin:admin").toString("base64");
const auth = "";
const authHeader = "Basic " + Buffer.from(auth).toString("base64");
const checkpointFile = "checkpoint6.json"; // File to store the last processed startKey

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

type CouchDBResponse = {
  total_rows: number;
  offset: number;
  rows: {
    id: string;
    key: string;
    value: {
      rev: string;
    };
    doc: {
      _id: string;
      _rev: string;
      version: string;
      downloads: number;
      dependencies: object[];
      devDependencies: object[];
      unpackedSize: number;
      fileCount: number;
    };
  }[];
  error?: string;
};

let totalDocs: null | number = null;
let fetchedCount = 0;
async function* fetchDocuments(batchSize = 100) {
  let { startKey: lastKey } = await getCheckpoint();
  let hasMore = true;

  while (hasMore) {
    const url = `${localDbUrl}/_all_docs?limit=${batchSize}${
      lastKey ? "&skip=1" : ""
    }&startkey=${encodeURIComponent(
      JSON.stringify(lastKey)
    )}&include_docs=true`;

    let data: CouchDBResponse;
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
      console.log(`Fetched ${fetchedCount} / ${totalDocs} documents`);

      lastKey = data.rows[data.rows.length - 1].id;
      yield {
        docs: data.rows.filter(
          (d) => !d.id.startsWith("_design") || d.doc.downloads !== null
        ),
        lastKey: data.rows[data.rows.length - 1].id,
      };
    }
  }
}

async function getDownloadStats(packageNames: string[]) {
  const response = await client.getObjects<{
    downloadsLast30Days: number;
    objectID: string;
  }>({
    requests: packageNames.map((packageName) => {
      return {
        indexName: "npm-search",
        objectID: packageName,
        attributesToRetrieve: ["downloadsLast30Days"],
      };
    }),
  });

  return response.results.map((result, index) => ({
    name: result?.objectID ?? packageNames[index],
    downloads: result?.downloadsLast30Days ?? null,
  }));
}

let cnt = 0;
let total = 0;
let packagesFetched = 0;
async function main(batchSize = 100) {
  for await (const { docs, lastKey } of fetchDocuments(batchSize)) {
    const overall = performance.now();
    const d = docs.map((d) => d.id);

    await setTimeout(1000);

    const stats = await getDownloadStats(d);

    const statsObject = Object.fromEntries(
      stats.map(({ name, downloads }) => {
        return [name, downloads];
      })
    );

    const bulkDocsPayload = {
      docs: docs.map((doc) => {
        return {
          ...doc.doc,
          downloads: statsObject[doc.id],
        };
      }),
    };

    while (true) {
      try {
        const response = await fetch(`${localDbUrl}/_bulk_docs`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            // Authorization: authHeader,
          },
          body: JSON.stringify(bulkDocsPayload),
        });

        console.log(await response.text());

        if (!response.ok) {
          console.error("Failed to enrich documents:", await response.text());
          await setTimeout(2000);
        } else {
          readline.moveCursor(process.stdout, 0, 1);
          // readline.clearLine(process.stdout, 0);
          console.log(
            `Enriched ${docs.length} documents.`,
            "Last key:",
            pc.red(lastKey)
          );
          await saveCheckpoint(lastKey); // + "\u0000" Save the checkpoint after each successful batch
          const ti = (performance.now() - overall) / 1000;
          total += ti;
          const pckPerS = packagesFetched / total;
          const remainingTime = (totalDocs ?? 0 - packagesFetched) / pckPerS;
          console.log(
            `Took: ${ti.toFixed(2)}s (avg: ${(total / ++cnt).toFixed(
              2
            )}s) (pck/s: ${pc.yellow(
              (packagesFetched / total).toFixed(2)
            )}) (${pc.cyan(
              (remainingTime / 60 / 60).toFixed(2) + "h"
            )} remaining)\n`
          );
          break;
        }
      } catch (e) {
        console.log(e);
        await setTimeout(2000);
      }
    }
  }

  process.exit(0);
}

main(1000).catch(console.error);
