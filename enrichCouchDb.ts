import pLimit from "p-limit";
import pThrottle, { AnyFunction } from "p-throttle";
import pc from "picocolors";
import * as readline from "readline";
import fs from "fs/promises";
import { setTimeout } from "timers/promises";
import { client } from "./api-client";

const localDbUrl = "https://npm.devminer.xyz/registry";
const auth = "";
const authHeader = "Basic " + Buffer.from(auth).toString("base64");
const checkpointFile = "checkpoint.json";

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
      modified: number;
      deprecated: boolean;
    };

const registries = [
  "https://r.cnpmjs.org/",
  // "https://registry.yarnpkg.com/",
  "https://registry.npmmirror.com/",
  // "https://registry.npmjs.org/",
];
type HeadersCallback = (headers: Headers) => void;
async function fetchWithHeadersCallback(
  url: string,
  options: RequestInit = {},
  headersCallback?: HeadersCallback
): Promise<Response> {
  const controller = new AbortController();
  const signal = controller.signal;

  // Merge provided signal with internal abort signal
  if (options.signal) {
    options.signal.addEventListener("abort", () => controller.abort());
  }
  options.signal = signal;

  // Make the initial fetch request
  const response = await fetch(url, options);

  // If headersCallback is provided, execute it and check for errors
  if (headersCallback) {
    headersCallback(response.headers);
  }

  return response;
}

class TooLargeError extends Error {}

async function fetchPackageDetails(packageName: string, registry: string) {
  let cnt = 0;
  while (true) {
    try {
      const response = await fetchWithHeadersCallback(
        `${registry}${packageName}/latest`,
        {
          signal: AbortSignal.timeout(5000 + cnt * 1000),
        },
        (headers) => {
          // throw error if content-type larger than 5MB
          if (Number(headers.get("content-length")) > 5 * 1024 * 1024) {
            throw new TooLargeError(
              `Content-Type larger than 5MB: ${
                Number(headers.get("content-length")) / 1024 / 1024
              }MB`
            );
          }
        }
      );
      if (!response.ok) {
        if (response.status === 403) {
          console.log(
            `Package ${packageName} access forbidden. Trying other registry.`
          );
          return fetchPackageDetails(
            packageName,
            "https://registry.yarnpkg.com/"
          );
        }

        if (response.status === 404) {
          return {
            name: packageName,
            error: 404,
          };
        }

        if (response.status === 429) {
          console.log(
            "Rate limit exceeded for registry",
            registry,
            ". Waiting 10sec and trying again"
          );
          await setTimeout(10000);
          continue;
        }

        if (response.status === 451) {
          console.log(
            `Package ${packageName} was blocked. Trying other registry.`
          );
          return fetchPackageDetails(
            packageName,
            "https://registry.yarnpkg.com/"
          );
        }

        if (response.status === 422) {
          console.log(
            `Package ${packageName} couldnt be processed. Trying other registry.`
          );
          return fetchPackageDetails(
            packageName,
            "https://registry.yarnpkg.com/"
          );
        }

        console.log(response.statusText);

        throw new Error(
          `Failed to fetch package ${registry}${packageName}/latest. Status code: ${response.status} / ${response.statusText}`
        );
      }
      const data = (await response.json()) as {
        dist: {
          unpackedSize: number;
          fileCount: number;
        };
        version: string;
        dependencies: Record<string, string>;
        devDependencies: Record<string, string>;
        deprecated: string;
      };

      // const [sizeResponse, downloadsResponse] = await Promise.all([
      //   fetch(data.dist.tarball, { method: "HEAD" }),
      //   fetch(`https://api.npmjs.org/downloads/point/last-month/${packageName}`),
      // ]);

      // const size = parseInt(
      //   sizeResponse.headers.get("content-length") || "0",
      //   10
      // );

      // console.log(sizeResponse);

      const unpackedSize = data.dist.unpackedSize || 0;
      const fileCount = data.dist.fileCount || 0;

      return {
        error: null,
        name: packageName,
        latestVersion: data.version,
        dependencies: data.dependencies ?? {},
        devDependencies: data.devDependencies ?? {},
        unpackedSize,
        fileCount,
        deprecated: !!data.deprecated,
      };
    } catch (error) {
      if (error instanceof TooLargeError) {
        console.log(error.message);
        return {
          name: packageName,
          error: error,
        };
      }

      if (error.code === "ABORT_ERR") {
        // readline.clearLine(process.stdout, 0);
        // console.log(`Timeout fetching ${packageName} from ${registry}`);
      } else {
        console.error(
          pc.red(
            `\nError fetching ${packageName} from ${registry}: ${error.message}\n`
          ),
          error
        );
      }
      if (cnt < 10) {
        cnt++;
        // console.log("Retrying...", cnt);
        continue;
      }
      console.log("Retries exceeded. Trying again in 10sec");
      await setTimeout(10000);
      cnt = 0;
      continue;
      process.exit();
      return {
        name: packageName,
        error: error,
      };
    }
  }
}

function progressPromise<T extends Promise<any>, X extends Awaited<T>>(
  promises: T[],
  tickCallback
): Promise<X[]> {
  var len = promises.length;
  var progress = 0;

  function tick(promise) {
    const start = performance.now();
    promise.then(function () {
      progress++;
      tickCallback(
        progress,
        len,
        promise.package,
        promise.registry,
        performance.now() - start
      );
    });
    return promise;
  }

  return Promise.all(promises.map(tick));
}

async function getDownloadStats(packageNames: string[]) {
  // chunk packageNames into 1000 array chunks
  const packageChunks = packageNames
    .map((packageName) => [packageName])
    .reduce(
      (acc, curr) => {
        if (acc[acc.length - 1].length < 1000) {
          acc[acc.length - 1].push(...curr);
        } else {
          acc.push(curr);
        }
        return acc;
      },
      [[]] as string[][]
    );

  const result = await Promise.all(
    packageChunks.map(async (packageNames) => {
      while (true) {
        try {
          const response = await client.getObjects<{
            downloadsLast30Days: number;
            objectID: string;
            modified: number;
          }>({
            requests: packageNames.map((packageName) => {
              return {
                indexName: "npm-search",
                objectID: packageName,
                attributesToRetrieve: ["downloadsLast30Days", "modified"],
              };
            }),
          });

          return response.results.map((result, index) => ({
            name: result?.objectID ?? packageNames[index],
            downloads: result?.downloadsLast30Days ?? null,
            modified: result?.modified ?? null,
          }));
        } catch (error) {
          console.log(error);
          await setTimeout(1000);
        }
      }
    })
  );

  return result.flat();
}

export async function fetchPackagesInfo(
  packageNames: string[],
  rateLimit: number | null = null,
  concurrency: number | null = null
): Promise<PackageDetails[]> {
  const noop: <F extends AnyFunction>(function_: F) => F = (fn) => fn;
  const throttlers = registries.map(() =>
    rateLimit ? pThrottle({ limit: rateLimit, interval: 1000 }) : noop
  );
  const limiters = registries.map(() => pLimit(concurrency || Infinity));

  const fetchTasks = packageNames.flatMap((packageName, index) => {
    const registryIndex = index % registries.length;
    const registry = registries[registryIndex];
    const throttled = throttlers[registryIndex](() =>
      fetchPackageDetails(packageName, registry)
    );
    const promise = limiters[registryIndex](throttled);
    (promise as any).package = packageName;
    (promise as any).registry = registry;
    return promise;
  });

  const [allDownloads, results] = await Promise.all([
    getDownloadStats(packageNames),
    progressPromise(fetchTasks, (progress, len, pck, registry, time) => {
      readline.clearLine(process.stdout, 0);
      readline.cursorTo(process.stdout, 0);
      console.log(
        `${pc.magenta(
          `Fetching ${progress}/${len} packages... (${pck} on ${registry} in ${(
            time / 1000
          ).toFixed(2)}s)`
        )}`.slice(0, process.stdout.columns - 1)
      );
      readline.moveCursor(process.stdout, 0, -1);
    }),
  ]);
  return results.map((result) => {
    const stats = allDownloads.find((d) => d.name === result.name);
    return {
      ...result,
      downloads: stats?.downloads ?? null,
      modified: stats?.modified ?? 0,
    };
  });

  // results.forEach((pkg, index) => {
  //   readline.clearLine(process.stdout, 0);
  //   readline.cursorTo(process.stdout, 0);
  //   if (pkg.error) {
  //     console.log(
  //       `${pc.red(`#${index + 1}`)} ${pc.yellow(pkg.name)} - ${pc.red(
  //         `Error: ${pkg.error}`
  //       )}`
  //     );
  //   } else {
  //     console.log(
  //       `${pc.green(`#${index + 1}`)} ${pc.yellow(pkg.name)} (${pc.magenta(
  //         pkg.latestVersion
  //       )}) - ${pc.cyan(`Downloads: ${pkg.downloads}`)}, ${pc.red(
  //         `Size: ${pkg.unpackedSize} bytes / ${pkg.fileCount} files`
  //       )}`
  //     );
  //   }
  // });
}

type Entry = {
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
    dependencies: object;
    devDependencies: object;
    unpackedSize: number;
    fileCount: number;
  };
};

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

let totalDocs: number | null = null;
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

    let data: {
      rows: Entry[];
      total_rows: number;
      error: string;
      offset: number;
    };
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
        docs: data.rows.filter((d) => !d.id.startsWith("_design")),
        lastKey: lastKey,
      };
    }
  }
}

const hasXDashes = (id: string, x = 5) => {
  if (id.startsWith("@")) return id.split("/")[1].split("-").length >= x;
  return id.split("-").length >= x;
};

let cnt = 0;
let total = 0;
let packagesFetched = 0;
async function main(batchSize = 100) {
  for await (const { docs, lastKey } of fetchDocuments(batchSize)) {
    const overall = performance.now();
    const d = docs
      .filter((d) => !hasXDashes(d.id) && d.doc.downloads > 1000)
      .map((d) => d.id);

    // const filtered = docs
    //   .filter((d) => hasXDashes(d.id) || d.doc.downloads <= 1000)
    //   .map((d) => d.id);

    if (d.length === 0) {
      await saveCheckpoint(lastKey);
      continue;
    }

    const packageinfo = await fetchPackagesInfo(d, 200);

    packagesFetched += packageinfo.length;

    const bulkDocsPayload = {
      docs: packageinfo
        .map((doc) => {
          const _rev = docs.find((d) => d.id === doc.name)?.value?.rev;

          if (!_rev) {
            console.log("Missing rev for", doc.name);
            process.exit();
            return null;
          }

          if (doc.error === 404) {
            return {
              _id: doc.name,
              _rev: _rev,
              _deleted: true,
            };
          }

          if (doc.error) {
            return {
              _id: doc.name,
              _rev: _rev,
              error: doc.error.message,
            };
          }

          return {
            _id: doc.name,
            _rev: _rev,
            version: doc.latestVersion,
            downloads: doc.downloads,
            dependencies: doc.dependencies,
            devDependencies: doc.devDependencies,
            unpackedSize: doc.unpackedSize,
            fileCount: doc.fileCount,
            modified: doc.modified,
            deprecated: doc.deprecated,
          };
        })
        .filter(Boolean),
    };

    // 40MB max
    if (JSON.stringify(bulkDocsPayload).length > 40000000) {
      console.error("Payload too big. Adjust batch size");
      process.exit(0);
    }

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

        const responses = await response.json().catch(async () => {
          console.log(await response.text(), bulkDocsPayload);
          return { ok: false };
        });

        if (!response.ok) {
          console.error("Failed to enrich documents:", await response.text());
          await setTimeout(2000);
        } else if (responses.some((r) => r.error)) {
          console.error(
            "Failed to enrich documents:",
            responses.filter((r) => r.error)[0]
          );
          console.log(
            bulkDocsPayload.docs.find(
              (r) => r!._id === responses.filter((r) => r.error)[0].id
            )
          );
          process.exit();
        } else {
          readline.moveCursor(process.stdout, 0, 1);
          // readline.clearLine(process.stdout, 0);
          console.log(
            `Enriched ${docs.length} documents.`,
            "Last key:",
            pc.red(lastKey)
          );
          // if (filtered.length > 0) {
          //   await fs.appendFile("filtered.txt", filtered.join("\n") + "\n");
          // }
          await saveCheckpoint(lastKey);
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

main(10000).catch(console.error);
