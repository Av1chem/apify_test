const Apify = require("apify");
const {handleStart, handleList, handleDetail} = require("./src/routes");
const {generateStartRequests} = require("./src/generators");
const {getTokens} = require("./src/crypto");
const {sGenPool, dGenPool} = require("./src/pools");
const {
  reduceMapping,
  mapDetail,
  writeToFile,
} = require("./src/dataset-operators");
const {
  utils: {log},
} = Apify;
let globals = require("./src/global");

Apify.main(async () => {
  let {
    zipCodes,
    networks,
    apiDistance,
    isTestMode,
    maxConcurrency,
    maxRequestRetries,
    handleRequestTimeoutSecs,
    maxBlockedMillis,
    maxUsedCpuRatio,
    maxEventLoopOverloadedRatio,
    builtinRequestHandler
  } = await Apify.getInput();

  if (isTestMode) {
    log.info("\n\nRunning in test mode.\n\n");
    zipCodes = zipCodes.slice(0, 2);
  }

  const apiTokens = await getTokens();

  const startRequests = new Apify.RequestList({
    sourcesFunction: () =>
      generateStartRequests(zipCodes, networks, apiDistance),
    persistStateKey: "startListState",
    persistRequestsKey: "startRequestsState",
  });
  await startRequests.initialize();
  const requestQueue = await Apify.openRequestQueue("otherRequests");

  const mappingDataset = await Apify.openDataset("mapping-dataset");
  const detailDataset = await Apify.openDataset("detail-dataset");
  const failedRequestsDataset = await Apify.openDataset("failed-requests");
  const tooMuchResultsDataset = await Apify.openDataset("too-much-results");
  const finalDataset = await Apify.openDataset("final");

  const crawler = new Apify.BasicCrawler({
    requestList: startRequests,
    requestQueue,
    maxConcurrency,
    maxRequestRetries,
    handleRequestTimeoutSecs,
    autoscaledPoolOptions: {
      snapshotterOptions: {
        maxBlockedMillis,
        maxUsedCpuRatio,
        memorySnapshotIntervalSecs: 5,
      },
      systemStatusOptions: {
        maxEventLoopOverloadedRatio,
      },
    },
    handleRequestFunction: async (context) => {
      const {
        userData: {type, idx, payload},
      } = context.request;
      log.info("Handling request.", {type, idx, payload});
      switch (type) {
        case "SEARCH":
          return handleList(context, requestQueue, mappingDataset, apiTokens, builtinRequestHandler);
        case "DETAIL":
          return handleDetail(context, requestQueue, detailDataset, apiTokens, builtinRequestHandler);
        default:
          return handleStart(
            context,
            requestQueue,
            apiTokens,
            false,
            tooMuchResultsDataset,
            builtinRequestHandler
          );
      }
    },
    handleFailedRequestFunction: async (context) => {
      await failedRequestsDataset.pushData({
        payload: context.request.userData,
        error: context.error,
      });
    },
  });

  log.info("Starting the crawl.");
  let qStats = setInterval(async () => {
    log.info(`QUEUE STATS: ${JSON.stringify(await requestQueue.getInfo())}`);
  }, 60000);

  let spFinished = sGenPool.run(),
    dpFinished = dGenPool.run();
  await crawler.run();
  clearInterval(qStats);
  log.info("Crawl finished.\n\n\n");
  globals.isFinished = true;
  await spFinished;
  await dpFinished;

  let mappingReduced = await reduceMapping(mappingDataset),
    finalArray = await mapDetail(detailDataset, mappingReduced);

  await finalDataset.pushData(finalArray);
});
