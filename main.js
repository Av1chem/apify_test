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
    builtinRequestHandler,
    dontStoreTheData,
    proxy,
    pUrl
  } = await Apify.getInput();

  const proxyConfiguration = await Apify.createProxyConfiguration({
    proxyUrls: [
      pUrl,
    ]
  });

  if (isTestMode) {
    log.info("\n\nRunning in test mode.\n\n");
    zipCodes = zipCodes.slice(0, 2);
  }

  let apiTokens = await getTokens();
  let gTkns = setInterval(async () => {
    apiTokens = await getTokens();
  }, 1800000);


  const startRequests = new Apify.RequestList({
    sourcesFunction: () =>
      generateStartRequests(zipCodes, networks, apiDistance),
    persistStateKey: "startListState",
    persistRequestsKey: "startRequestsState",
  });
  await startRequests.initialize();
  const requestQueue = await Apify.openRequestQueue("otherRequests");

  const mappingDataset = dontStoreTheData ? undefined : await Apify.openDataset("mapping-dataset");
  const detailDataset = dontStoreTheData ? undefined : await Apify.openDataset("detail-dataset");
  const failedRequestsDataset = dontStoreTheData ? undefined : await Apify.openDataset("failed-requests");
  const tooMuchResultsDataset = dontStoreTheData ? undefined : await Apify.openDataset("too-much-results");
  const finalDataset = dontStoreTheData ? undefined : await Apify.openDataset("final");

  const crawler = new Apify.CheerioCrawler({
    requestList: startRequests,
    requestQueue,
    maxConcurrency,
    maxRequestRetries,
    handleRequestTimeoutSecs,
    proxyConfiguration,
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
    handlePageFunction: async (context) => {
      const {
        userData: {type, idx, payload},
      } = context.request;
      log.info("Handling request.", {type, idx, payload});
      switch (type) {
        case "SEARCH":
          return handleList(context, requestQueue, mappingDataset, apiTokens, builtinRequestHandler, proxy);
        case "DETAIL":
          return handleDetail(context, requestQueue, detailDataset, apiTokens, builtinRequestHandler, proxy);
        default:
          return handleStart(
            context,
            requestQueue,
            apiTokens,
            false,
            tooMuchResultsDataset,
            builtinRequestHandler,
            proxy
          );
      }
    },
    handleFailedRequestFunction: async (context) => {
      if (!dontStoreTheData) {
        await failedRequestsDataset.pushData({
          payload: context.request.userData,
          error: context.error,
        });
      }
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
  clearInterval(gTkns);
  log.info("Crawl finished.\n\n\n");
  globals.isFinished = true;
  await spFinished;
  await dpFinished;

  if (!dontStoreTheData) {
    let mappingReduced = await reduceMapping(mappingDataset),
      finalArray = await mapDetail(detailDataset, mappingReduced);

    await finalDataset.pushData(finalArray);
  }
});
