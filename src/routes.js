const Apify = require("apify");
const {
  utils: {log},
} = Apify;

const {cryptoRequest, cryptoRequestOld} = require("./crypto");
const {
  generateSearchRequests,
  generateDetailRequests,
} = require("./generators");

let globals = require("./global");

exports.handleStart = async (
  context,
  requestQueue,
  apiTokens,
  isTestMode,
  tooMuchResultsDataset,
  builtinRequestHandler,
  proxy
) => {
  // Handle search
  let resp = await (builtinRequestHandler ? cryptoRequestOld: cryptoRequest)(context.request, apiTokens, proxy);

  globals.sGenQueue.push({
    fromRequest: context.request,
    response: resp,
    requestQueue,
    isTestMode,
    tooMuchResultsDataset
  });
};

exports.handleList = async (
  context,
  requestQueue,
  mappingDataset,
  apiTokens,
  builtinRequestHandler,
  proxy
) => {
  // Handle pagination
  let resp = await (builtinRequestHandler ? cryptoRequestOld: cryptoRequest)(context.request, apiTokens, proxy);

  globals.dGenQueue.push({
    fromRequest: context.request,
    response: resp,
    requestQueue,
    mappingDataset,
  });
};

exports.handleDetail = async (
  context,
  requestQueue,
  detailDataset,
  apiTokens,
  builtinRequestHandler,
  proxy
) => {
  // Handle details
  let resp = await (builtinRequestHandler ? cryptoRequestOld: cryptoRequest)(context.request, apiTokens, proxy);
  if (detailDataset === undefined) {
    return
  }
  if (context.request.userData.p_type === "P") {
    await detailDataset.pushData(
      Object.assign({}, resp, {
        ribbon_entity_type: 1,
        providerId: `P${context.request.userData.payload.professionalId}`,
      })
    );
  } else {
    await detailDataset.pushData(
      Object.assign({}, resp.facility, {
        ribbon_entity_type: 2,
        providerId: "F" + resp.facility.providerId.slice(1),
      })
    );
  }
};
