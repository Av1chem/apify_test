const Apify = require("apify");
const {
  utils: {log, requestAsBrowser},
} = Apify;
const {v4: uuidv4} = require("uuid");

exports.generateStartRequests = (zipCodes, networks, apiDistance) => {
  let pairs = shuffleArray(cartesian(zipCodes, networks)),
    reqIdx = -1;

  return pairs.map((pair) => {
    reqIdx++;

    return new Apify.Request({
      url: "https://bcbsma-prod.apigee.net/member/web/v1/vitalscommon/searchbyproviders",
      method: "POST",
      headers: {
        uitxnid: `WEB_v3.0_${uuidv4()}`,
        "Content-Type": "application/json",
      },
      uniqueKey: `start,${pair[0]["lat"]},${pair[0]["lng"]},${pair[1]["network"]["id"]}`,
      userData: {
        type: "START",
        payload: {
          geoLocation: `${pair[0]["lat"]},${pair[0]["lng"]}`,
          limit: 20,
          page: 1,
          radius: apiDistance,
          networkId: pair[1]["network"]["id"],
          searchForTH: false,
          useridin: "undefined",
          fadVendorMemberNumber: null,
        },
        network: pair[1]["network"],
        idx: `${reqIdx}`,
      },
    });
  });
};

exports.generateSearchRequests = async (
  fromRequest,
  response,
  requestQueue,
  isTestMode,
  tooMuchResultsDataset
) => {
  let promises = Array(),
    totalCount = 0;

  if (response.totalCount > 2000) {
    log.warning("Too much results.", {
      idx: fromRequest.userData.idx,
      totalCount: response.totalCount,
    });
    if (tooMuchResultsDataset !== undefined) {
      promises.push(
        tooMuchResultsDataset.pushData({
          payload: fromRequest.userData,
          totalCount: response.totalCount,
        })
      );
    }

    totalCount = 1999;
  } else if (response.totalCount === 2000) {
    totalCount = 1999;
  } else {
    totalCount = response.totalCount;
  }
  let totalPages = Math.floor(totalCount / 20) + 1;
  log.info(
    `idx: ${fromRequest.userData.idx}, totalCount: ${response.totalCount}, totalPages: ${totalPages}`
  );

  for (let i = 1; i <= (isTestMode ? 1 : totalPages); i++) {
    promises.push(
      requestQueue.addRequest(
        new Apify.Request({
          url: "https://bcbsma-prod.apigee.net/member/web/v1/vitalscommon/searchbyproviders",
          method: "POST",
          headers: {
            uitxnid: `WEB_v3.0_${uuidv4()}`,
            "Content-Type": "application/json",
          },
          uniqueKey: fromRequest.uniqueKey + `,${i}`,
          userData: {
            type: "SEARCH",
            payload: {
              geoLocation: fromRequest.userData.payload.geoLocation,
              limit: fromRequest.userData.payload.limit,
              page: i,
              radius: fromRequest.userData.payload.radius,
              networkId: fromRequest.userData.payload.networkId,
              searchForTH: fromRequest.userData.payload.searchForTH,
              useridin: fromRequest.userData.payload.useridin,
              fadVendorMemberNumber:
              fromRequest.userData.payload.fadVendorMemberNumber,
            },
            network: fromRequest.userData.network,
            idx: fromRequest.userData.idx + `-${i}`,
          },
        })
      )
    );
  }

  return promises;
};

exports.generateDetailRequests = async (
  fromRequest,
  response,
  requestQueue
) => {
  let promises = Array();

  for (const p of response.providers) {
    if (p.providerType === "P") {
      promises.push(
        requestQueue.addRequest(
          new Apify.Request({
            url: "https://bcbsma-prod.apigee.net/member/web/v1/vitalscommon/professionalprofile",
            method: "POST",
            headers: {
              uitxnid: `WEB_v3.0_${uuidv4()}`,
              "Content-Type": "application/json",
            },
            uniqueKey: `P${p.providerId}`,
            userData: {
              type: "DETAIL",
              p_type: "P",
              payload: {
                professionalId: p.providerId,
                networkId: fromRequest.userData.payload.networkId,
              },
              network: fromRequest.userData.network,
              idx: fromRequest.userData.idx + `-P${p.providerId}`,
            },
          })
        )
      );
    } else if (p.providerType === "F") {
      promises.push(
        requestQueue.addRequest(
          new Apify.Request({
            url: "https://bcbsma-prod.apigee.net/member/web/v1/vitalscommon/facilityprofile",
            method: "POST",
            headers: {
              uitxnid: `WEB_v3.0_${uuidv4()}`,
              "Content-Type": "application/json",
            },
            uniqueKey: `F${p.providerId}`,
            userData: {
              type: "DETAIL",
              p_type: "F",
              payload: {
                facilityId: p.providerId,
                networkId: fromRequest.userData.payload.networkId,
              },
              network: fromRequest.userData.network,
              idx: fromRequest.userData.idx + `-F${p.providerId}`,
            },
          })
        )
      );
    } else {
      log.error(`Unexpected data. ${JSON.stringify(p)}`);
    }
  }

  return promises;
};

const cartesian = (...a) =>
  a.reduce((a, b) => a.flatMap((d) => b.map((e) => [d, e].flat())));

const shuffleArray = (a) => {
  let j, x, i;
  for (i = a.length - 1; i > 0; i--) {
    j = Math.floor(Math.random() * (i + 1));
    x = a[i];
    a[i] = a[j];
    a[j] = x;
  }

  return a;
};
