const Apify = require('apify');
const {generateSearchRequests, generateDetailRequests} = require('./generators')
let globals = require('./global')

exports.sGenPool = new Apify.AutoscaledPool({
    maxConcurrency: 50,
    loggingIntervalSecs: null,
    runTaskFunction: async () => {
        let {fromRequest, response, requestQueue, isTestMode, tooMuchResultsDataset} = globals.sGenQueue.pop()
        let promises = generateSearchRequests(fromRequest, response, requestQueue, isTestMode, tooMuchResultsDataset)

        await promises;
    },
    isTaskReadyFunction: async () => {
        return !!globals.sGenQueue.length
    },
    isFinishedFunction: async () => {
        return globals.isFinished
    },
});

exports.dGenPool = new Apify.AutoscaledPool({
    maxConcurrency: 50,
    loggingIntervalSecs: null,
    runTaskFunction: async () => {
        let {fromRequest, response, requestQueue, mappingDataset} = globals.dGenQueue.pop()
        let promises = generateDetailRequests(fromRequest, response, requestQueue)
        if (mappingDataset !== undefined) {
            await Promise.all([
                promises,
                mappingDataset.pushData(response.providers.map(
                    x => Object.assign(x, {network: fromRequest.userData.network}))
                )
            ]);
        } else {
            await promises;
        }
    },
    isTaskReadyFunction: async () => {
        return !!globals.dGenQueue.length
    },
    isFinishedFunction: async () => {
        return globals.isFinished
    },
});


