const fs = require("fs");
const HashMap = require("hashmap");
const Apify = require("apify");
const {
  utils: { log },
} = Apify;

exports.reduceMapping = async (dataset) => {
  const meta = await dataset.getInfo();
  log.info("Reducing the dataset", meta);
  let idx = 1;
  return (
    await dataset.reduce((memo, value) => {
      let providerKey = `${value.providerType}${value.providerId}`,
        mappedProvider = memo.get(providerKey);

      if (mappedProvider === undefined) {
        let compiledValue = Object.assign(
          {},
          {
            rawData: value,
            locations: new HashMap(
              value.locations.map((x) => [
                `${x.id}`,
                {
                  rawData: x,
                  networks: new HashMap([
                    [`${value.network.id}`, value.network],
                  ]),
                },
              ])
            ),
          }
        );
        memo.set(providerKey, compiledValue);
      } else {
        value.locations.forEach((loc) => {
          let locHash = `${loc.id}`,
            mappedLoc = mappedProvider.locations.get(locHash);

          if (mappedLoc === undefined) {
            mappedProvider.locations.set(locHash, {
              rawData: loc,
              networks: new HashMap([[`${value.network.id}`, value.network]]),
            });
          } else {
            let netHash = `${value.network.id}`;
            mappedLoc.networks.set(netHash, value.network);
          }
        });
      }
      if (idx % 1000 === 0) {
        log.info(`${idx} of ${meta.itemCount} records reduced, size=${memo.size}`);
      }
      idx++;

      return memo;
    }, new HashMap())
  ).forEach((value) => {
    let locationsJson = value.locations
      .values()
      .map((el) =>
        Object.assign({}, el.rawData, { networks: el.networks.values() })
      );
    value.asJson = Object.assign({}, value.rawData, {
      locations: locationsJson,
    });
    value.asJson.ribbon_entity_type = (value.asJson.providerType === "P") ? 1 : 2;
    delete value.asJson.network;
  });
};

exports.mapDetail = async (dataset, mapping) => {
  const meta = await dataset.getInfo();
  log.info("Mapping the dataset", meta);
  log.info(`Using reduced mapping of size ${mapping.size}`);
  let idx = 1;

  return await dataset.map((el) => {
    let mappedProvider = mapping.get(el.providerId);
    if (mappedProvider === undefined) {
      log.error(`No mapping found for ${el.providerId}`);
      return undefined;
    } else {
      if (
        el.ribbon_entity_type === 1 &&
        el.locations.length != mappedProvider.asJson.locations.length
      ) {
        log.error(
          `Inconsistency in locations for ${el.providerId}: ${el.locations.length} crawled, ` +
            `${mappedProvider.asJson.locations.length} mapped.`
        );
      } else if (
        el.ribbon_entity_type === 2 &&
        el.location.length != mappedProvider.asJson.locations.length
      ) {
        log.error(
          `Inconsistency in locations for ${el.providerId}: ${el.location.length} crawled, ` +
            `${mappedProvider.asJson.locations.length} mapped.`
        );
      }

      el.specialties = mappedProvider.asJson.specialties;
      for (const mappedLoc of mappedProvider.asJson.locations) {
        let elLoc;
        if (el.ribbon_entity_type === 1) {
          elLoc = el.locations.find((el) => el.id === mappedLoc.id);
        } else {
          elLoc = el.location.find((el) => el.locationId === mappedLoc.id);
        }

        if (elLoc === undefined) {
          log.error(
            `No location found for p=${el.providerId}, l=${mappedLoc.id}`
          );
        } else {
          Object.assign(elLoc, {
            awards: mappedLoc.awards,
            networks: mappedLoc.networks,
          });
        }
      }
    }

    if (idx % 100 === 0) {
      log.info(`${idx} of ${meta.itemCount} records mapped`);
    }
    idx++;

    return el;
  });
};

exports.writeToFile = (finalArray) => {
  log.info("Writing to file");
  fs.writeFileSync("finalDataset.json", JSON.stringify(finalArray, null, 4));
};
