const {
  utils: { log, requestAsBrowser },
} = require("apify");
const CryptoJS = require("crypto-js");
const unirest = require("unirest");

exports.getTokens = async () => {
  let tokens = (
    await requestAsBrowser({
      url: "https://mobilemember.bluecrossma.com/dglwebapi1/mobilekeyservice/v1/gettokens",
      json: true,
    })
  ).body;
  log.info({ tokens });

  return tokens;
};

exports.cryptoRequest = async (request, tokens) => {
  // newest version
  let encryptedResponse = {},
    plainPayload = Object.assign({}, request.userData.payload, {
      key2id: tokens.key2id,
    }),
    encryptedPayload = getEncrypted(plainPayload, tokens);
  while (!encryptedResponse.hasOwnProperty("message")) {
    encryptedResponse = await unirest(request.method, request.url)
      .timeout(180000)
      .headers(request.headers)
      .send(encryptedPayload)
      .then((res) => {
        return res.body;
      });
  }
  return getDecrypted(encryptedResponse.message, tokens);
};

exports.cryptoRequestOld = async (request, tokens) => {
  // old version
  let plainPayload = Object.assign({}, request.userData.payload, {
      key2id: tokens.key2id,
    }),
    encryptedPayload = getEncrypted(plainPayload, tokens),
    finalRequest = Object.assign({}, request, {
      payload: encryptedPayload,
      json: true,
    }),
    rawResponse = await requestAsBrowser(finalRequest),
    encryptedResponse = rawResponse.body;

  return getDecrypted(encryptedResponse.message, tokens);
};

const getEncrypted = (payload, tokens) => {
  let r = CryptoJS.PBKDF2(
      tokens.key1phrase,
      CryptoJS.enc.Hex.parse(tokens.key1salt),
      {
        keySize: 4,
        iterations: 1e4,
      }
    ),
    a = CryptoJS.AES.encrypt(JSON.stringify(payload), r, {
      iv: CryptoJS.enc.Hex.parse(tokens.key1iv),
    }).ciphertext.toString(CryptoJS.enc.Base64);

  return JSON.stringify({
    message: a,
    key1id: tokens.key1id,
  });
};

const getDecrypted = (message, tokens) => {
  let n = CryptoJS.PBKDF2(
      tokens.key2phrase,
      CryptoJS.enc.Hex.parse(tokens.key2salt),
      {
        keySize: 4,
        iterations: 1e4,
      }
    ),
    i = CryptoJS.AES.decrypt(message, n, {
      iv: CryptoJS.enc.Hex.parse(tokens.key2iv),
    }).toString(CryptoJS.enc.Utf8);

  return JSON.parse(i);
};
