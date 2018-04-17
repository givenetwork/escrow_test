require('now-env');

var _ = require('lodash');
const request = require('request')
const express = require('express');
const app = express();
const bodyParser = require("body-parser");
const path = require('path');
const StellarSdk = require('stellar-sdk');
const fs = require('fs');
const glob = require('glob-fs')({ builtins: false });

const stellarServer = new StellarSdk.Server(process.env.STELLAR_SERVER_URL);
const stellarNetworkDomain = process.env.STELLAR_DOMAIN;
const stellarNetworkName = process.env.STELLAR_NETWORK_NAME;
const stellarNetworkSeed = process.env.STELLAR_NETWORK_SEED;
StellarSdk.Network.use(new StellarSdk.Network(stellarNetworkSeed));

// const posix = require('posix');
// try {
//   posix.setrlimit('nofile', { soft: 10000 });
// }
// catch(err) {
//   console.log(err);
// }

var msKeepAlive = 0;
if ('KEEPALIVE_SECS' in process.env) {
  msKeepAlive = process.env.KEEPALIVE_SECS * 1000;
}
var doQuit = false;
var timerKeepAlive;

function keepAlive() {
  if (msKeepAlive == 0) { return; }

  clearTimeout(timerKeepAlive);
  timerKeepAlive = setTimeout(function() { process.exit(); }, msKeepAlive);
}

var eventsDir = '/event_queue/';
var responsesDir = '/responses/';
var dataDir = 'tracking_data/'

const dateOfBirth = new Date();
app.use('/health', (req, res) => res.status(200).json({ agent_id: agent_account_id, version: 1.0, dateOfBirth, pwd: process.env.PWD }));
app.use(bodyParser.urlencoded({ extended: false }));
app.use(bodyParser.json());


var agent_account_id = "no signing key provided";
var agent_asset_code = 'STELLARWATCH';

if ('AGENT_SIGNING_KEY' in process.env) {
  var kp = StellarSdk.Keypair.fromSecret(process.env.AGENT_SIGNING_KEY);
  agent_account_id = kp.publicKey();
}

//
// Local Cache
//
var accountListeners = []
const accounts = [
  agent_account_id
]

//
// Server-Listener (keeps the listeners active)
//
app.listen(process.env.API_PORT || 3000, function(){
	console.log("Connected & Listen to port 3000 at /api ..");
  main();
});

app.get('/accounts/add',function(req,res) {
  var accountId = req.query.account_id;
  startAccountListener(accountId);
  res.json({ success: 1, status: "Account added " + accountId });
  keepAlive();
});

app.get('/accounts/remove',function(req,res) {
  var accountId = req.query.account_id;
  removeAccountListener(accountId);
  res.json({ success: 1, status: "Account removed " + accountId });
  keepAlive();
});

app.get('/accounts',function(req,res) {
  res.json(_.keys(accountListeners));
  keepAlive();
});

async function main() {
  var agentAccount = await writeAccount(agent_account_id);
  startAgentListener(agent_account_id);
  startListeners();
  //setTimeout(readEvents, 5000);
  keepAlive();
}

//
// Getters and Helpers
//

function startListeners() {
  _.forEach(getActiveClients(), function(a) {
        startAccountListener(a);
  });
}

function getAccountDirectory(acctId = null) {
  //var acctDir = 'data/' + a.split(/(....)/).filter(Boolean).join('/');
  var baseDir = dataDir;
  if (acctId) { return baseDir + acctId + '/'; }
  return baseDir;
}

function getAccountFilename(acctId) {
  return getAccountDirectory(acctId) + '.data.json';
  //return getAccountDirectory(acctId) + acctId + '.json';
}

function getEventResponsesDirectory(acctId, hookId, eventId = null) {
  var baseDir = getAccountDirectory(acctId) + hookId + responsesDir;
  if (eventId) { return baseDir + eventId + '/'; }
  return baseDir;
}

function getEventFilename(acctId, hookId, eventId) {
  eventDir = getEventResponsesDirectory(acctId, hookId, eventId);
  return eventDir + '.data.json';
}

function getResponseFilename(acctId, hookId, responseId) {
  var eventId = responseId.split("_")[0]
  return getEventResponsesDirectory(acctId, hookId, eventId) + responseId;
}

function getAccountQueueDirectory(acctId, hookId) {
  return getAccountDirectory(acctId) + hookId + eventsDir;
}

function getAccountQueuedEventIDs(acctId, hookId) {
  var evtDir = getAccountQueueDirectory(acctId, hookId);
  if (fsExistsSync(evtDir)) {
    var files = fs.readdirSync(evtDir);
    return files;
  } else {
    console.log("Event Queue not found: " + evtDir);
    return [];
  }
}

function getAccountEventIDs(acctId, hookId) {
  var acctEventsDir = getEventResponsesDirectory(acctId, hookId);
  if (fsExistsSync(acctEventsDir)) {
    var eventids = fs.readdirSync(acctEventsDir);
    return eventids;
  } else {
    return [];
  }
}

function getEvent(acctId, hookId, msgId) {
  var msgFile = getEventFilename(acctId, hookId, msgId);
  console.log("Loading Event File: ", msgFile);
  if (fsExistsSync(msgFile)) {
    return JSON.parse(fs.readFileSync(msgFile, 'utf8'));
  } else {
    return null;
  }
}

function getEventResponseIds(acctId, hookId, msgId, matchstr) {
  var respDir = getEventResponsesDirectory(acctId, hookId, eventId);

  // glob is merging with past results; reset this way due to weak JS-Fu
  var glob = require('glob-fs')({ builtins: false });

  matchstr = respDir + msgId + '_' + matchstr;
  var respFiles = glob.readdirSync(matchstr);

  respFiles.forEach(function(part, index, arr) {
    arr[index] = part.substr(part.lastIndexOf("/")+1);
  });
  return respFiles;
}

function getResponse(acctId, hookId, responseId) {
  var respFile = getResponseFilename(acctId, hookId, responseId);
  if (fsExistsSync(respFile)) {
    return JSON.parse(fs.readFileSync(respFile, 'utf8'));
  } else {
    return null;
  }
}

function writeResponse(acctId, hookId, resp) {
  var respDir = getEventResponsesDirectory(acctId, hookId, resp.id);

  //console.log("Making Response Dir: " + respDir);
  mkdirpSync(respDir);
  var json = JSON.stringify(resp, null, 2);
  var respFile = respDir + resp.event_id;
  //console.log("Writing Response Data: " + '[not shown at this time]');
  //console.log(json);
  fs.writeFileSync(respFile, json, 'utf8');
}

function fsExistsSync(filePath) {
  try {
    fs.accessSync(filePath);
    return true;
  } catch (e) {
    return false;
  }
}

const acct_fields = {

  // Operations
  'operations': ['source_account'],

  // All Operations
  'operations:create_account': ['funder','account'],
  'operations:payment': ['from','to','asset_issuer'],
  'operations:path_payment': ['from','to','asset_issuer'],
  'operations:manage_offer': ['buy_asset_issuer','sell_asset_issuer'],
  'operations:create_passive_offer': ['buy_asset_issuer','sell_asset_issuer'],
  'operations:set_options': ['buy_asset_issuer','sell_asset_issuer'],
  'operations:change_trust': ['asset_issuer'],
  'operations:allow_trust': ['asset_issuer'],
  'operations:merge_account': ['destination'],
  'operations:manage_data': ['source_account'],

  // Payments
  'payments': ['source_account'],
  'payments:create_account': ['funder','account'],  // 0
  'payments:payment': ['from','to','asset_issuer'], // 1


  // Effects
  'effects': ['source_account'],

  // Account Effects
  'effects:account_created': ['account'],     // 0
  'effects:account_merged': ['account'],      // 1
  'effects:account_credited': ['account'],    // 2
  'effects:account_debited': ['account'],     // 3
  'effects:thresholds_updated': ['account'],  // 4
  'effects:homedomain_updated': ['account'],  // 5
  'effects:flags_updated': ['account'],       // 6

  // Signer Effects
  'effects:signer_created': ['account'],  // 10
  'effects:signer_removed': ['account'],  // 11
  'effects:signer_updated': ['account'],  // 12

  //Trustline Effects
  'effects:trustline_created': ['account','asset_issuer'],      // 20
  'effects:trustline_removed': ['account','asset_issuer'],      // 21
  'effects:trustline_updated': ['account','asset_issuer'],      // 22
  'effects:trustline_authorized': ['account','asset_issuer'],   // 23
  'effects:trustline_deauthorized': ['account','asset_issuer'], // 24

  // Trading Effects
  'effects:offer_created': ['account','seller','sold_asset_issuer','bought_asset_issuer'], // 30
  'effects:offer_removed': ['account','seller','sold_asset_issuer','bought_asset_issuer'], // 31
  'effects:offer_updated': ['account','seller','sold_asset_issuer','bought_asset_issuer'], // 32
  'effects:trade': ['account','seller','sold_asset_issuer','bought_asset_issuer'],         // 33

  // Data Effects
  'effects:data_created': ['account'], // 40
  'effects:data_removed': ['account'], // 41
  'effects:data_updated': ['account'], // 42

  // Transactions
  'transactions': ['source_account']
}

//String.prototype.hashCode = function() {
function hashCode(str) {
  var hash = 0, i, chr;
  //if (this.length === 0) return hash;
  //for (i = 0; i < this.length; i++) {
    //chr   = this.charCodeAt(i);
  if (str.length === 0) return hash;
  for (i = 0; i < str.length; i++) {
    chr   = str.charCodeAt(i);
    hash  = ((hash << 5) - hash) + chr;
    hash |= 0; // Convert to 32bit integer
  }
  return (hash >>> 0).toString(16);
};

// For the managed data URLs
async function writeAccount(acctId) {

  //console.log("Loading Account: " + acctId);
  try {
    var acct = await stellarServer.loadAccount(acctId);

    var acctDir = getAccountDirectory(acctId);
    //console.log("Making Dir: " + acctDir);
    mkdirpSync(acctDir);
    var acctFile = getAccountFilename(acctId);
    json = JSON.stringify(acct, null, 2);
    //console.log("Writing Account Data: " + json);
    fs.writeFileSync(acctFile, json, 'utf8');


    // Clean up the CursorData
    var eventhooks = getAccountURLs(acctId);
    for (topic in eventhooks) {
      for (hookId in eventhooks[topic]) {
        //console.log("Processing Event Hook: ", topic, " / ", hookId);
        var cursorData = getCursorData(acctId, topic, hookId);
        //console.log("Got cursorData: ", cursorData);
        if (!('url' in cursorData)) {
          cursorData['url'] = eventhooks[topic][hookId];
          updateCursorData(acctId, topic, hookId, cursorData);
        }
        if (!('active' in cursorData)) {
          cursorData = await activatePostURL(cursorData, acctId, topic, hookId, true);
          //console.log("CursorData received: ", cursorData);
          updateCursorData(acctId, topic, hookId, cursorData);
        }
      }
    }

    return acct;

  } catch(e) {
    console.error(e);
    return null;
  }
}

function mkdirpSync(dir) {
  if (fsExistsSync(dir)) {
    return;
  } else {
    mkdirpSync(path.dirname(dir));
    fs.mkdirSync(dir);
  }
}

function getCursorDirectory(acctId, topic, hookId) {
  var acctDir = getAccountDirectory(acctId);
  var cursorDir = acctDir + hookId + '/cursors/' + topic + '/';
  //console.log("CursorDir: ", cursorDir);
  mkdirpSync(cursorDir);
  return cursorDir;
}

function getCursorFilename(acctId, topic, hookId) {
  var cursorDir = getCursorDirectory(acctId, topic, hookId);
  var cursorFile = cursorDir + '.data.json';
  //console.log("CursorFile: ", cursorFile);
  return cursorFile;
}

// For the managed data URLs
function updateCursorData(acctId, topic, hookId, cursorData) {
  //console.log("Updating Cursor for: ", topic, " / ", hookId, " / ",  acctId);

  try {
    var cursorFile = getCursorFilename(acctId, topic, hookId);
    //console.log("Got Cursor Filename: ", cursorFile);

    var json = JSON.stringify(cursorData, null, 2);
    //console.log("Writing Cursor Data: " + json);
    fs.writeFileSync(cursorFile, json, 'utf8');

    return cursorData;

  } catch(e) {
    console.log("Error Updating Cursor Data: ");
    console.error(e);
    return null;
  }
}

function getCursorData(acctId, topic, hookId) {
  var cursorFile = getCursorFilename(acctId, topic, hookId);
  if (fsExistsSync(cursorFile)) {
    cursorData = JSON.parse(fs.readFileSync(cursorFile, 'utf8'));
    return cursorData;
  }
  return {};
}

function getActiveClients() {
  var activeAccounts = [];
  var agentData = getAccount(agent_account_id);
  var balances = agentData["balances"];
  for (tlIdx in balances) {
    tLine = balances[tlIdx];

    if (tLine['asset_type'] == 'native'
          || tLine['asset_code'] != agent_asset_code)
    {
      continue;
    }
    //console.log("Adding Active Client: ", tLine);

    activeAccounts.push(tLine['asset_issuer']);
  }
  return activeAccounts;
}

function getHorizonServerCall(topic) {
  var serverCall = null;
  if (topic == 'transactions') {
    serverCall = stellarServer.transactions();
  } else if (topic == 'operations') {
    serverCall = stellarServer.operations();
  } else if (topic == 'payments') {
    serverCall = stellarServer.payments();
  } else if (topic == 'effects') {
    serverCall = stellarServer.effects();
  }
  return serverCall;
}

function getGraphQLServerCall(topic) {
  var serverCall = null;
  if (topic == 'asset_operations') {
    serverCall = stellarServer.effects();
  }
  return serverCall;
}

function getServerCall(topic) {
  return getHorizonServerCall(topic);
}

var post_mark = new Date();
async function processEvent(acctId, topic, hookId) {
  var now = new Date();
  //console.log("-------------------------------- Delay of: ", now - post_mark);
  post_mark = now;
  var cursorData = getCursorData(acctId, topic, hookId);
  if (!cursorData['active']) { return; }

  var postURL = cursorData['url'];
  var paging_token = cursorData['paging_token'];

  var paging_token_save = paging_token;
  //console.error("Testing for Topic: ", topic, "; HookId: ", hookId, "; Paging_token: ", paging_token);
  if (topic.startsWith('asset_')) {
    // Use GraphQL Endpoints
    try {
      var graphql = null;
      var req = null;
      var evt = null;
      if (topic.endsWith('_operations')) {
        evt = await getAssetOperations(acctId, paging_token);
        //console.log("Results: ", JSON.stringify(evt, null, 2));
      }
      if (evt) {
        var resp = await postEvent(acctId, postURL, topic, evt);
        //console.log("response: ", JSON.stringify(resp, null, 2));
        if (resp.statusCode == 200) {
          cursorData['paging_token'] = evt['paging_token'];
        }
      }

    } catch(err) {
      console.error("An error happened while retrieving the event.");
      if (typeof(err) == 'object') {
        console.error(JSON.stringify(err, null, 2));
      } else {
        console.error(JSON.stringify(err, null, 2));
      }
    }
  } else {
    // Use Horizon Endpoints
    var serverCall = getServerCall(topic);

    try {

      if (!paging_token) {
        //console.log("No Paging Token: ", topic, "/", hookId);
        //console.log("Trying to refresh Horizon Paging_Token.");
        var r = await serverCall
          .forAccount(acctId).limit(1).order('desc').cursor('now').call();
        //console.log("Returned.");

        //console.log("Results: ", JSON.stringify(r, null, 2));
        if (r.records.length > 0) {
          var evt = r.records[0];
          paging_token = evt['paging_token'];
          cursorData['paging_token'] = paging_token;
        }
        serverCall = getServerCall(topic);
      }

      var r = await serverCall
        .forAccount(acctId).limit(1).order('asc').cursor(paging_token).call();

      //console.log("Returned.");
      if (r.records.length > 0) {
        var evt = r.records[0];
        //console.log(JSON.stringify(evt, null, 2));

        var resp = await postEvent(acctId, postURL, topic, evt);
        if (resp.statusCode == 200) {
          cursorData['paging_token'] = evt['paging_token'];
        }
      }
    } catch(err) {
      console.error("An error happened while retrieving the event.");
      if (typeof(err) == 'object') {
        console.error(JSON.stringify(err, null, 2));
      } else {
        console.error(JSON.stringify(err, null, 2));
      }
    }
  }
  if (cursorData['paging_token'] != paging_token_save) {
    updateCursorData(acctId, topic, hookId, cursorData);
  }
}

async function readEventCursors(acctId) {
  //console.log("Reading events for account: " + acctId);
  if (!acctId in getActiveClients()) {
    console.log("Account deactivated; stopping event Reader: " + acctId);
    return;
  }
  var acctData = await writeAccount(acctId);
  var credit = getClientCredit(acctId);
  //console.log("Account Credit is: ", credit);

  var eventhooks = getAccountURLs(acctId);
  postDelay = 10000 * eventhooks.length;
  if (credit['limit']) {
    postDelay = parseInt(1000 / parseFloat(credit['limit']));
  }
  topicHooks = [];
  for (topic in eventhooks) {
    for (hookId in eventhooks[topic]) {
      topicHooks.push([topic, hookId]);
    }
  }
  ysm.array.forEachTime(topicHooks,
    async function (topicHook, index, arr) {
      await processEvent(acctId, topicHook[0], topicHook[1]);
    },
    postDelay, // Wait for X time to honor the account's "postRate"
    function() {
      if (acctId in accountListeners) {
        accountListeners[acctId]['timeout'] = setTimeout(function() { readEventCursors(acctId) }, postDelay);
      }
    }
  );
}

function getAccount(acct) {
  var acctData = null;
  var acctFile = getAccountFilename(acct);
  //console.log("Loading Account File: " + acctFile);
  if (fsExistsSync(acctFile)) {
    // Load the account data
    acctData = JSON.parse(fs.readFileSync(acctFile, 'utf8'));
  }
  return acctData;
}

function getClientCredit(clientId) {
  var agentData = getAccount(agent_account_id);
  var balances = agentData.balances;
  for (tIdx in balances) {
    var tLine = balances[tIdx];
    if (tLine['asset_code'] == agent_asset_code
          && tLine['asset_issuer'] == clientId)
    {
      tLine['limit'] = parseFloat(tLine['limit']);
      return tLine;
    }
  }
  return {
    'limit': 0,
    'balance': '0',
    'asset_code': agent_asset_code,
    'asset_issuer': clientId
  };
}

async function updateClientCredits() {
  var txnFee = 0.0000100; // should be txn fee from SDK (ledger lookup)
  var baseReserve = 0.5;  // should be reserve from SDK (ledger lookup)
  const phi = 1.618033988749895

  var kp = StellarSdk.Keypair.fromSecret(process.env.AGENT_SIGNING_KEY);
  var agentAccount = await writeAccount(kp.publicKey());
  var txn = new StellarSdk.TransactionBuilder(agentAccount);

  var agentData = getAccount(agent_account_id);
  var balances = agentData.balances;
  for (tIdx in balances) {
    var tLine = balances[tIdx];
    if (tLine['asset_type'] == 'native' ||
        tLine['asset_code'] != agent_asset_code) {
      continue;
    }

    tLine['limit'] = (parseFloat(tLine['limit']) / phi).toFixed(7);
    //console.log('Updating trustline: ', JSON.stringify(tLine, null, 2));

    var clientAsset = new StellarSdk.Asset(tLine['asset_code'], tLine['asset_issuer']);

    txn = txn.addOperation(StellarSdk.Operation.changeTrust({
          'asset': clientAsset,
          'limit': tLine['limit']
      }))

    if ((tIdx > 0 && ((tIdx % 20) == 0) ) || (tIdx == (balances.length - 1)) ) {
      txn.build();
      txn.sign(kp);
      try {
        var txnResult = await stellarServer.submitTransaction(txn);

        //console.log('Client credits updated: ', JSON.stringify(txnResult, null, 2));
      } catch(err) {
        console.log('An error has occured:');
        if (typeof(err) == 'object') {
          console.log(JSON.stringify(err, null, 2));
        } else {
          console.log(err);
        }
      }
      // Reset the txn and start accumulating more operations
      txn = new StellarSdk.TransactionBuilder(agentAccount);
    }
  }
}

function getAccountURLs(acct) {
  var eventhooks = {};

  // Load the account data
  var acctData = getAccount(acct);
  if (acctData) {
    // Fetch the eventhook URLs from the account's data_attr
    Object.keys(acctData['data_attr']).forEach(function(key) {
      //console.log("Testing key: " + key);

      // Use the agent_asset_code as the service identifier
      var agentDataPrefix = agent_asset_code.toLowerCase() + ':';
      if (key.toLowerCase().startsWith(agentDataPrefix)) {
        var postURL = decodeData(acctData['data_attr'][key], 'base64');
        // if the URL is not empty
        if (postURL) {
          var hookId = hashCode(postURL);
          var topic = key.toLowerCase().split(':')[1];

          // Test that topic requested is a valid topic
          if (getServerCall(topic) || topic.startsWith('asset_')) {
            // Add the topic dictionary if it's not already there
            if (!(topic in eventhooks)) { eventhooks[topic] = {}; }
            // Lastly, set the URL for the topicHook
            eventhooks[topic][hookId] = postURL;
          }
        }
      }
    });
  }

  // If any topicHooks were added, return them else return null
  if (Object.keys(eventhooks).length > 0) {
    return eventhooks;
  } else {
    return null;
  }
}

// Should we do something like this? or an object with functions keyed by type?
//account.on.transactions(function(message) { /* Post transaction event */ });
//account.on.operations(function(message) { /* Post operation event */ });
//account.on.effects(function(message) { /* Post effect event */ });
//account.on.payments(function(message) { /* Post payment event */ });

function postEvent(account, postURL, postTopic, message) {
  var hookId = hashCode(postURL);
  console.log("Posting to eventhook [", hookId, "/", postTopic, "]: ", postURL); 
  if (!('type' in message)) {
    message['type'] = '';
  }

  var postJSON = {
    'topic': postTopic,
    'type': message.type,
    'id': message.id,
    'extras': message.extras,
    'network': {
      'domain': stellarNetworkDomain,
      'name': stellarNetworkName,
      'seed': stellarNetworkSeed,
      'id': StellarSdk.Keypair.master().publicKey()
    }
  };
  if (!('extras' in message)) {
    postJSON['extras'] = message.extras;
  }
  postBody = JSON.stringify(postJSON);

  var public_key = "no signing key provided";
  var signature = "no signing key provided";
  if ('AGENT_SIGNING_KEY' in process.env) {
    var kp = StellarSdk.Keypair.fromSecret(process.env.AGENT_SIGNING_KEY);
    public_key = kp.publicKey();
    signature = kp.sign(postBody).toString('hex');
  }


    //console.log("Calling URL: " + postURL);
    //console.log("Sender: " + public_key);
    //console.log("Signature: " + signature);
    //console.log("Body: " + postBody);
  return new Promise(function(resolve, reject) {
    request( {
      headers: {
        'X-Request-Sender-Id': public_key,
        'X-Request-Signature-ed25519-hex': signature,
        'X-Request-Topic': postTopic,
        'X-Request-Type': message.type,
        'Content-Type': 'application/json'
      },
      method: 'POST',
      uri: postURL,
      body: postBody
    }, function(error, response, body) {
      keepAlive();
      status_code = response && response.statusCode;
      if (!status_code) {
        status_code = -1;
      }
      //console.log('error:', error);
      //console.log('statusCode:', status_code);
      //console.log('body:', body);

      resp = {}
      resp['status_code'] = status_code
      resp['event_id'] = message.id +'_'+status_code+'_'+(new Date()).toJSON();
      resp['id'] = message.id;
      resp['topic'] = postTopic
      resp['response'] = response && response.toJSON();
      resp['body'] = body;
      resp['error'] = error;

      //console.log('set acct response:', JSON.stringify(resp, null, 2));
      writeResponse(account, hookId, resp);

      if (status_code == 200) {
        //console.log("Event delivered. ", postTopic ,": ", message.id);
        resolve( response.toJSON() );
      } else if (response) {
        //console.log(status_code, " request url: " + postURL);
        resolve( response.toJSON() );
      } else {
        resolve( {'status_code': status_code, 'error': error} );
      }
    });
  });
}

async function startAccountListener(a) {
  console.log("Adding Stellar observer", a, "at", (new Date()).toJSON());
  // Make sure we have a directory and the custom URL data
  var acct = await writeAccount(a);
  if (!acct) { return; }

  accountListeners[a] = {};
  // Set the timout for the cursor event
  accountListeners[a]['timeout'] = setTimeout(function() { readEventCursors(a) }, 0);
}

function removeAccountListener(a) {
  console.log("Removing Stellar observer", a, "at", (new Date()).toJSON());
  clearTimeout(accountListeners[a]);
  //accountListeners[a]();
  delete(accountListeners[a]);
  keepAlive();
}

async function activatePostURL(cursorData, acctId, topic, hookId, activate) {
  var postURL = cursorData['url'];
  var postTopic = 'agent_events';
  var evt = {
    topic: postTopic,
    type: topic,
    id: acctId,
    extras: { 'activate_account': activate }
  }
  console.log("Confirming Request for: ", topic, "; with: ", postURL);
  cursorData['active'] = false;
  var resp = await postEvent(acctId, postURL, postTopic, evt);
  console.log("Confirm Activation Response: ", resp);
  if (resp['statusCode'] == 200) {
    var json = JSON.parse(resp['body']);
    //console.log("Confirm Request Value: ", json['extras']);
    if ('extras' in json && 'activate_account' in json['extras']) {
      cursorData['active'] = activate && json['extras']['activate_account'];
    }
  }
  console.log("Request to activate: ", topic, ": ", cursorData['active'], " for ", postURL);

  return cursorData;
}

var stopAgentListener = null;
function startAgentListener(agentId) {
  console.log("Starting Agent Listener", agentId, "at", (new Date()).toJSON());
  stopAgentListener = stellarServer.operations()
    .forAccount(agentId)
    .cursor('now')
    .stream({
      onmessage: async function (message) {
        //console.log("Agent Account Event: ", JSON.stringify(message, null, 2));
        if (message.type == 'payment'
              && message.to == agentId
              && message.asset_type == 'native'
              //&& message.asset_issuer == agentId
              //&& message.asset_code == agent_asset_code
        ) {
          // Payments add to the credit amount for the account
          // If account has no eventhook URLs, payment is refunded (less txnFee)
          // If exactly 3x txnFee, refund the balance, deactivating the account

          var txnFee = 0.0000100; // should be txn fee from SDK (ledger lookup)
          var baseReserve = 0.5;  // should be reserve from SDK (ledger lookup)
          var updateCredit = false;

          var fromId = message.from;
          var xlmAmount = parseFloat(message.amount);
          var sendRefund = false;

          var acct = await writeAccount(fromId);
          var credit = getClientCredit(fromId);
          //console.log("Account Credit: ", JSON.stringify(credit, null, 2));

          // First confirm with each website that it accepts these POST events
          var eventhooks = getAccountURLs(fromId);
          var notSpam = false;
          for (topic in eventhooks) {
            for (hookId in eventhooks[topic]) {
              var cursorData = getCursorData(fromId, topic, hookId);
              cursorData = await activatePostURL(cursorData, fromId, topic, hookId, true);
              updateCursorData(fromId, topic, hookId, cursorData);
              notSpam = notSpam || cursorData['active'];
            }
          }

          // If Client has requested activation
          if (credit['limit'] == 0) {
            xlmAmount -= txnFee; // Taking trustline or refund txnFee payment

            //console.log("Account Activated: ", notSpam);
            updateCredit = (notSpam && xlmAmount > (txnFee + baseReserve));
            if (updateCredit) {
              //console.log("Received XLM to activate Account.", xlmAmount);
              credit['limit'] = xlmAmount;
            } else {
              //console.log("Account not active or not enough XLM to activate.");
              sendRefund = (xlmAmount > 0);
              if (!sendRefund) {
                //console.log("Account sent too little XLM, no refund issued.");
                xlmAmount = 0;
	      }
            }

          // If Client has requested deactivation and a refund
          } else if (xlmAmount.toFixed(7) == (txnFee * 3).toFixed(7)) {
            updateCredit = true;
            xlmAmount = parseFloat(credit['limit']); // Incoming xlmAmount not added to limit yet, can refund the whole existing balance
            //console.log("Received ", (txnFee*3).toFixed(7), " XLM. Refunding ", agent_asset_code, " Credits: ", xlmAmount.toFixed(7));
            credit['limit'] = 0;

          // If Client did not send enough XLM or account not activated
          } else if (xlmAmount <= (txnFee * 2) || !notSpam) {
            updateCredit = false;
            //console.log("Account sent too little XLM or account not active.");
            xlmAmount -= txnFee; // Taking the refund txnFee payment
            sendRefund = (xlmAmount > 0);
            if (!sendRefund) {
              //console.log("Account sent too little XLM, no refund issued.");
              xlmAmount = 0;
	    }

          // Else no activations, problems, or refunds; increase the credit line
          } else {
            updateCredit = true;
            credit['limit'] += xlmAmount;
            //console.log("Received payment of ", xlmAmount.toFixed(7), " XLM. Increasing ", agent_asset_code, " Credits to: ", credit['limit'].toFixed(7));
          }

          // if agentId allowed to send XLM to itself; it's an infinite loop
          var refundSent = sendRefund && (fromId == agentId);
          if (fromId != agentId) {
            var kp = StellarSdk.Keypair.fromSecret(process.env.AGENT_SIGNING_KEY);
            var agentAccount = await writeAccount(kp.publicKey());

            var transaction = new StellarSdk.TransactionBuilder(agentAccount);

            if (sendRefund) {
              //console.log("Refunding XLM credits. XLM: ", xlmAmount.toFixed(7));
              transaction = transaction.addOperation(
                  StellarSdk.Operation.payment({
                      'destination': fromId,
                      'asset': StellarSdk.Asset.native(),
                      'amount': xlmAmount.toFixed(7)
                  })
              );
            }

            // Clear out any trustline balance that the account sent
            var clientAsset = new StellarSdk.Asset(agent_asset_code, fromId);
            if ('balance' in credit && parseFloat(credit['balance']) > 0) {
              //console.log("Returning ", agent_asset_code, " Asset balance: ", credit['balance']);
              transaction = transaction.addOperation(
                  StellarSdk.Operation.payment({
                      'destination': fromId,
                      'asset': clientAsset,
                      'amount': credit['balance']
                  })
              );
            }

            // Update the agent's credit limit on the trustline for the account
            if (updateCredit) {
              //console.log("Updating ", agent_asset_code, " Credit: ", credit['limit'].toFixed(7));
              var newLimit = '0';
              if (credit['limit'] > 0) newLimit = credit['limit'].toFixed(7);
              transaction = transaction.addOperation(
                  StellarSdk.Operation.changeTrust({
                      'asset': clientAsset, 'limit': newLimit
                  })
              );
            }

            transaction = transaction.build();
            transaction.sign(kp);
            try {
              if (transaction.operations.length > 0) {
                var transactionResult = await stellarServer.submitTransaction(transaction);
                refundSent = sendRefund;
                // Get updated stored state for the account
                // TODO: Manage this straight on the accountListeners Object
                await writeAccount(kp.publicKey());
                //console.log('Transaction Succesful: ', JSON.stringify(transactionResult, null, 2));
              }
            } catch(err) {
              //console.log("Troublehoot: ", err);
              console.log('Error on Transaction:');
              if (typeof(err) == 'object') {
                console.log(JSON.stringify(err, null, 2));
              } else {
                console.log(err);
              }
            }
          }
          if (refundSent) {
            //console.log('Successfully refunded XLM: ', xlmAmount.toFixed(7), " to ", fromId);
            credit['limit'] -= xlmAmount;
            if (credit['limit'] < 0) { credit['limit'] = 0; }
          }

          if (credit['limit'] > 0 && fromId in accountListeners) {
            //console.log("Already listenting for account.");
          } else if (credit['limit'] > 0) {
            startAccountListener(fromId);
          } else if (fromId in accountListeners) {
            removeAccountListener(fromId);
          }

          //console.log("NEW CREDIT: ", JSON.stringify(credit, null, 2));
        }
      }
      , onerror: function (e) {
        console.error(e);
      }
    }
  );
}

function decodeData(encString, encType) {
  var buf = null;
  if (typeof Buffer.from === "function") {
    // Node 5.10+
    buf = Buffer.from(encString, encType).toString('ascii'); // Ta-da
  } else {
    // older Node versions
    buf = new Buffer(encString, encType).toString('ascii'); // Ta-da
  }
  return buf;
}

const gqlURL = 'https://mike-horizon-test.gly.sh/graphql';
function getAssetOperations(acctId, paging_token = null) {
  if (!paging_token) { paging_token = 0; }
  var queryName = 'allHistoryOperations';

  graphql = '' +
'query iOps {' +
'   ' + queryName + '(' +
'    orderBy: [ID_ASC],' +
'    filter:{' +
'      or: [' +
'        {details: {contains:{asset_issuer: "' + acctId + '"}}},' +
'        {details: {contains:{sell_asset_issuer: "' + acctId + '"}}},' +
'        {details: {contains:{buy_asset_issuer: "' + acctId + '"}}}' +
'      ]' +
'      rowId: {greaterThan:"' + paging_token + '"},' +
'    }';
  if (!paging_token) {
    graphql += '    last: 1,';
  } else {
    graphql += '    first: 1,';
  }
  graphql +=
'  ), ' +
'  { nodes { rowId, type } } ' +
'}';

  var queryJson = {"query": graphql};
  return new Promise(function(resolve, reject) {
    request(
      {
        headers: {
          'Content-Type': 'application/json'
        },
        method: 'POST',
        uri: gqlURL,
        json: queryJson
      },
      function(error, response, body) {
        if (!error) {
          //console.log('body: ', JSON.stringify(body));
          if ('data' in body && queryName in body['data']) {
            var data = body['data'][queryName];
            if (data['nodes'].length) {
              var idx = data['nodes'].length - 1;
              var evt = {}; //data['nodes'][idx]['details'];
              evt['paging_token'] = data['nodes'][idx]['rowId'];
              evt['id'] = data['nodes'][idx]['rowId'];
              evt['type'] = data['nodes'][idx]['type'];
              resolve(evt);
            } else {
              resolve(null);
            }
          }
        } else {
          reject(error);
        }
      }
    );
  });
}

/* ysm.array.js; Yan Morin <progysm@gmail.com>; 2014-12-09 */
var ysm = {};
ysm.array = {};

/**
 * Run a function for each item inside an array, using a timeout
 * @param Array arr
 * @param Function callback (parameters are value, index, array)
 * @param Number timems (in milliseconds)
 * @return undefined
 */
ysm.array.forEachTime = function(arr, callback, timems, finish) {
   var i = 0;
   (function c() {
       if (i < arr.length) {
           callback(arr[i], i, arr);
           i++;
           if (i === arr.length) {if (finish) { return finish(); }}
           else { setTimeout(c, timems); }
       }
   })();
}

/**
 * Run a function for each item inside an array, infinitely, using a timeout
 * @param Array arr
 * @param Function callback (parameters are value, index, array)
 * @param Number timems (in milliseconds)
 * @return undefined
 */
ysm.array.forEachTimeLoop = function(arr, callback, timems) {
   var i = 0;
   (function c() {
       if (i < arr.length) {
           callback(arr[i], i, arr);
           i++;
           if (i === arr.length) { i = 0; }
           setTimeout(c, timems);
       }
   })();
}
