require('dotenv').config()

var _ = require('lodash');
const request = require('request')
const express = require('express');
const app = express();
const bodyParser = require("body-parser");
const path = require('path');
const StellarSdk = require('stellar-sdk');
const fs = require('fs');
const mkdirp = require('mkdirp');
const sleep = require('system-sleep');
const glob = require('glob-fs')({ builtins: false });

const stellarServer = new StellarSdk.Server(process.env.STELLAR_SERVER_URL);
const stellarNetworkDomain = process.env.STELLAR_DOMAIN;
const stellarNetworkName = process.env.STELLAR_NETWORK_NAME;
const stellarNetworkSeed = process.env.STELLAR_NETWORK_SEED;
StellarSdk.Network.use(new StellarSdk.Network(stellarNetworkSeed));

const posix = require('posix');
try {
  posix.setrlimit('nofile', { soft: 10000 });
}
catch(err) {
  console.log(err);
}

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

app.use(bodyParser.urlencoded({ extended: false }));
app.use(bodyParser.json());


var agent_account_id = "no signing key provided";
var agent_asset_code = 'NTFY';

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
  addAccountListener(accountId);
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

//Should rearrange events to have an "account" context too
//People are likely to want to see events for "their" account(s)
//Perhaps repull them from the ledger instead of the FS (won't have status code)
app.get('/events',function(req,res) {
  var acctId = req.query.account_id;
  var events = {}
  var eventhooks = getAccountURLs(acctId);
  for (eIdx in eventhooks) {
    var hookId = hashCode(eventhooks[eIdx]);
    var eventIds = getAccountQueuedEventIDs(acctId, hookId);
    for (eIdx in eventIds) {
      var eventId = eventIds[eIdx];

      // Make sure the eventid entry is built only once
      if (!(eventId in events)) { events[eventId] = {}; }
      else { continue; }

      events[eventId]['data'] = getEvent(acctId, hookId, eventId);
      events[eventId]['responses'] = getEventResponseIds(acctId, hookId, eventId, '*');
    }
  }
  res.json(events);
  keepAlive();
});

// TODO: how do you do this?
app.get('/responses',function(req,res) {
  var acctId = req.query.account_id;
  // return event data and response file contents
});

async function main() {
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
        setTimeout(function() { readEventCursors(a) }, 5000);
        addAccountListener(a);
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

function getEvent(acctId, hookId, eventId) {
  var eventFile = getEventFilename(acctId, hookId, eventId);
  console.log("Loading Event File: ", eventFile);
  if (fsExistsSync(eventFile)) {
    return JSON.parse(fs.readFileSync(eventFile, 'utf8'));
  } else {
    return null;
  } 
}

function getEventResponseIds(acctId, hookId, eventId, matchstr) {
  var respDir = getEventResponsesDirectory(acctId, hookId, eventId);

  // glob is merging with past results; reset this way due to weak JS-Fu
  var glob = require('glob-fs')({ builtins: false });

  matchstr = respDir + eventId + '_' + matchstr;
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


function isEventSent(acctId, hookId, eventId) {
  var respFiles = getEventResponseIds(acctId, hookId, eventId, '200_*');
  return (respFiles.length > 0);
}

function writeResponse(acctId, hookId, resp) {
  var respDir = getEventResponsesDirectory(acctId, hookId, resp.event_id);

  console.log("Making Response Dir: " + respDir);
  mkdirp(respDir, function(err) {
    if (err == null) { 
      var json = JSON.stringify(resp, null, 2);
      var respFile = respDir + resp.id;
      console.log("Writing Data: " + '[not shown at this time]');
      //console.log(json);
      fs.writeFileSync(respFile, json, 'utf8');
    } else {
      console.log("Error making dir: " + err);
    }
  });
}

// TODO: events must be tracked at the account level
// the same event must be successfully deliverd for multiple accounts
function deleteAccountQueuedEvent(acctId, hookId, evt) {
  var eventFile = getAccountQueueDirectory(acctId, hookId) + evt.id;
  console.log("Deleting: " + eventFile);
  fs.unlink(eventFile, function (err) { return; });
}

function fsExistsSync(filePath) {
  try {
    fs.accessSync(filePath);
    return true;
  } catch (e) {
    return false;
  }
}

function writeEvent(acctId, hookId, evt) {
  respDir = getEventResponsesDirectory(acctId, hookId, evt.id);
  console.log("Making Event Dir: " + respDir);
  mkdirp(respDir, function(err) {
    if (err == null) { 
      var eventFile = getEventFilename(acctId, hookId, evt.id);
      console.log("Writing event file: " + eventFile);
      var json = JSON.stringify(evt, null, 2);
      console.log(json);
      fs.writeFileSync(eventFile, json, 'utf8');
    }
  });
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

function getAccountListFromEvent(evt, topic) {
  // for all topics 'transctions', 'operations', 'payments', and 'effects'
  var msg_accounts = [evt.source_account];

  // for topics that have a type: 'operations', 'payments', and 'effects'
  if ( 'type' in evt ) {
    var opType = topic + ':' + evt['type'];
    if (opType in acct_fields) {
      var fields = acct_fields[opType];
      for (fIdx in fields) {
        if (fields[fIdx] in evt) {
          var acctId = evt[ fields[fIdx] ];
          if ( !msg_accounts.includes(acctId) ) {
            msg_accounts.push(acctId);
          }
        }
      }
    }
  }
  return msg_accounts;
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

function queueEvent(evt, topic) {
  var acctList = getAccountListFromEvent(evt, topic);
  for (aIdx in acctList) {
    acctId = acctList[aIdx];
    var aDir = getAccountDirectory(acctId);
    if (!fsExistsSync(aDir)) { continue; }

    var eventhooks = getAccountURLs(acctId);
    for (hookId in eventhooks) {
      //TODO: make sure this works without the "topic" code
      writeEvent(acctId, hookId, evt);

      var eventDir = getAccountQueueDirectory(acctId, hookId);
      mkdirp(eventDir, function(err) {
        if (err == null) { 
          var eventFile = eventDir + evt.id;
          if (!fsExistsSync(eventFile)) {
            console.log("Queueing event file: " + eventFile);
            var json = JSON.stringify(evt, null, 2);
            fs.writeFileSync(eventFile, json, 'utf8');
          }
        }
      });
    }
  }
}

// For the managed data URLs
async function writeAccount(a) {

  console.log("Loading Account: " + a);
  try {
    var acct = await stellarServer.loadAccount(a);

    var acctDir = getAccountDirectory(a);
    console.log("Making Dir: " + acctDir);
    await mkdirp(acctDir);
    var acctFile = getAccountFilename(a);
    json = JSON.stringify(acct, null, 2);
    console.log("Writing Account Data: " + json);
    fs.writeFileSync(acctFile, json, 'utf8');

    return acct;

  } catch(e) {
    console.error(e);
    return null;
  }
}

// For the managed data URLs
async function updateCursorData(acctId, cursorData) {
  console.log("Updating Sequence Tracking for account: " + acctId);

  var acctData = await writeAccount(acctId);
  try {
    var acctDir = getAccountDirectory(acctId);
    // Make sure account directory exists
    await mkdirp(acctDir);
    var cursorFile = acctDir + '.cursors.json'

    json = JSON.stringify(cursorData, null, 2);
    console.log("Writing Cursor Data: " + json);
    fs.writeFileSync(cursorFile, json, 'utf8');

    return cursorData;

  } catch(e) {
    console.error(e);
    return null;
  }
}

function getCursorData(acctId) {
  var acctDir = getAccountDirectory(acctId);
  var cursorData = {};
  var cursorFile = acctDir + '.cursors.json';
  if (fsExistsSync(cursorFile)) {
    cursorData = JSON.parse(fs.readFileSync(cursorFile, 'utf8'));
  }

  var eventhooks = getAccountURLs(acctId);
  for (topic in eventhooks) {
    if (!(topic in cursorData)) { cursorData[topic] = {}; }
    for (hookId in eventhooks[topic]) {
      if (!(hookId in cursorData[topic])) { cursorData[topic][hookId] = {}; }
      cursorData[topic][hookId]['url'] = eventhooks[topic][hookId];
    }
  }
  
  for (topic in cursorData) {
    if (topic in eventhooks) {
      for (hookId in cursorData[topic]) {
        if (!(hookId in eventhooks[topic])) {
          delete(cursorData[topic][hookId]);
        }
      }
    } else {
      delete(cursorData[topic]);
    }
  }
  return cursorData;
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
    console.log("Adding Active Client: ", tLine);

    activeAccounts.push(tLine['asset_issuer']);
  }
  return activeAccounts;
}

function getActiveAccounts() {
  var activeAccounts = [];
  var acctDir = getAccountDirectory(null);
  //console.log("Looking in dir: " + acctDir);
  if (fsExistsSync( acctDir )) {
    var acctIds = fs.readdirSync(acctDir);
    //console.log("Found Accounts; Count: " + acctIds.length);
    for (aIdx in acctIds) {
      //Check account is within the txn rate threshhold
      var credit = getClientCredit(acctIds[aIdx]);

      // For now just check that the credit balance > 0
      if (credit['limit'] > 0) {
        activeAccounts.push(acctIds[aIdx]);
        //console.log("Adding Active Account: " + acctIds[aIdx]);
      }
    }
  }
  return activeAccounts;
}

function getServerCall(topic) {
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

async function readEventCursors(acctId) {
  console.log("Reading events for account: " + acctId);

  //var cursorData = accountListeners[acctId]['cursors'];
  var cursorData = getCursorData(acctId);
  await updateCursorData(acctId, cursorData);
  for (topic in cursorData) {
    for (hookId in cursorData[topic]) {
      var postURL = cursorData[topic][hookId]['url'];
      var paging_token = cursorData[topic][hookId]['paging_token'];

      var serverCall = getServerCall(topic);
      if (!serverCall) { continue; }

      if (!paging_token) {
        console.log("No Paging Token: ", topic, "/", hookId);
        var r = await serverCall
          .forAccount(acctId)
          .limit(1)
          .order('desc')
          .cursor('now')
          .call();

        console.log("Results: ", JSON.stringify(r, null, 2));
        if (r.records.length > 0) {
          var evt = r.records[0];
          paging_token = evt['paging_token'];
          cursorData[topic][hookId]['paging_token'] = paging_token;
        }
        serverCall = getServerCall(topic);
      }

      try {

        console.error("Testing for Topic: ", topic, "; HookId: ", hookId, "; Paging_token: ", paging_token);
        var r = await serverCall
          .forAccount(acctId)
          .limit(1)
          .order('asc')
          .cursor(paging_token)
          .call();

        if (r.records.length > 0) {
          var evt = r.records[0];
          console.log(JSON.stringify(evt, null, 2));

          var statusCode = await postEvent(acctId, postURL, topic, evt);
          if (statusCode == 200) {
            cursorData[topic][hookId]['paging_token'] = evt['paging_token'];
          }
        }
      } catch(err) {
        console.error("An error happened while retrieving the event.");
        console.error(JSON.stringify(err, null, 2));
      }

      // Wait for X time to honor the account's "postRate"
    }
    await updateCursorData(acctId, cursorData);
  }
  // Wait for X time to honor the account's "postRate"
  setTimeout(function() { readEventCursors(acctId) }, 5000);
}

function readEvents() {
  //var acctList = getActiveAccounts();
  //var acctList = getActiveClients();
  var acctList = Object.keys(accountListeners);
  for (aIdx in acctList) {
    var acctId = acctList[aIdx];
    console.log("Reprocessing messages for account: " + acctId);

    var eventhooks = getAccountURLs(acctId);
    for (hookId in eventhooks) {
      //TODO:Get next ID for "hookType" (txn, op, efkt)
      var eventIds = getAccountQueuedEventIDs(acctId, hookId);
      console.log("Reprocessing messages for URL: " + hookId + ' - ' + eventIds.length + ' messages');
      for (eIdx in eventIds) {
        keepAlive();
        var evt = getEvent(acctId, hookId, eventIds[eIdx]);
        var idx = parseInt(eIdx) + 1
        console.log("Event ID [" + idx + "/" + eventIds.length + "]: " + eventIds[eIdx]);
        console.log("Event: " + '[not shown at this time]'); //JSON.stringify(evt, null, 2));
        //Check backoff schedule here?
        if (evt) {
          if (isEventSent(acctId, hookId, evt.id)) {
            console.log("Event already delivered: " + evt.id);
            deleteAccountQueuedEvent(acctId, hookId, evt);
          } else {
            console.log("Resubmitting failed event: " + evt.id);
            postEvent(acctId, eventhooks[hIdx], "operations", evt);
            keepAlive();
            sleep(2000); // 5 seconds
          }
        }
      }
    }
  }
  // No KeepAlive() calls are made unless there are queued events to process
  // This timeout is separate from that one and will not keep the progam alive
  setTimeout(readEvents, 5000);
}

function getAccount(acct) {
  var acctData = null;
  var acctFile = getAccountFilename(acct);
  console.log("Loading Account File: " + acctFile);
  if (fsExistsSync(acctFile)) {
    // Load the account data
    acctData = JSON.parse(fs.readFileSync(acctFile, 'utf8'));
  }
  return acctData;
}

function getClientCredit(clientId) {
  var agentData = getAccount(agent_account_id);
  balances = agentData.balances;
  for (tIdx in balances) {
    tLine = balances[tIdx];
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

async function updateClientCredit(tLine) {
  var txnFee = 0.0000100; // should be txn fee from SDK (ledger lookup)

  tLine['limit'] = tLine['limit'].toFixed(7);
  console.log('Updating trustline: ', JSON.stringify(tLine, null, 2));

  var clientAsset = new StellarSdk.Asset(tLine['asset_code'], tLine['asset_issuer']);

  var kp = StellarSdk.Keypair.fromSecret(process.env.AGENT_SIGNING_KEY);
  var agentAccount = await writeAccount(kp.publicKey());
  var transaction = new StellarSdk.TransactionBuilder(agentAccount)
      .addOperation(StellarSdk.Operation.changeTrust({
          'asset': clientAsset,
          'limit': tLine['limit']
      }))
      .build();

  transaction.sign(kp);
  try {
    var transactionResult = await stellarServer.submitTransaction(transaction);
    
    console.log('Successfully updated trustline: ', JSON.stringify(tLine, null, 2));
  } catch(err) {
    console.log('An error has occured:');
    console.log(JSON.stringify(err, null, 2));
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
      var agentDataPrefix = 'stellarwatch:';
      if (key.startsWith(agentDataPrefix)) {
        var postURL = decodeData(acctData['data_attr'][key], 'base64');
        if (postURL) {
          var hookId = hashCode(postURL);
          var topic = key.toLowerCase().split(':')[1];

          if (getServerCall(topic)) {
            if (!(topic in eventhooks)) { eventhooks[topic] = {}; }
            eventhooks[topic][hookId] = postURL;
          }
        }
      }
    });
  }

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
  console.log("Posting message to eventhook [", hookId, "]: ", postURL);

  if (!('type' in message)) {
    message['type'] = '';
  }

  var postBody = JSON.stringify({
    'topic': postTopic,
    'type': message.type, 
    'id': message.id, 
    'network': {
      'domain': stellarNetworkDomain,
      'name': stellarNetworkName,
      'seed': stellarNetworkSeed,
      'id': StellarSdk.Keypair.master().publicKey()
    }
  });

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
      resp['id'] = message.id + '_' + status_code + '_' + (new Date()).toJSON();
      resp['event_id'] = message.id;
      resp['topic'] = postTopic
      resp['response'] = response;
      resp['body'] = body;
      resp['error'] = error;

      //console.log('set acct response:', JSON.stringify(resp, null, 2));
      writeResponse(account, hookId, resp);

      if (status_code == 200) {
        console.log("Event delivered. ", postTopic ,": ", message.id);
        deleteAccountQueuedEvent(account, hookId, message);
        resolve(status_code);
      } else {
        console.log(status_code, " request url: " + postURL);
        resolve(status_code);
      }
    });
  });
}

async function addAccountListener(a) {
  console.log("Adding Stellar observer", a, "at", (new Date()).toJSON());
  // Make sure we have a directory and the custom URL data
  var acct = await writeAccount(a);
  if (!acct) { return; }

  accountListeners[a] = stellarServer.operations()
    .forAccount(a)
    //.cursor(acct['cursor'])
    .cursor('now')
    .stream({
      onmessage: function (message) {
        keepAlive();
        idKey = message.id
        short_hash = message.transaction_hash.slice(0,3) + '..' + message.transaction_hash.slice(-4)
        console.log("OPID", idKey, " TXNID:", short_hash," paging_token", message.paging_token);
        //console.log("message", message);

        console.log("Received new event: " + idKey);
        console.log("message", message);
        queueEvent(message, "operations");
      }
    });

  // For now, deactivate the listener
  accountListeners[a]();
}

function removeAccountListener(a) {
  console.log("Removing Stellar observer", a, "at", (new Date()).toJSON());
  accountListeners[a]();
  delete(accountListeners[a]);
  keepAlive();
}

var stopAgentListener = null;
async function startAgentListener(agentId) {
  console.log("Starting Agent Listener", agentId, "at", (new Date()).toJSON());
  stopAgentListener = stellarServer.operations()
    .forAccount(agentId)
    .cursor('now')
    .stream({
      onmessage: async function (message) {
        if (message.type == 'payment'
              && message.to == agentId
              && message.asset_type == 'native'
              //&& message.asset_issuer == agentId
              //&& message.asset_code == 'NTFY'
        ) {
          // Payments add to the credit amount for the account
          // If account has no eventhook URLs, payment is refunded (less txnFee)
          // If exactly 3x txnFee, refund the balance, deactivating the account

          var txnFee = 0.0000100; // should be txn fee from SDK (ledger lookup)
          var baseReserve = 0.5;  // should be reserve from SDK (ledger lookup)
          var updateCredit = true;
          
          var fromId = message.from;
          var xlmAmount = parseFloat(message.amount);
          var sendRefund = false;

          var acct = await writeAccount(fromId);
          var credit = getClientCredit(fromId);
          console.log("Account Credit: ", JSON.stringify(credit, null, 2));

          // If Client has requested activation
          if (credit['limit'] == 0) {
            var eventhooks = getAccountURLs(fromId);
            xlmAmount -= txnFee; // Taking trustline or refund payment txnFee

            console.log("Eventhooks: ", eventhooks);
            updateCredit = (eventhooks && xlmAmount > (txnFee + baseReserve))
            if (updateCredit) {
              console.log("Received XLM to activate Account.", xlmAmount);
              credit['limit'] = xlmAmount;
            } else {
              console.log("Account not active and not enough XLM to activate.");
              sendRefund = (xlmAmount > 0);
              if (!sendRefund) {
                console.log("Account sent too little XLM, no refund issued.");
                xlmAmount = 0;
	      }
            } 

          // If Client has requested deactivation and a refund
          } else if (xlmAmount.toFixed(7) == (txnFee * 3).toFixed(7)) {
            xlmAmount = parseFloat(credit['limit']); // Incoming xlmAmount not added to limit yet, can refund the whole existing balance
            console.log("Received ", (txnFee*3).toFixed(7), " XLM. Refunding NTFY Credits: ", xlmAmount.toFixed(7));
            credit['limit'] = 0;

          // If Client did not send enough XLM
          } else if (xlmAmount <= (txnFee * 2)) {
            console.log("Account sent too little XLM.");
            updateCredit = false;
            xlmAmount -= txnFee; // Taking the refund payment txnFee
            sendRefund = (xlmAmount > 0);
            if (!sendRefund) {
              console.log("Account sent too little XLM, no refund issued.");
              xlmAmount = 0;
	    }

          // Else no activations, problems, or refunds; increase the credit line
          } else {
            credit['limit'] += xlmAmount;
            console.log("Received payment of ", xlmAmount.toFixed(7), " XLM. Increasing NTFY Credits to: ", credit['limit'].toFixed(7));
          }

          // if agentId allowed to send XLM to itself; it's an infinite loop
          var refundSent = sendRefund && (fromId == agentId);
          if (fromId != agentId) {
            var kp = StellarSdk.Keypair.fromSecret(process.env.AGENT_SIGNING_KEY);
            var agentAccount = await writeAccount(kp.publicKey());

            var transaction = new StellarSdk.TransactionBuilder(agentAccount);

            if (sendRefund) {
              console.log("Refunding XLM credits. XLM: ", xlmAmount.toFixed(7));
              transaction = transaction.addOperation(
                  StellarSdk.Operation.payment({
                      'destination': fromId,
                      'asset': StellarSdk.Asset.native(),
                      'amount': xlmAmount.toFixed(7)
                  })
              );
            }

            // Add operations to update limit and refund trustline balances
            var clientAsset = new StellarSdk.Asset(agent_asset_code, fromId);
            if ('balance' in credit && parseFloat(credit['balance']) > 0) {
              console.log("Refunding NTFY Asset balance: ", credit['balance']);
              transaction = transaction.addOperation(
                  StellarSdk.Operation.payment({
                      'destination': fromId,
                      'asset': clientAsset,
                      'amount': credit['balance']
                  })
              );
            }

            if (updateCredit) {
              console.log("Updating NTFY Credit: ", credit['limit'].toFixed(7));
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
                console.log('Transaction Succesful: ', JSON.stringify(transactionResult, null, 2));
              }
            } catch(err) {
              //console.log("Troublehoot: ", err);
              console.log('Error on Transaction:');
              console.log(JSON.stringify(err, null, 2));
            }
          }
          if (refundSent) {
            console.log('Successfully refunded XLM: ', xlmAmount.toFixed(7), " to ", fromId);
            credit['limit'] -= xlmAmount;
            if (credit['limit'] < 0) { credit['limit'] = 0; }
          }

          if (credit['limit'] > 0 && fromId in accountListeners) {
            console.log("Already listenting for account.");
          } else if (credit['limit'] > 0) {
            addAccountListener(fromId);
          } else if (fromId in accountListeners) {
            removeAccountListener(fromId);
          }

          console.log("NEW CREDIT: ", JSON.stringify(credit, null, 2));
        }
      }
      , onerror: function (e) {
        console.error(e);
      }
    });
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
