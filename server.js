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

//
// Local Cache
//
var accountListeners = []
const accounts = [
  "GB25PA4Z5NF34WSL737ZE7VKBMBPNDJGJAAMHKYCDGYZ6QFPQLDIUJSF"
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
  var opTypes = ['payment']
  var events = {}
  for (tIdx in opTypes) {
    var opType = opTypes[tIdx];
    var eventIds = getAccountQueuedEventIDs(acctId, opType);
    for (eIdx in eventIds) {
      var eventId = eventIds[eIdx];

      // Make sure the eventid entry is built only once
      if (!(eventId in events)) { events[eventId] = {}; }
      else { continue; }

      events[eventId]['data'] = getEvent(acctId, opType, eventId);
      events[eventId]['responses'] = getEventResponseIds(acctId, opType, eventId, '*');
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
  startListeners();
  setTimeout(readEvents, 5000);
  keepAlive();
}

//
// Getters and Helpers
//

function startListeners() {
  _.forEach(accounts, function(a) {
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

function getEventResponsesDirectory(acctId, type, eventId = null) {
  var baseDir = getAccountDirectory(acctId) + type + responsesDir;
  if (eventId) { return baseDir + eventId + '/'; }
  return baseDir;
}

function getEventFilename(acctId, type, eventId) {
  eventDir = getEventResponsesDirectory(acctId, type, eventId);
  return eventDir + '.data.json';
}

function getResponseFilename(acctId, type, responseId) {
  var eventId = responseId.split("_")[0]
  return getEventResponsesDirectory(acctId, type, eventId) + responseId;
}

function getAccountQueueDirectory(acctId, type) {
  return getAccountDirectory(acctId) + type + eventsDir;
}

function getAccountQueuedEventIDs(acctId, type) {
  var evtDir = getAccountQueueDirectory(acctId, type);
  if (fsExistsSync(evtDir)) {
    var files = fs.readdirSync(evtDir);
    return files;
  } else {
    console.log("Event Queue not found: " + evtDir);
    return [];
  }
}

function getAccountEventIDs(acctId, type) {
  var acctEventsDir = getEventResponsesDirectory(acctId, type);
  if (fsExistsSync(acctEventsDir)) {
    var eventids = fs.readdirSync(acctEventsDir);
    return eventids;
  } else {
    return [];
  }
}

function getEvent(acctId, type, eventId) {
  var eventFile = getEventFilename(acctId, type, eventId);
  console.log("Loading Event File: ", eventFile);
  if (fsExistsSync(eventFile)) {
    return JSON.parse(fs.readFileSync(eventFile, 'utf8'));
  } else {
    return null;
  } 
}

function getEventResponseIds(acctId, type, eventId, matchstr) {
  var respDir = getEventResponsesDirectory(acctId, type, eventId);

  // glob is merging with past results; reset this way due to weak JS-Fu
  var glob = require('glob-fs')({ builtins: false });

  matchstr = respDir + eventId + '_' + matchstr;
  var respFiles = glob.readdirSync(matchstr);

  respFiles.forEach(function(part, index, arr) {
    arr[index] = part.substr(part.lastIndexOf("/")+1);
  });
  return respFiles;
}

function getResponse(acctId, type, responseId) {
  var respFile = getResponseFilename(acctId, type, responseId);
  if (fsExistsSync(respFile)) {
    return JSON.parse(fs.readFileSync(respFile, 'utf8'));
  } else {
    return null;
  }
}


function isEventSent(acctId, type, eventId) {
  var respFiles = getEventResponseIds(acctId, type, eventId, '200_*');
  return (respFiles.length > 0);
}

function writeResponse(acctId, type, resp) {
  var respDir = getEventResponsesDirectory(acctId, type, resp.event_id);

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
function deleteAccountQueuedEvent(acctId, evt) {
  var eventFile = getAccountQueueDirectory(acctId, evt.type) + evt.id;
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

function writeEvent(acctId, type, evt) {
  respDir = getEventResponsesDirectory(acctId, type, evt.id);
  console.log("Making Event Dir: " + respDir);
  mkdirp(respDir, function(err) {
    if (err == null) { 
      var eventFile = getEventFilename(acctId, type, evt.id);
      console.log("Writing event file: " + eventFile);
      var json = JSON.stringify(evt, null, 2);
      console.log(json);
      fs.writeFileSync(eventFile, json, 'utf8');
    }
  });
}

const acct_fields = {

  // Operations
  'eventhook:operation': ['source_account'],
  'eventhook:operation:create_account': ['funder','account'],
  'eventhook:operation:payment': ['from','to','asset_issuer'],
  'eventhook:operation:path_payment': ['from','to','asset_issuer'],
  'eventhook:operation:manage_offer': ['buy_asset_issuer','sell_asset_issuer'],
  'eventhook:operation:create_passive_offer': ['buy_asset_issuer','sell_asset_issuer'],
  'eventhook:operation:set_options': ['buy_asset_issuer','sell_asset_issuer'],
  'eventhook:operation:change_trust': ['asset_issuer'],
  'eventhook:operation:allow_trust': ['asset_issuer'],
  'eventhook:operation:merge_account': ['destination'],
  'eventhook:operation:manage_data': ['source_account'],

  // Effects
  'eventhook:effect': ['source_account'],

  // Account Effects
  'eventhook:effect:account_created': ['account'], // 0
  'eventhook:effect:account_merged': ['account'], // 1
  'eventhook:effect:account_credited': ['account'], // 2
  'eventhook:effect:account_debited': ['account'],  // 3
  'eventhook:effect:thresholds_updated': ['account'],  // 4
  'eventhook:effect:homedomain_updated': ['account'],  // 5
  'eventhook:effect:flags_updated': ['account'],  // 6

  // Signer Effects
  'eventhook:effect:signer_created': ['account'],  // 10
  'eventhook:effect:signer_removed': ['account'],  // 11
  'eventhook:effect:signer_updated': ['account'],  // 12

  //Trustline Effects
  'eventhook:effect:trustline_created': ['account','asset_issuer'], // 20
  'eventhook:effect:trustline_removed': ['account','asset_issuer'], // 21
  'eventhook:effect:trustline_updated': ['account','asset_issuer'], // 22
  'eventhook:effect:trustline_authorized': ['account','asset_issuer'], // 23
  'eventhook:effect:trustline_deauthorized': ['account','asset_issuer'], // 24

  // Trading Effects
  'eventhook:effect:offer_created': ['account','seller','sold_asset_issuer','bought_asset_issuer'], // 30
  'eventhook:effect:offer_removed': ['account','seller','sold_asset_issuer','bought_asset_issuer'], // 31
  'eventhook:effect:offer_updated': ['account','seller','sold_asset_issuer','bought_asset_issuer'], // 32
  'eventhook:effect:trade': ['account','seller','sold_asset_issuer','bought_asset_issuer'], // 33

  // Data Effects
  'eventhook:effect:data_created': ['account'], // 40
  'eventhook:effect:data_removed': ['account'], // 41
  'eventhook:effect:data_updated': ['account'], // 42

  // Transactions
  'eventhook:transaction': ['source_account']
}

function getAccountListFromEvent(evt) {
  var msg_accounts = [evt.source_account];
  t = 'eventhook:operation:' + evt.type; // t = evt.type;
  for (op in acct_fields) {
    opType = op; //op.replace('eventhook:','').replace('operation:','');
    if (t == opType) {
      var fields = acct_fields[op];
      for (fIdx in fields) {
        msg_accounts.push(evt[ fields[fIdx] ]);
      }
    }
  }
  return msg_accounts;
}

function queueEvent(evt) {
  var type = evt.type;

  var acctList = getAccountListFromEvent(evt);
  for (aIdx in acctList) {
    acctId = acctList[aIdx];
    var aDir = getAccountDirectory(acctId);
    if (!fsExistsSync(aDir)) { continue; }

    writeEvent(acctId, type, evt);

    var eventDir = getAccountQueueDirectory(acctId, type);
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

// For the managed data URLs
function writeAccount(a) {

  console.log("Loading Account: " + a);
  stellarServer.loadAccount(a)
    .then(function(account) {
      var aDir = getAccountDirectory(a);
      console.log("Making Dir: " + aDir);
      mkdirp(aDir, function(err) {
        if (err == null) { 
          var acctFile = getAccountFilename(a);
          json = JSON.stringify(account, null, 2);
          console.log("Writing Account Data: " + json);
          fs.writeFileSync(acctFile, json, 'utf8');
        }
      });
    })
    .catch(function(e) {
      console.error(e);
    });
}

function getActiveAccounts() {
  var activeAccounts = [];
  var acctDir = getAccountDirectory(null);
  //console.log("Looking in dir: " + acctDir);
  if (fsExistsSync( acctDir )) {
    var acctIds = fs.readdirSync(acctDir);
    //console.log("Found Accounts; Count: " + acctIds.length);
    for (aIdx in acctIds) {
      // For now, always add the account
      activeAccounts.push(acctIds[aIdx]);
      //console.log("Adding Active Account: " + acctIds[aIdx]);
      
      var acctFile = getAccountDirectory(acctIds[aIdx]) + '.credit.json';
      if (fsExistsSync(acctFile)) {
        // Load the account data
        var acctCredits = JSON.parse(fs.readFileSync(acctFile, 'utf8'));
      }
    }
  }
  return activeAccounts;
}

function readEvents() {
  var acctList = getActiveAccounts();
  //var acctList = accountListeners;
  for (aIdx in acctList) {
    var acctId = acctList[aIdx];
    console.log("Reprocessing messages for account: " + acctId);
    var opTypes = ['create_account','payment'];
    for (tIdx in opTypes) {
      var opType = opTypes[tIdx];
      var eventIds = getAccountQueuedEventIDs(acctId, opType);
      console.log("Reprocessing messages for type: " + opType + ' - ' + eventIds.length + ' messages');
      for (eIdx in eventIds) {
        keepAlive();
        var evt = getEvent(acctId, opType, eventIds[eIdx]);
        var idx = parseInt(eIdx) + 1
        console.log("Event ID [" + idx + "/" + eventIds.length + "]: " + eventIds[eIdx]);
        console.log("Event: " + '[not shown at this time]'); //JSON.stringify(evt, null, 2));
        //Check backoff schedule here?
        if (evt) {
          if (isEventSent(acctId, opType, evt.id)) {
            console.log("Event already delivered: " + evt.id);
            deleteAccountQueuedEvent(acctId, evt);
          } else {
            console.log("Resubmitting failed event: " + evt.id);
            processEvent(evt, acctId);
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

function getAccountURLs(acct) {
  var acctList = getActiveAccounts();
  //var acctList = accountListeners;
  var acctFile = getAccountFilename(acct);
  if (acctList.indexOf(acct) > -1 && fsExistsSync(acctFile)) {
    // Load the account data
    var acctData = JSON.parse(fs.readFileSync(acctFile, 'utf8'));

    // Fetch the eventhook URLs from the account's data_attr
    var eventhooks = {}
    Object.keys(acctData['data_attr']).forEach(function(key) {
      console.log("Testing key: " + key);
      if (key.startsWith('eventhook:')) {
        eventhooks[key] = decodeData(acctData['data_attr'][key], 'base64');
      }
    });
    return eventhooks;

    // // hardcode the URL response for now
    // var eventhooks = {
      // 'eventhook:operation:create_account': [process.env.WEBHOOK_URL],
      // 'eventhook:operation:payment': [process.env.WEBHOOK_URL]
    // }
    // return eventhooks;
  } else {
    return {}
  }
}

function processEvent(message, acctId = null) {
  keepAlive();

  var msg_accounts = [acctId];
  if (acctId == null) {
    msg_accounts = getAccountListFromEvent(message);
  }

  for (aIdx in msg_accounts) {
    var acctId = msg_accounts[aIdx];
    if (acctId in accountListeners) {
      postOperation(acctId, message);
    }
  }
}

// Should we do something like this? or an object with functions keyed by type?
//account.on.transaction(function(message) { /* Post transaction event */ });
//account.on.operation(function(message) { /* Post operation event */ });
//account.on.effect(function(message) { /* Post effect event */ });
//account.on.payment(function(message) { /* Post payment event */ });

function postOperation(account, message) {
    var opTypes = ['', message.type];
    var foundEventURL = false;
    for (opIdx in opTypes) {
      var opType = "eventhook:operation"
      if (opTypes[opIdx]) opType = opType + ':' + opTypes[opIdx];
      console.log("Examing account: ", account, " for event: ", opType);

      eventhooks = getAccountURLs(account);
      console.log("Eventhooks: ", eventhooks);

      // Should use keyName as a glob "matchString" instead of an exact match?
      // Especially if we want to support multiple URLs for the same event type
      if (eventhooks && opType in eventhooks) { 
        foundEventURL = true;
        postURL = eventhooks[opType];
        //postURL = postURL + "?id=" + message.id;

        postBody = JSON.stringify({
          'id': message.id, 
          'network': {
            'domain': 'stellar.org', 
            'name':'TestNet', 
            'id': 'GADDR'
          }
        })
        kp = StellarSdk.Keypair.fromSecret('SBEREMKEHT6WDHHQVWAO62THX63AGSU2AIZLU6CXYUVJV3HKULNTCB34')
        //signature = kp.sign(postURL).toString('hex')
        signature = kp.sign(postBody).toString('hex')
        console.log("Calling URL: " + postURL);
        console.log("Sender: " + kp.publicKey());
        console.log("Body: " + JSON.stringify(postBody,null,2));
        console.log("Signature: " + signature);
        request(
          {
            headers: {
              'X-Request-Sender-Id': kp.publicKey(),
              'X-Request-Signature-ed25519-hex': signature,
              'Content-Type': 'application/json'
            },
            method: 'POST',
            uri: postURL,
            body: postBody
          }, function (error, response, body) {
            keepAlive();
            status_code = response && response.statusCode;
            console.log('error:', error);
            console.log('statusCode:', status_code);
            //console.log('body:', body);
 
            resp = {}
            resp['status_code'] = status_code
            resp['id'] = message.id + '_' + status_code + '_' + (new Date()).toJSON();
            resp['event_id'] = message.id;
            resp['response'] = response;
            resp['body'] = body;
            resp['error'] = error;

            //console.log('set acct response:', JSON.stringify(resp, null, 2));
            writeResponse(account, message.type, resp);

            if (status_code == 200) {
              console.log("Event delivered. " + message.id);
              deleteAccountQueuedEvent(account, message);
            }
            else if (status_code == 404) {
              console.log("404 request url: " + idKey);
            }
          });
      } else {
        console.log("Account does not have service for event: " + opType);
      }
    }
    if (!foundEventURL) {
      console.log("Account has no handlers for operation: " + message.type);
      deleteAccountQueuedEvent(account, message);
    }
}

function addAccountListener(a) {
  console.log("Adding Stellar observer", a, "at", (new Date()).toJSON());
  writeAccount(a);

  accountListeners[a] = stellarServer.payments()
    .forAccount(a)
    //.cursor(acct['cursor'])
    .stream({
      onmessage: function (message) {
        keepAlive();
        idKey = message.id
        short_hash = message.transaction_hash.slice(0,3) + '..' + message.transaction_hash.slice(-4)
        console.log("OPID", idKey, " TXNID:", short_hash," paging_token", message.paging_token);
        //console.log("message", message);

        var respDir = getEventResponsesDirectory(a, message.type, message.id);
        if (!fsExistsSync(respDir)) {
          console.log("Received new event: " + idKey);
          console.log("message", message);
          queueEvent(message);
          processEvent(message);
        }
      }
    });
}

function removeAccountListener(a) {
  console.log("Removing Stellar observer", a, "at", new Date());
  accountListeners[a]();
  delete(accountListeners[a]);
  keepAlive();
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
