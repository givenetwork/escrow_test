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
const glob = require('glob-fs')({ gitignore: true });

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

var msgsDir = '/msg_queue/';
var respDir = '/responses/';
var acctDir = 'accts/' 

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
  accountId = req.query.id;
  addAccountListener(accountId);
  res.json({ success: 1, status: "Account added " + accountId });
  keepAlive();
});

app.get('/accounts/remove',function(req,res) {
  accountId = req.query.id;
  removeAccountListener(accountId);
  res.json({ success: 1, status: "Account removed " + accountId });
  keepAlive();
});

app.get('/accounts',function(req,res) {
  res.json(_.keys(accountListeners));
  keepAlive();
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

function getAccountFilename(acctId) {
  return acctDir + '/' + acctId + '.json';
}

function getEventDirectory(type, msgId) {
  return type + respDir + msgId + '/';
}

function getMessageFilename(type, msgId) {
  return type + msgsDir + msgId;
}

function getEventQueueIDs(type) {
  var msgDir = type + msgsDir;
  var files = fs.readdirSync(msgDir);
  return files;
}

function getEventData(type, eventId) {
  var msgFile = getMessageFilename(type, eventId);
  var msg = JSON.parse(fs.readFileSync(msgFile, 'utf8'));
  return msg;
}

function isEventSent(type, eventId) {
  var eventDir = getEventDirectory(type, eventId);
  var respFiles = glob.readdirSync(eventDir + eventId + '_200_*', {});
  return (respFiles.length > 0);
}

function writeResponse(type, resp) {
  var eventDir = type + respDir + resp.event_id;

  console.log("Making Response Dir: " + eventDir);
  mkdirp(eventDir, function(err) {
    console.log("Writing Data: " + JSON.stringify(resp));
    if (err == null) { 
      var respFile = eventDir + '/' + resp.id;
      var json = JSON.stringify(resp, null, 2);
      fs.writeFileSync(respFile, json, 'utf8');
    }
  });
}

function deleteEvent(type, evt) {
  var msgFile = getMessageFilename(type, evt.id);
  console.log("Deleting: " + msgFile);
  fs.unlink(msgFile, function (err) { return; });
}

function writeEvent(type, evt) {
  //console.log("Making Message Dir: " + msgsDir);
  var msgDir = type + msgsDir;
  mkdirp(msgDir, function(err) {
    if (err == null) { 
      var msgFile = getMessageFilename(type, evt.id);
      if (!fs.existsSync(msgFile)) {
        console.log("Initializing event file: " + msgFile);
        //var json = JSON.stringify({cursor: 'now', responses: []});
        var json = JSON.stringify(evt, null, 2);
        fs.writeFileSync(msgFile, json, 'utf8');
      }
    }
  });
}

// For the managed data URLs
function writeAccount(a) {
  var acctFile = getAccountFilename(a);
  var cursor;
  if (fs.existsSync(acctFile)) {
    var acct = JSON.parse(fs.readFileSync(acctFile, 'utf8'));
    cursor = acct["cursor"];
    console.log("Loaded Acct: " + JSON.stringify(acct));
  }
  //I'm only doing this here to cache the account's webhook and cursor data
  //if there's an efficient way to track that data in the event hook; 
  console.log("Loading Account: " + a);
  stellarServer.loadAccount(a)
    .then(function(account) {
      //var acctDir = 'accts/' + a.split(/(....)/).filter(Boolean).join('/');
      console.log("Making Dir: " + acctDir);
      mkdirp(acctDir, function(err) {
        if (err == null) { 
          // Should rewrite this to only save the cursor and callback URLs
          account["cursor"] = cursor;
          json = JSON.stringify(account, null, 2);
          console.log("Writing Data: " + json);
          fs.writeFileSync(acctFile, json, 'utf8');
        }
      });
    })
    .catch(function(e) {
      console.error(e);
    });
}

function readEvents() {
  types = ['payments'];
  for (tidx in types) {
    var type = types[tidx];
    var eventids = getEventQueueIDs(type);
    console.log("Reprocessing messages for type: " + type + ' - ' + eventids.length + ' messages');
    for (eidx in eventids) {
      keepAlive();
      var msg = getEventData(type, eventids[eidx]);
      var idx = parseInt(eidx) + 1
      console.log("Event: [" + idx + "/" + eventids.length + "] " + eventids[eidx]);
      //Check backoff schedule here?
      if (isEventSent(type, msg.id)) {
        console.log("Message already delivered. " + msg.id);
        deleteEvent(type, msg);
      } else {
        console.log("Resubmitting failed event. " + msg.id);
        processEvent(type, msg);
        keepAlive();
        sleep(2000); // 5 seconds
      }
    }
  }
  // No KeepAlive() calls are made unless there are files processed
  setTimeout(readEvents, 5000);
}

function processEvent(type, message) {
          keepAlive();
          postURL = process.env.WEBHOOK_URL + "?id=" + message.id;
          console.log("Calling URL: " + postURL);
          request(postURL, function (error, response, body) {
            keepAlive();
            status_code = response && response.statusCode;
            console.log('error:', error);
            console.log('statusCode:', status_code);
            console.log('body:', body);
 
            resp = {}
            resp['status_code'] = status_code
            resp['id'] = message.id + '_' + status_code + '_' + (new Date()).toJSON();
            resp['event_id'] = message.id;
            resp['response'] = response;
            resp['body'] = body;
            resp['error'] = error;

            console.log('set acct response:', JSON.stringify(resp));
            writeResponse(type, resp);

            if (status_code == 200) {
              console.log("Message delivered. " + message.id);
              deleteEvent(type, message);
            }
            else if (status_code == 404) {
              console.log("404 request url: " + idKey);
            }
          });
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

        var type = 'payments';
        var eventDir = getEventDirectory(type, message.id);
        if (!fs.existsSync(eventDir)) {
          console.log("Received new event: " + idKey + "\n" + message);
          console.log("message", message);
          writeEvent(type, message);
          processEvent(type, message);
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

