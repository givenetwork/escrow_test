require('dotenv').config()

var _ = require('lodash');
const request = require('request')
const express = require('express');
const app = express();
const bodyParser = require("body-parser");
const path = require('path');
const StellarSdk = require('stellar-sdk');

const stellarServer = new StellarSdk.Server(process.env.STELLAR_SERVER_URL);

const posix = require('posix');
posix.setrlimit('nofile', { soft: 10000 });

app.use(bodyParser.urlencoded({ extended: false }));
app.use(bodyParser.json());

//
// Local Cache
//
var accountListeners = []
const accounts = [
  "GDXS57IS3JLYHRZT7NCQDEZABZGCBQN6J2JGUHEDFUEQDSIWDPXUAKB5"
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
});

app.get('/accounts/remove',function(req,res) {
  accountId = req.query.id;
  removeAccountListener(accountId);
  res.json({ success: 1, status: "Account removed " + accountId });
});

app.get('/accounts',function(req,res) {
  res.json(_.keys(accountListeners));
});

async function main() {
  startListeners();
}

//
// Getters and Helpers
//

function startListeners() {
  _.forEach(accounts, function(a) {
      addAccountListener(a);
  });

}


function addAccountListener(a) {
  console.log("Adding Stellar observer", a, "at", new Date());
  accountListeners[a] = stellarServer.transactions()
    .forAccount(a)
    .stream({
      onmessage: function (message) {
        console.log("TXID", message.id);
        request(process.env.WEBHOOK_URL + "?id=" + message.id, function (error, response, body) {
          console.log('error:', error);
          console.log('statusCode:', response && response.statusCode);
          console.log('body:', body);
        });
      }
    })
}

function removeAccountListener(a) {
  console.log("Removing Stellar observer", a, "at", new Date());
  accountListeners[a]();
  delete(accountListeners[a]);
}
