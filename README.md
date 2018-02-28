# GIVE ESCROW TEST

## SETUP

1. Set the environment variables
```
$ cp .env.example .env
```

2. Install dependencies
```
$ yarn/npm install
```

3. Set any accounts you want to listen to by default in server.js

4. Start the Server
```
$ yarn/npm start
```

## HOOKS

```
localhost:3000/accounts/add?id=STELLARACCOUNTID - will add the account
localhost:3000/accounts/remove?id=STELLARACCOUNTID - will add the account listener
localhost:3000/accounts - lists the current account ID's it's listening to
```
