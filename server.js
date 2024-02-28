// LIB
const port = process.env.PORT || 3010;
const express = require('express');
const bodyparser = require('body-parser');
const nodeFetch = require('node-fetch')
const fetch = require('fetch-cookie')(nodeFetch);
const routes = require('./route');
const app = express();
const server = app.listen(3010);
server.keepAliveTimeout = 61 * 1000;
const morgan = require('morgan')
const logger = require('./logger');
const pjson = require("./package.json");
//const expressWinston=require('express-winston');

// SSL
process.env['NODE_TLS_REJECT_UNAUTHORIZED'] = 0


app.use(morgan('combined'));
  
// ROUTES
app.use(bodyparser.json());
app.use((req, res, next) => {
    res.set('Access-Control-Allow-Origin', '*')
    res.header("Access-Control-Allow-Methods", "GET, PUT, POST, DELETE, OPTIONS");
    res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type")
    next()
})
routes(app);

//app.listen(port);
logger.info(`Payment -> Mobopay running → PORT ${server.address().port}`);
