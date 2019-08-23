'use strict';
/** server takes on value of ./initializers/server.js.module.exports ie  start */
var server = require('./initializers/server');
const { createLogger, format, transports } = require('winston');
var logger = createLogger({ 
  format: format.combine(
    format.splat(),
    format.simple()
  ),
  transports: [
    new (transports.Console)({ 'timestamp': true })
  ],
  exitOnError: false
});

logger.info('[APP] Starting server initialization');

// Initialize the server
// server = start()
server(function(err){
  if (err) {
    logger.error('[APP] initialization failed', err);
  } else {
    logger.info('[APP] initialized SUCCESSFULLY');
  }
})
