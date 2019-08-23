'use strict';

// Loading required libraries and config file
const cassandra = require('cassandra-driver');
var config = require('../../config/config.json');
//var winston = require('winston');
// TJH was level: 'error'
const { createLogger, format, transports } = require('winston');
var logger = createLogger({ 
    format: format.combine(
        format.splat(),
        format.simple()
      ),
    transports: [
        new (transports.Console)({ 'timestamp': true, level: 'info' })
    ],
    exitOnError: false
});

const cassandraHosts = config.garage.backend.cassandraHosts;
// Connection to Cassandra
const client = new cassandra.Client({ contactPoints: cassandraHosts, keyspace: config.garage.backend.cassandraKeyspace });

// Reading constants from config file
const IS_LIVE = config.garage.isLive;
// TJH added - optionally multiply x, y by -1 before long/lat conversion
const X_MULT = config.garage.coordMult.x
const Y_MULT = config.garage.coordMult.y
const CLI_SEND_PERIOD_IN_MS = config.garage.backend.webSocketSendPeriodInMs; 
const SENSOR_TYPE=config.garage.backend.sensorType;
const ORIGIN_LAT = config.garage.backend.originLat;
const ORIGIN_LON = config.garage.backend.originLon;

var spotSet;

/**  Calculates latitude of object*/
function getObjectLat(objectY) {
    var objectLat = ORIGIN_LAT - (360 * objectY * 0.001 / 40000)
    return objectLat
}

/** Calculates longitude of object */
function getObjectLon(objectX, objectLat) {
    var objectLon = ORIGIN_LON - ((360 * objectX * 0.001) / (40000 * Math.cos((ORIGIN_LAT + objectLat) * Math.PI / 360)))
    return objectLon
}

/** Returns the required attributes of an object in a particular format. */
function getFormattedObject(row, garageId, garageLevel) {
    var objectX = row.object.centroid.x * X_MULT
    var objectY = row.object.centroid.y * Y_MULT
    var objectLat = getObjectLat(objectY)
    var objectLon = getObjectLon(objectX, objectLat)
    var objId = row.object.id;
    var obj = {
        timestamp: row.timestamp,
        garageId: garageId,
        garageLevel: garageLevel,
        id: objId,
        trackerid: row.object.trackerid,
        classid: row.object.classid,
        classdesc: row.object.classdesc,
        orientation: row.object.orientation,
        sensorType: row.sensor.type,
        eventType: row.event.type,
        removed: 0,
        x: objectX,
        y: objectY,
        lat: objectLat,
        lon: objectLon
    }
    return obj
}


function getFormattedCarObject(row, state, garageLevel) {
    var removed = (row.event.type === 'exit' || row.event.type === 'empty') ? 1 : 0;
    var objectX = row.object.coordinate.x * X_MULT
    var objectY = row.object.coordinate.y * Y_MULT
    var objectLat = getObjectLat(objectY)
    var objectLon = getObjectLon(objectX, objectLat)
    var objId = row.object.id;
    var parkingSpot = ""
    if (state === "parked" || state === "empty") {
        parkingSpot = row['place']['parkingspot']['id']
    }
    var car = {
        timestamp: row.timestamp,
        color: row.object.vehicle.color,
        garageLevel: garageLevel,
        id: objId,
        licensePlate: row.object.vehicle.license,
        licenseState: row.object.vehicle.licensestate,
        orientation: row.object.orientation,
        parkingSpot: parkingSpot,
        sensorType: row.sensor.type,
        state: state,
        eventType: row.event.type,
        removed: removed,
        type: row.object.vehicle.type,
        x: objectX,
        y: objectY,
        lat: objectLat,
        lon: objectLon
    }
    return car
}

/** A generic function which receives a queryObject and uses it to obtain results from cassandra. The queryObject consists of the query and the params. */
function getResultsFromCassandra(queryObject){
    return new Promise(function (resolve, reject) {
        client.execute(queryObject['query'], queryObject['params'],{ prepare: true })
        .then(result => {
            resolve(result.rows);
        }).catch(error => {
            reject(error);
        });
    });
}

/** Generates the first cassandra parking spot query for live system */
function getParkingStateQuery(garageId,garageLevel){
    let spotStateQueryObject = {}
    spotStateQueryObject['query'] = "select timestamp, place, sensor, object, event from parkingSpotState where garageid=? and level=?";
    spotStateQueryObject['params'] = [garageId, garageLevel];
    return spotStateQueryObject;
}

/** Generates the first cassandra parking spot query for playback system */
function getParkingSpotPlaybackQueries(sensorType,garageId,garageLevel,currentTimestamp,startTimestamp){
    var queryList = []
    for (let spot of spotSet) {
        let parkingSpotQueryObject = {}
        if (IS_LIVE && config.garage.live.apis.uiDelaySeconds!==0) {
            parkingSpotQueryObject['query'] = "select timestamp, spotid, place, sensor, object, event from parkingSpotPlayback where garageid=:garageId and level=:garageLevel and sensortype=:sensorType and spotid=:spotId and timestamp <= :ts limit 1";
            parkingSpotQueryObject['params'] = [garageId, garageLevel, sensorType, spot, currentTimestamp];
        }else if(!IS_LIVE){
            parkingSpotQueryObject['query'] = "select timestamp, spotid, place, sensor, object, event from parkingSpotPlayback where garageid=:garageId and level=:garageLevel and sensortype=:sensorType and spotid=:spotId and timestamp >= :startTs and timestamp <= :currentTs limit 1";
            parkingSpotQueryObject['params'] = [garageId, garageLevel, sensorType, spot, startTimestamp, currentTimestamp];
        }
        queryList.push(parkingSpotQueryObject);
    }
    return(queryList);
}

/** Generates subsequent cassandra parking spot queries for live/playback system */
function getParkingSpotDeltaQuery(sensorType,garageId,garageLevel,previousTimestamp,currentTimestamp){
    let parkingSpotQueryObject = {}
    parkingSpotQueryObject['query'] = "select timestamp, place, sensor, object, event from parkingSpotDelta where garageid=? and level=? and sensortype=? and timestamp > ? and timestamp <= ?";
    parkingSpotQueryObject['params'] = [garageId, garageLevel, sensorType, previousTimestamp, currentTimestamp];
    return parkingSpotQueryObject;
}

/** Gets the resolved results and formats the car objects */
function getFormattedParkingResults(parkingResult,garageLevel){
    if (parkingResult['event']['type'] === 'parked') {
        var state = "parked"
        return(getFormattedCarObject(parkingResult, state, garageLevel));
    } else if (parkingResult['event']['type'] === 'empty') {
        var state = "empty"
        return(getFormattedCarObject(parkingResult, state, garageLevel));
    }
}

/** Used to decide which kind of parking spot query needs to be made */
function getParkingSpotResults(isDeltaParkingSpotQuery, garageId, garageLevel, previousTimestamp, currentTimestamp, startTimestamp) {
    return new Promise(function (resolve, reject) {
        if (!isDeltaParkingSpotQuery) {
            if (IS_LIVE && config.garage.live.apis.uiDelaySeconds===0) {
                let spotStateQueryObject=getParkingStateQuery(garageId,garageLevel);
                getResultsFromCassandra(spotStateQueryObject).then(spotStateList=>{
                    let formattedResults=spotStateList.map(parkingResult => getFormattedParkingResults(parkingResult,garageLevel));
                    resolve(formattedResults);
                }).catch(error=>{
                    reject(error);
                })
            }else{
                let queryObjectList=getParkingSpotPlaybackQueries(SENSOR_TYPE,garageId,garageLevel,currentTimestamp, startTimestamp);
                let playbackQueryList=queryObjectList.map(getResultsFromCassandra);
                Promise.all(playbackQueryList).then(playbackResultList=>{
                    let playbackResults=[].concat(...playbackResultList);
                    let formattedResults=playbackResults.map(parkingResult => getFormattedParkingResults(parkingResult,garageLevel));
                    resolve(formattedResults);
                }).catch(error=>{
                    reject(error);
                })
            }
        }else{
            let queryObject=getParkingSpotDeltaQuery(SENSOR_TYPE,garageId,garageLevel,previousTimestamp,currentTimestamp);
            getResultsFromCassandra(queryObject).then(deltaResults=>{
                let formattedResults=deltaResults.map(parkingResult => getFormattedParkingResults(parkingResult,garageLevel));
                resolve(formattedResults);
            }).catch(error=>{
                reject(error);
            });
        }
    });
}

/** Generates the cassandra aisle query for live/playback system  
 * and formats the results
*/
function getAisleResults(garageId, garageLevel, previousTimestamp, currentTimestamp) {
    return new Promise(function (resolve, reject) {
        let aisleQueryObject={}
        aisleQueryObject["query"]= "select messageid, timestamp, place, sensor, object, event from aisle where messageid=? and timestamp > ? and timestamp <= ?";
        aisleQueryObject["params"]=[garageId + "-" + garageLevel, previousTimestamp, currentTimestamp]
        getResultsFromCassandra(aisleQueryObject).then(aisleResultList=>{
            let formattedAisleResultList=[];
            for(let aisleResult of aisleResultList){
                var state = "moving";
                var car = getFormattedCarObject(aisleResult, state, garageLevel);
                formattedAisleResultList.push(car);
            }
            resolve(formattedAisleResultList);
        }).catch(error=>{
            reject(error);
        });
    });
}

/** Generates the cassandra detected object query for live/playback system  
 * and formats the results
*/
function getObjResults(garageId, garageLevel, previousTimestamp, currentTimestamp) {
    return new Promise(function (resolve, reject) {
        let detqueryObject={}
        detqueryObject["query"]= "select messageid, timestamp, place, sensor, object, event, zone, videopath, analyticsmodule from objectmarker where messageid=? and timestamp > ? and timestamp <= ?";
        detqueryObject["params"]=[garageId + "-" + garageLevel, previousTimestamp, currentTimestamp]
        getResultsFromCassandra(detqueryObject).then(detResultList=>{
            let formatteddetResultList=[];
            for(let detResult of detResultList){

                var obj = getFormattedObject(detResult, garageId, garageLevel);
                formatteddetResultList.push(obj);
            }
            resolve(formatteddetResultList);
        }).catch(error=>{
            reject(error);
        });
    });
}


/** Used to generate cassandra query results asynchronously */
function getResults(isDeltaParkingSpotQuery, garageId, garageLevel, previousTimestamp, currentTimestamp, startTimestamp) {
    return new Promise(function (resolve, reject) {
        let taskList=[]
        //taskList.push(getParkingSpotResults(isDeltaParkingSpotQuery, garageId, garageLevel, previousTimestamp, currentTimestamp, startTimestamp));
        taskList.push(getObjResults(garageId, garageLevel, previousTimestamp, currentTimestamp));
        Promise.all(taskList).then(results => {
            resolve({
                // parkingSpotResults: results[0],
                objResults: results[0]
            });
        }).catch(error => {
            reject(error);
        });
    });
}


/**Set the object dictionary for current read to latest timestamp of each obj*/
function getObjDict(ObjResults) {
    var ObjDict = {}
    for (let i = 0; i < ObjResults.length; i++) {
        let Obj = ObjResults[i];
        let eventTimestamp=Obj.timestamp;
        if (ObjDict.hasOwnProperty(Obj['id'])) {
            if (new Date(eventTimestamp) >= ObjDict[Obj['id']]['timestamp']) {
                ObjDict[Obj['id']] = { timestamp: new Date(eventTimestamp), object: Obj }
            }
        } else {
            ObjDict[Obj['id']] = { timestamp: new Date(eventTimestamp), object: Obj }
        }
    }
    return (ObjDict);
}

/** Used to maintain state of objects so that they can be retired 
 *  after a certain amount of time 
 *  Updates object state based on current read 
 */
function updateObjState(ObjResults, ObjState){
    for (var i = 0; i < ObjResults.length; i++) {
        var obj = ObjResults[i];
        var eventTimestamp = obj.timestamp;
        if (ObjState.hasOwnProperty(obj['id'])) {
            if (new Date(eventTimestamp) >= ObjState[obj['id']]['timestamp']) {
                ObjState[obj['id']] = { timestamp: new Date(eventTimestamp), object: obj }
            }
        } else {
            ObjState[obj['id']] = { timestamp: new Date(eventTimestamp), object: obj }
        }
    }
}

/** Objects persisting for certain amount of time are retired
 *  using this function.
 */
function retireObjects(ObjDict, ObjState, currentTimestamp) {
    for (const key of Object.keys(ObjState)) {
        let lastTimestampOfObj = ObjState[key]['timestamp'];
        if (currentTimestamp - lastTimestampOfObj >= config.garage.backend.carRemovalPeriodInMs) {
            let obj = ObjState[key]['object'];
            obj['removed'] = 1;
            ObjDict[obj['id']] = { timestamp: currentTimestamp, object: obj }
            delete ObjState[key]
        }
    }
}

/** converts Objdict into a single list after post processing */
function getResultList(ObjDict) {
    let resultList=[];
    for (const key of Object.keys(ObjDict)) {
        if (ObjDict.hasOwnProperty(key)) {
            resultList.push(ObjDict[key]["object"]);
        }
    }
    return (resultList);
}

/** Objects in the list are sorted on the basis of event timestamp */
function sortByEventTimestamp(resultList) {   
    resultList.sort(function (a, b) {
        return a.timestamp - b.timestamp;
    });
    return (resultList);
}

/** Used to send the processed message to web socket client at regular intervals of time */
function sendMessage(ws, startTimestamp, currentTimestamp, previousTimestamp, isDeltaParkingSpotQuery, garageId, garageLevel, ObjState) {
    return new Promise(function (resolve, reject) {
        var results = {
            timestamp: currentTimestamp.toISOString(),
            objects: new Array(),
            markers: new Array()
        }
        getResults(isDeltaParkingSpotQuery, garageId, garageLevel, previousTimestamp, currentTimestamp, startTimestamp).then(result => {
            //let parkingSpotResults = result.parkingSpotResults
            //logger.info("Receive Results: %s: %s ", currentTimestamp.toISOString(), JSON.stringify(result));
            let ObjResults = result.objResults
            var ObjDict = getObjDict(ObjResults);   // create dict of current results - this is passed to UI
            updateObjState(ObjResults, ObjState);   // maintain latest timestamp of object over time to enable removal
            retireObjects(ObjDict, ObjState, currentTimestamp);  // remove objects persisting beyond a threshold and mark as removed in Objdict
            let resultList = getResultList(ObjDict);       
            let sortedObjects = sortByEventTimestamp(resultList);

            results["objects"] = sortedObjects;

            var msgToDisplay = JSON.stringify(results)
            if (msgToDisplay.length > 2000) {
                msgToDisplay = msgToDisplay.substring(0, 1996) + " ..."
            }
            logger.info("Current timestamp: %s", currentTimestamp.toISOString())
            logger.debug("Sending msg: %s ", msgToDisplay);
            var msgToSend = JSON.stringify({ metadata: { timestamp: currentTimestamp.toISOString(), garageLevel: garageLevel }, data: results });
            ws.send(msgToSend, function ack(error) {
                if (error) {
                    //logger.info("[SEND UPDATES] Unable to send message: %s: %s . Closing connection", currentTimestamp, msgToDisplay)
                    reject({ error: error, msgToDisplay: msgToDisplay });
                }
                resolve({ error: null, msgToDisplay: msgToDisplay });
            });
        }).catch(error => {
            // logger.info("[SEND UPDATES] Unable to send message: %s: %s . Closing connection", currentTimestamp, msgToDisplay)
            reject({ error: error, msgToDisplay: null });
        });
    });
}

module.exports = {
    /** Initializes the set of spots. This is done when the server starts and reads the parking spot config file. */
    init: function (spots) {
        spotSet =spots
    },
    /** Uses the startTimestamp, garageId and garageLevel sent by websocket client to send results to the client at regular intervals of time 
     *  Initiated when 'M' symbol clicked on in browser or zoom in to sufficient level on location
    */
    sendUpdates: function (ws, startTimestamp, garageId, garageLevel) {
        try {
            var timestampInMs = Date.parse(startTimestamp)
            if (isNaN(timestampInMs) == false) {
                startTimestamp = new Date(startTimestamp);
                let timeDifference= new Date()-startTimestamp;  //diff between current time and starttimestamp from client in ms
                let currentTimestamp=new Date(new Date()-timeDifference);  //almost same as startTimestamp
                let previousTimestamp=new Date(startTimestamp-CLI_SEND_PERIOD_IN_MS);
                var isDeltaParkingSpotQuery = false  //not used but keep as future placeholder
                var ObjState = {}
                var intObj = setInterval(function () {
                    sendMessage(ws, startTimestamp, currentTimestamp, previousTimestamp, isDeltaParkingSpotQuery, garageId, garageLevel, ObjState).then(result => {
                        isDeltaParkingSpotQuery = true
                        previousTimestamp=new Date(currentTimestamp.toISOString());  // toISOString converts to UTC string
                        currentTimestamp=new Date(new Date()-timeDifference);        // fn called every CLI_SEND_PERIOD_IN_MS so effectively increments by this amt + fn exec time
                    }).catch(errorObject => {
                        console.error(errorObject.error)
                        logger.info("[SEND UPDATES] Unable to send message: %s: %s . Closing connection", currentTimestamp, errorObject.msgToDisplay)
                        clearInterval(intObj);
                    });
                }, CLI_SEND_PERIOD_IN_MS);
                return intObj;
            } else {
                logger.error('[SERVER ERROR] Invalid Timestamp');
                ws.send(JSON.stringify({ error: " Invalid Timestamp" }), function ack(err) {
                    if (err) {
                        logger.info("[SEND UPDATES] Unable to send error message to client that error has occurred: %s . Closing connection", JSON.stringify({ error: err }))
                    }
                });
            }
        } catch (e) {
            logger.error('[SERVER ERROR] Error: %s', e);
            ws.send(JSON.stringify({ error: e.toString() }), function ack(err) {
                if (err) {
                    logger.info("[SEND UPDATES] Unable to send error message to client that error has occurred: %s . Closing connection", JSON.stringify({ error: err }))
                }
            });
        }
    }
}