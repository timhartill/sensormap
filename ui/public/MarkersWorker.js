importScripts('https://cdnjs.cloudflare.com/ajax/libs/socket.io/2.1.0/socket.io.js');

let backendSocket;

/**
 * Objects are managed and updated in a dictionary.
 * Latest messages are sent through the WebSocket. 
 */
onmessage = (e) => {
    let objs = {
        detected: {}
    };

    //let anomalyCars;
    //let msgcount = 0

    if (e.data[2].endsWith('#/garage')) {
        if (backendSocket !== undefined) {
            backendSocket.close();
        }

        backendSocket = new WebSocket(e.data[0]);
        backendSocket.onopen = (event) => {
            backendSocket.send(e.data[1]);
        };

        backendSocket.onmessage = (event) => {
            let streamData = JSON.parse(event.data);
            let newObjs = streamData.data.objects.map((obj) => {
                //if (car.state !== 'moving' && car.sensorType === 'Camera') {
                //    msgcount += 1
                //};
                return obj;
            });

            if (newObjs.length !== 0) {
                /* this is not the first incoming data */
                let savedObjs = {detected: {}}  //{ parked: {}, moving: {} };
                savedObjs.detected = Object.assign({}, objs.detected);
                /* iterate through new coming data */
                for (let i = 0; i < newObjs.length; i++) {

                    let stateID;

                    stateID = newObjs[i].id;
                    if (newObjs[i].removed == 0) {
                        savedObjs.detected[stateID.toString()] = newObjs[i];
                    } else {
                        if (savedObjs.detected.hasOwnProperty(stateID.toString())) {
                            delete savedObjs.detected[stateID.toString()];
                        }
                    }
                }
                objs = savedObjs;

                /* loop through savedObjs to get updateObjs object which needs to be passed back */
                let updateObjs = {};

                Object.entries(savedObjs.detected).forEach(([key, value]) => {
                    updateObjs[key] = value;
                });

                postMessage(updateObjs);

            }
        }

        backendSocket.onerror = (event) => {
            console.log('[WEBSOCKET ERR]');
        }

        backendSocket.onclose = (event) => {
            console.log('[WEBSOCKET CLOSE]');
        }

    }
};


