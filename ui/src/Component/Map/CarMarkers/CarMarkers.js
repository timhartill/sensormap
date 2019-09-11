import React, { Component } from 'react';
import moment from 'moment';  //TJH Added - calc start timestamp in this module vs smartgaragepage to avoid delay bug

import CarMarker from './CarMarker/CarMarker';

const google = window.google;

/**
 * CarMarkers.js manages and updates each object marker, 
 * including its status and its marker’s style. 
 * All object markers are located one layer above the ground map.
 * 
 * Use WebSocket to receive and send messages to update the status, 
 * of each obj marker. 
 */
class CarMarkers extends Component {
    state = {
        objects: {}
    }

    componentDidMount() {
        /* if there is a websocket url. */
        if (this.props.websocket.url !== '' || this.props.websocket.url !== undefined) {
            /* if browser supports web worker. */
            if (window.Worker) {
                /* use web worker to finish websocket call and generate data. */
                this.myWorker = new Worker('/MarkersWorker.js');

                //console.log('carmarkers.js componentDidMount config:', this.props.config)
                /* reflect ui delay */
                let delayTimestamp;

                if (this.props.config.isLive) {
                    delayTimestamp = moment().subtract(this.props.config.live.apis.uiDelaySeconds, 's').utc().format();
                    //console.log('delayTimestamp: ', delayTimestamp)
                }
                else {
                    delayTimestamp = moment.utc(this.props.config.playback.apis.startTimestamp).subtract(this.props.config.playback.apis.uiDelaySeconds, 's').utc().format();
                }
                
                let socketRequest;
                socketRequest = JSON.stringify({
                    startTimestamp: delayTimestamp,    //TJH changed from this.props.websocket.startTimestamp,
                    garageLevel: this.props.websocket.garageLevel,
                    garageId: this.props.websocket.garageId
                });
                //console.log('carmarkers.js componentDidMount starttimestamp:', delayTimestamp)

                this.myWorker.postMessage([this.props.websocket.url, socketRequest, window.location.hash]);
                this.myWorker.onmessage = (m) => {
                    this.setState({ objects: m.data });
                }
            }
        }
    }

    shouldComponentUpdate(nextProps, nextState) {
        /* only update when zoom/garageLevel/bounds/markers (moving/add/remove) change */
        return this.props.zoom !== nextProps.zoom || this.props.websocket.garageLevel !== nextProps.websocket.garageLevel || (this.props.bounds !== null && !this.props.bounds.equals(nextProps.bounds)) || this.state.objects !== nextState.objects;
    }

    componentDidUpdate(prevProps, prevState) {
        /* if there is a websocket url or zoom changed, and location changed */
        if ((this.props.websocket.url !== '' || this.props.websocket.url !== undefined || this.props.zoom !== prevProps.zoom) && this.props.websocket.garageLevel !== prevProps.websocket.garageLevel) {
            this.setState({
                objects: {}
            });

            let socketRequest;
            socketRequest = JSON.stringify({
                startTimestamp: this.props.websocket.startTimestamp,
                garageLevel: this.props.websocket.garageLevel,
                garageId: this.props.websocket.garageId
            });
            //console.log('carmarkers.js componentDidUpdate starttimestamp:', this.props.websocket.startTimestamp)

            this.myWorker.postMessage([this.props.websocket.url, socketRequest, window.location.hash]);
            this.myWorker.onmessage = (m) => {
                this.setState({ objects: m.data });
            }
        }
    }

    componentWillUnmount() {
        this.myWorker.terminate();
    }

    render() {

        let objmarkers = [];
        let isOpen = true;   //false;
        Object.entries(this.state.objects).forEach(([key, value]) => {
            //isOpen = value.state === 'moving';
            if (this.props.bounds === null || this.props.bounds.contains(new google.maps.LatLng(value.lat, value.lon))) {
                objmarkers.push(
                    <CarMarker
                        key={key}
                        obj={value}
                        clearPlate={this.props.clearPlate}
                        isOpen={isOpen}
                        zoom={this.props.zoom}
                        config={this.props.config}
                        objectClasses={this.props.objectClasses}
                    />
                );
            }
        });
        return objmarkers;
    }
}

export default CarMarkers;