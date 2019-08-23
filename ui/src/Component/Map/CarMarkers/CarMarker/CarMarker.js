import React, { Component } from 'react';
import { Marker, InfoWindow } from 'react-google-maps';
//import Moment from 'moment';

import classes from './CarMarker.css';
import green from '../../../../assets/green.png';
import blue from '../../../../assets/blue.png';
import red from '../../../../assets/red.png';
import pink from '../../../../assets/pink.png';
import mustard from '../../../../assets/mustard.png';
import black from '../../../../assets/black.png';


/**
 * Add the information dialog box to each parked/moving obj marker.
 * After click on the obj marker, the dialog box will pop up and 
 * disappear after `dialogAutoRefreshIntervalSeconds` secs and it's 
 * 5 secs by default; 
 * 
 * Configure the color and the size of the marker. To change the
 * color of the markers, replace the image file `green.png` or `blue.png`
 * with a new file showing different colored circle image;
 * 
 * Configurable variable: `dialogAutoRefreshIntervalSeconds`
 */
class CarMarker extends Component {
    state = {
        isOpen: false,
        touched: false,
        dialogAutoRefreshIntervalSeconds: this.props.config.isLive ? this.props.config.live.webSocket.dialogAutoCloseSeconds : this.props.config.playback.webSocket.dialogAutoCloseSeconds
    }

    /* toggle to open/close info window */
    onToggleHandler = () => {
        this.setState({ isOpen: !this.state.isOpen, touched: true });
        if (this.props.clearPlate !== undefined) {
            this.props.clearPlate();
        }
        if (this.props.jump !== undefined) {
            this.props.jump(this.props.index);
            this.autoCloseHandler();
        }
    }

    /* close info window after x=dialogAutoRefreshIntervalSeconds seconds, 
     * and the default setting is 5 secs */
    autoCloseHandler = () => {
        this.timeout = setTimeout(() => {
            if (this.marker) {
                this.setState({ isOpen: false });
            }
        }, this.state.dialogAutoRefreshIntervalSeconds * 1000);
    }

    componentDidMount() {
        if (this.props.isOpen) {
            this.setState({ isOpen: true });
        }
    }

    componentWillReceiveProps(newProps) {
        /* if props.isOpen true and this marker is not touched, open info window */
        if (newProps.isOpen && !this.state.touched) {
            this.setState({ isOpen: newProps.isOpen });
            /* this is not a moving obj, start auto close timer */
            //if (newProps.obj.state !== 'moving') {
            //    this.autoCloseHandler();
            //}
        }
        /* if props.isOpen false and marker is touched, set touched to false */
        else if (!newProps.isOpen && this.state.touched) {
            this.setState({ touched: false });
        }
        /* if obj.state changes from moving to parked, start auto close timer */
        //else if (!newProps.isOpen && this.state.isOpen && !this.state.touched && this.props.obj.state === 'moving' && newProps.obj.state === 'parked') {
        //    this.autoCloseHandler();
        //}
    }

    shouldComponentUpdate(nextProps, nextState) {
        /* update component when obj is moving and lat/lng changed, or isOpen changed, or map zoom changed */
        return (this.props.obj.lat !== nextProps.obj.lat || this.props.obj.lon !== nextProps.obj.lon) || this.state.isOpen !== nextState.isOpen || this.props.zoom !== nextProps.zoom;
    }

    componentDidUpdate() {
        //if (this.props.obj.state !== 'moving') {
        //    this.autoCloseHandler();
        //}
    }

    componentWillUnmount() {
        clearTimeout(this.timeout);
    }

    render() {
        let icon, info, marker, scaler;
        /* decide the size and the color of obj icon shown on map */
        
        if (this.props.obj !== undefined) {
            /* decide the colour and scale based on class of object */
            switch (this.props.obj.classid) {
                case '0': // other
                    marker = black;
                    scaler = 1
                    break;
                case '1': // person
                    marker = green;
                    scaler = 1
                    break;
                case '2': // car
                    marker = red;
                    scaler = 2
                    break;
                case '3': // other vehicle type
                    marker = blue;
                    scaler = 2
                    break;
                case '4': // object
                    marker = mustard;
                    scaler = 1
                    break;
                case '5': // bicycle
                    marker = pink;
                    scaler = 1
                    break; 
                default:
                    marker = black;
                    scaler = 1
                    break;                          
            }
            info = this.props.obj.trackerid;
            /* for any obj, show dot in appropriate size*/
            switch (this.props.zoom) {
                case 19:
                    icon = {
                        url: marker,
                        scaledSize: { width: scaler*12, height: scaler*12 },
                        anchor: { x: scaler*6, y: scaler*6 }
                    };
                    break;
                case 20:
                    icon = {
                        url: marker,
                        scaledSize: { width: scaler*20, height: scaler*20 },
                        anchor: { x: scaler*10, y: scaler*10 }
                    };
                    break;
                case 21:
                    icon = {
                        url: marker,
                        scaledSize: { width: scaler*36, height: scaler*36 },
                        anchor: { x: scaler*18, y: scaler*18 }
                    };
                    break;
                default:
                    icon = {
                        url: marker,
                        scaledSize: { width: scaler*12, height: scaler*12 },
                        anchor: { x: scaler*6, y: scaler*6 }
                    };
                    break;
            }
        }

        return (
            <Marker
                ref={(ref) => { this.marker = ref; }}
                position={{ lat: this.props.obj.lat, lng: this.props.obj.lon }}
                onClick={this.onToggleHandler}
                icon={icon}
            >
                {this.state.isOpen ? (
                    <InfoWindow onCloseClick={this.onToggleHandler}>
                        <span className={classes.info}>{info}</span>
                    </InfoWindow>
                ) : ''}
            </Marker>
        );
    }
}

export default CarMarker;