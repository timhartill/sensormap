import React from 'react';
import ReactDOM from 'react-dom';
import './index.css';
import App from './App';
import { unregister } from './registerServiceWorker';
import { HashRouter } from 'react-router-dom';

//const googletemp = window.google
/**
 * This file, index.js, is the entry point of React App. 
 */

/*
function getGoogleMaps() {
    // Return a promise for the Google Maps API
    return new Promise((resolve) => {
        // Add a global handler for when the API finishes loading
        window.resolveGoogleMapsPromise = () => {
            // Resolve the promise            
            resolve(googletemp);
            // Tidy up
            delete window.resolveGoogleMapsPromise;
        };

        // Load the Google Maps API with a callback to resolveGoogleMapsPromise
        const script = document.createElement("script");
        const API = window.appConfig.GOOGLE_MAP_API_KEY;
        script.src = `https://maps.googleapis.com/maps/api/js?key=${API}&callback=resolveGoogleMapsPromise&v=3.exp&libraries=geometry,drawing,places,visualization`;
        script.async = true;
        document.body.appendChild(script);
    });
}

getGoogleMaps()    
*/

ReactDOM.render((
    <HashRouter>
        <App />
    </HashRouter>
), document.getElementById('root'));

unregister();
