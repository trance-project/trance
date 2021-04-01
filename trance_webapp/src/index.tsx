import React from 'react';
import ReactDOM from 'react-dom';
import {Provider} from 'react-redux';

import './static/fonts/EricaOne-Regular.ttf';
import './index.css';
import App from './App';
import store from './redux/store';



ReactDOM.render(
    /**
     * React strictMode is disable on Material UI V4 and is said to be fixed when V5 is released
     */
  // <React.StrictMode>
    <Provider store={store}>
        <App />
    </Provider>,
  // </React.StrictMode>,
  document.getElementById('root')
);

