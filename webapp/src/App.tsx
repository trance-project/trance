import React from 'react';
import {
    BrowserRouter,
    Switch,
    Route
} from "react-router-dom";

import Dashboard from './template/Dashboard';
import Builders from './template/Builders';
import Reports from './template/Builders';

import './App.css';

function App() {
  return (
      <BrowserRouter>
          <div className="App">
              <Switch>
                  <Route path={"/reports"}>
                      <Reports/>
                  </Route>
                  <Route path={"/tables"}>
                      <Dashboard/>
                  </Route>
                  <Route path={"/builder"}>
                      <Builders/>
                  </Route>
                  <Route path={"/"}>
                      <Dashboard/>
                  </Route>
              </Switch>
          </div>
      </BrowserRouter>
  );
}

export default App;
