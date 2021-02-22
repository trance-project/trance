import React from 'react';
import {
    BrowserRouter,
    Switch,
    Route
} from "react-router-dom";

import Overview from "./component/Overview/Overview";
import QueryBuilder from "./containers/QueryBuilder/QueryBuilder";

import Layout from "./hoc/Layout/Layout";

import './App.css';

function App() {
  return (
      <BrowserRouter>
          <div className="App">
              <Layout>
                  <Switch>
                      <Route path={"/reports"}>
                          <h1> Reports to be constructed</h1>
                      </Route>
                      <Route path={"/tables"}>
                          <h1>Tables to be constructed</h1>
                      </Route>
                      <Route path={"/builder"}>
                            <QueryBuilder/>
                      </Route>
                      <Route path={"/"}>
                          <Overview/>
                      </Route>
                  </Switch>
              </Layout>
          </div>
      </BrowserRouter>
  );
}

export default App;
