import React from 'react';
import {
    BrowserRouter,
    Switch,
    Route
} from "react-router-dom";

import Overview from "./component/Overview/Overview";
import QueryBuilder from "./containers/QueryBuilder/QueryBuilder";
import QueryView from "./component/Query/QueryView/QueryView";
import Layout from "./hoc/Layout/Layout";

import './App.css';
import PlanOutput from "./containers/PlanOutput/PlanOutput";
import {tranceTheme} from "./hoc/TranceTheme/TranceTheme";
import {ThemeProvider} from "@material-ui/core/styles";

function App() {
  return (
      <ThemeProvider theme={tranceTheme}>
          <BrowserRouter>
              <div className="App">
                  <Layout>
                      <Switch>
                          <Route path={"/report"}>
                              <PlanOutput/>
                          </Route>
                          <Route path={"/tables"}>
                              <h1>Tables to be constructed</h1>
                          </Route>
                          <Route path={"/queryView"}>
                              <QueryView/>
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
      </ThemeProvider>
  );
}

export default App;
