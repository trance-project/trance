/**
 * Trace Routed is the component to handle and set the routing options available for the Trance project.
 * This should be the place to edit|change|add routes to the app
 */

import React from 'react';
import {BrowserRouter, Route, Switch} from "react-router-dom";

import Layout from "./hoc/Layout/Layout";
import './App.css';
import {pageRoutes} from "./utils/Public_enums";
import {useAppSelector,useAppDispatch} from './redux/Hooks/hooks';
import {goToRoute} from './redux/NavigationSlice/navigationSlice';
import CompilerView from "./containers/CompilerView/CompilerView";
import PlanOutput from "./containers/PlanOutput/PlanOutput";
import BlocklyComponent
    from "./component/Query/QueryBuilderComponents/StandardCompilationBuilder/BlocklyBuilder/Blockly/Blockly_Component";
import Overview from "./component/Overview/Overview";
import AlertNotification from "./hoc/AlertNotification/AlertNotification";

/**
 * This Component is used for navigation, if you would like
 * to add a new page this the webapp this is the place you will do it
 * @constructor
 */
function TranceRouter() {

    const activePage = useAppSelector(state => state.navigation.activePage);
    const dispatch = useAppDispatch();

    const goto_Route = (page: pageRoutes) => {
        dispatch(goToRoute(page));
    }

    return (
            <BrowserRouter>
                <div className="App">
                    <Layout activePage={activePage} goto_Route={goto_Route}>
                        <Switch>
                            <Route path={"/report"}>
                                <AlertNotification>
                                    <PlanOutput/>
                                </AlertNotification>
                            </Route>
                            <Route path={"/tables"}>
                                <h1>Tables to be constructed</h1>
                            </Route>
                            <Route path={"/queryView"}>
                                <AlertNotification>
                                    <CompilerView/>
                                </AlertNotification>
                            </Route>
                            <Route path={"/builder"}>
                                <AlertNotification>
                                    <BlocklyComponent/>
                                </AlertNotification>
                            </Route>
                            <Route path={"/"}>
                                <AlertNotification>
                                    <Overview/>
                                </AlertNotification>
                            </Route>
                        </Switch>
                    </Layout>
                </div>
            </BrowserRouter>
    );
}

export default TranceRouter;
