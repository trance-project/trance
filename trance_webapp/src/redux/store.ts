import {configureStore} from '@reduxjs/toolkit';
import navigationReducer from "./NavigationSlice/navigationSlice";
import queryReducer from "./QuerySlice/querySlice";
import tranceObjectReducer from "./TranceObjectSlice/tranceObjectSlice";
import restApiErrorHandlerReducer from "./RestApiErrorHandlerSlice/restApiErrorHandlerSlice";

/**
 * Using the Redux Toolkit to create a central store for ease of access of methods and state across the webapp.
 * Please use the https://redux-toolkit.js.org/ as a ref on have to make changes to the redux store
 */

const store =  configureStore({
    reducer:{
        navigation: navigationReducer,
        query: queryReducer,
        tranceObject: tranceObjectReducer,
        restErrorHandle: restApiErrorHandlerReducer
    }
});

/**
 * Infer the `RootState` and `AppDispatch` types from the store itself
 */
export type RootState = ReturnType<typeof store.getState>

/**
 * Inferred type: {navigation: NavigationState, query: QueryState,
 * tranceObject: TranceObjectState, restErrorHandle: restApiErrorHandlerReducer}
 */
export type AppDispatch = typeof store.dispatch

/**
 * export default store to pass to the provider in the index.tsx
 */
export default store