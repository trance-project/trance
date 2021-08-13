/**
 * This File is used the handle rest api request to promt
 * the user a loading screen and if a error
 * has occured in the api response
 * @author Brandon Moore
 */

import {createSlice, PayloadAction} from "@reduxjs/toolkit";
import * as queryApi from '../QuerySlice/thunkQueryApiCalls';
import {blocklyDelete} from "../QuerySlice/thunkQueryApiCalls";


/**
 * Define a type for the rest handler slice type
 */
interface RestHandlerState {
    //used to show Indications to user is error or if a request is made and waiting for response
    loading: "Idle" | "Loading" | "Error";
    message: string;
}

/**
 * Define the initial state using type
 */
const initialState: RestHandlerState = {
    message: "",
    loading: "Idle"
}

/**
 * Reducer slice used to manage anything to an rest call pending or error
 */
export const restHandlerSlice = createSlice({
    name: 'restHandlerReducer',
    initialState,
    reducers: {
        clearMessage: (state) => {
            state.loading = "Idle";
            state.message = "";
        }
    },
    extraReducers: builder => {
        /**
         * Handler fetchQueryListSummary request
         */
        builder.addCase(queryApi.fetchQueryListSummary.pending, (state) => {
            console.log("[restHandlerSlice] Pending call for List Summary");
            state.loading = "Loading";
        });
        builder.addCase(queryApi.fetchQueryListSummary.fulfilled, (state) => {
            console.log("[restHandlerSlice] Fulfilled call for List Summary");
            state.loading = "Idle";
        });
        builder.addCase(queryApi.fetchQueryListSummary.rejected, (state, action) => {
            console.log("[restHandlerSlice] rejected call for List Summary", action.payload);
            state.loading = "Error";
            state.message = action.payload as string;
        });

        /**
         * handler sendStandardNrcCode request
         */
        builder.addCase(queryApi.sendStandardNrcCode.pending, (state) => {
            console.log("[restHandlerSlice] Pending call for List Summary");
            state.loading = "Loading";
        });
        builder.addCase(queryApi.sendStandardNrcCode.fulfilled, (state) => {
            console.log("[restHandlerSlice] Fulfilled call for List Summary");
            state.loading = "Idle";
            state.message = "Query Validated"
        });
        builder.addCase(queryApi.sendStandardNrcCode.rejected,(state, action) => {
            console.log("[restHandlerSlice] sendStandardNrcCode", action.payload);
            state.loading = "Error";
            state.message = action.payload as string;
        });

        /**
         * handler updateBlocklyQuery request
         */
        builder.addCase(queryApi.updateBlocklyQuery.pending, (state) => {
            console.log("[restHandlerSlice] Pending call for List Summary");
            state.loading = "Loading";
        });
        builder.addCase(queryApi.updateBlocklyQuery.fulfilled, (state) => {
            console.log("[restHandlerSlice] Fulfilled call for List Summary");
            state.loading = "Idle";
            state.message = "Query Updated"
        });
        builder.addCase(queryApi.updateBlocklyQuery.rejected,(state, action) => {
            console.log("[restHandlerSlice] sendStandardNrcCode", action.payload);
            state.loading = "Error";
            state.message = action.payload as string;
        });

        /**
         * handler blocklyDelete request
         */
        builder.addCase(queryApi.blocklyDelete.pending, (state) => {
            console.log("[restHandlerSlice] Pending call for blocklyDelete");
            state.loading = "Loading";
        });
        builder.addCase(queryApi.blocklyDelete.fulfilled, (state) => {
            console.log("[restHandlerSlice] Fulfilled call for blocklyDelete");
            state.loading = "Idle";
            state.message = "Query Deleted"
        });
        builder.addCase(queryApi.blocklyDelete.rejected,(state, action) => {
            console.log("[restHandlerSlice] blocklyDelete", action.payload);
            state.loading = "Error";
            state.message = action.payload as string;
        });

        /**
         * handler runStandardPlan request
         */
        builder.addCase(queryApi.runStandardPlan.pending, (state) => {
            console.log("[restHandlerSlice] Pending call for runStandardPlan");
            state.loading = "Loading";
        });
        builder.addCase(queryApi.runStandardPlan.fulfilled, (state) => {
            console.log("[restHandlerSlice] Fulfilled call for runStandardPlan");
            state.loading = "Idle";
            state.message = "Plan Executed Successfully"
        });
        builder.addCase(queryApi.runStandardPlan.rejected,(state, action) => {
            console.log("[restHandlerSlice] runStandardPlan", action.payload);
            state.loading = "Error";
            state.message = action.payload as string;
        });
    }
})

export const {clearMessage} = restHandlerSlice.actions;

export default restHandlerSlice.reducer;