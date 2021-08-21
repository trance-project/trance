/**
 * This File is used the handle rest api request to promt
 * the user a loading screen and if a error
 * has occured in the api response
 * @author Brandon Moore
 */

import {createSlice, PayloadAction} from "@reduxjs/toolkit";
import * as queryApi from '../QuerySlice/thunkQueryApiCalls';


/**
 * Define a type for the rest handler slice type
 */
interface RestHandlerState {
    //used to show Indications to user is error or if a request is made and waiting for response
    loading: "idle" | "loading" | "error";
    error: string;
}

/**
 * Define the initial state using type
 */
const initialState: RestHandlerState = {
    error: "",
    loading: "idle"
}

/**
 * Reducer slice used to manage anything to an rest call pending or error
 */
export const restHandlerSlice = createSlice({
    name: 'restHandlerReducer',
    initialState,
    reducers: {
        clearMessage: (state) => {
            state.loading = "idle";
            state.error = "";
        }
    },
    extraReducers: builder => {
        /**
         * Handler fetchQueryListSummary request
         */
        builder.addCase(queryApi.fetchQueryListSummary.pending, (state) => {
            console.log("[restHandlerSlice] Pending call for List Summary");
            state.loading = "loading";
        });
        builder.addCase(queryApi.fetchQueryListSummary.fulfilled, (state) => {
            console.log("[restHandlerSlice] Fulfilled call for List Summary");
            state.loading = "idle";
        });
        builder.addCase(queryApi.fetchQueryListSummary.rejected, (state, action) => {
            console.log("[restHandlerSlice] rejected call for List Summary", action.payload);
            state.loading = "error";
            state.error = action.payload as string;
        });

        /**
         * handler sendStandardNrcCode request
         */
        builder.addCase(queryApi.sendStandardNrcCode.pending, (state) => {
            console.log("[restHandlerSlice] Pending call for List Summary");
            state.loading = "loading";
        });
        builder.addCase(queryApi.sendStandardNrcCode.fulfilled, (state) => {
            console.log("[restHandlerSlice] Fulfilled call for List Summary");
            state.loading = "idle";
        });
        builder.addCase(queryApi.sendStandardNrcCode.rejected,(state, action) => {
            console.log("[restHandlerSlice] sendStandardNrcCode", action.payload);
            state.loading = "error";
            state.error = action.payload as string;
        });
    }
})

export const {clearMessage} = restHandlerSlice.actions;

export default restHandlerSlice.reducer;