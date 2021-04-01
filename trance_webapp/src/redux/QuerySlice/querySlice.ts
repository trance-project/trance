import {createSlice, PayloadAction} from "@reduxjs/toolkit";
import {QuerySummaryList, Query} from "../../utils/Public_Interfaces";
import * as api from './thunkQueryApiCalls';

/**
 * Defined a type for the query slice type
 */
interface QueryState {
    queryListSummary: QuerySummaryList[];
    selectedQuery: Query | undefined;

    //used to show Indications to user is error or if a request is made and waiting for response
    loading: "idle" | "loading" | "error";
    error: string;
}

/**
 * Defined the initial state using type
 */
const initialState: QueryState = {
    queryListSummary: [],
    selectedQuery: undefined,

    loading: "idle",
    error:""
}

/**
 * Reducer slice used to manage anything related to queries
 */
export const querySlice = createSlice({
    name: 'queryReducer',
    initialState,
    reducers:{
         // Use the PayloadAction type to declare the contents of `action.payload`
         SetSelectedQuery: (state, action: PayloadAction<Query>) => {
             state.selectedQuery = action.payload
         }
    },
    extraReducers: builder => {
        builder.addCase(api.fetchQueryListSummary.fulfilled, (state, action: PayloadAction<QuerySummaryList[]>) => {
            state.queryListSummary = action.payload;
        });
        builder.addCase(api.fetchSelectedQuery.fulfilled, (state, action: PayloadAction<Query>) => {
            state.selectedQuery = action.payload
        })
    }
});

export const {SetSelectedQuery } = querySlice.actions;

export default querySlice.reducer;