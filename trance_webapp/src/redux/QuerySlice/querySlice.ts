import {createSlice, PayloadAction} from "@reduxjs/toolkit";
import {QuerySummary, Query} from "../../utils/Public_Interfaces";
import * as api from './thunkQueryApiCalls';

/**
 * Defined a type for the query slice type
 */
interface QueryState {
    queryListSummary: QuerySummary[];
    selectedQuery: QuerySummary | undefined;
    responseQuery: String;

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
    responseQuery: "",

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
         SetSelectedQuery: (state, action: PayloadAction<QuerySummary>) => {
             state.selectedQuery = action.payload
         },
         createNewQuery: (state, action: PayloadAction<QuerySummary>) => {
             state.selectedQuery = action.payload
         },
         updateBlocklyQuery: (state, action: PayloadAction<string>) => {
             const newSelectedQuery = {...state.selectedQuery} as QuerySummary;
             newSelectedQuery.xmlDocument = action.payload;
             state.selectedQuery = newSelectedQuery;
         }
    },
    extraReducers: builder => {
        builder.addCase(api.fetchQueryListSummary.fulfilled, (state, action: PayloadAction<QuerySummary[]>) => {
            state.queryListSummary = action.payload;
        });
        builder.addCase(api.fetchSelectedQuery.fulfilled, (state, action: PayloadAction<QuerySummary>) => {
            state.selectedQuery = action.payload
        });
        builder.addCase(api.sendStandardNrcCode.fulfilled, (state, action: PayloadAction<String>) => {
            state.responseQuery = action.payload
        });
        builder.addCase(api.blocklyCreateNew.fulfilled, (state, action: PayloadAction<QuerySummary>) => {
            const newQuerySummary: QuerySummary = {
                name: action.payload.name,
                xmlDocument: action.payload.xmlDocument
            }

            state.selectedQuery = action.payload
            state.queryListSummary = [...state.queryListSummary, newQuerySummary]

        });

    }
});

export const {SetSelectedQuery, createNewQuery , updateBlocklyQuery} = querySlice.actions;

export default querySlice.reducer;