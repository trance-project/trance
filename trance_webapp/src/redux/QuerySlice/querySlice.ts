import {createSlice, PayloadAction} from "@reduxjs/toolkit";
import {QuerySummary, Query} from "../../utils/Public_Interfaces";
import * as api from './thunkQueryApiCalls';

/**
 * Defined a type for the query slice type
 */
interface QueryState {
    queryListSummary: QuerySummary[];
    selectedQuery: QuerySummary | undefined;
    responseQuery: string;
    nrcQuery: string

}

/**
 * Defined the initial state using type
 */
const initialState: QueryState = {
    queryListSummary: [],
    selectedQuery: undefined,
    responseQuery: "",
    nrcQuery: "",
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
             state.nrcQuery = "";
         },
         createNewQuery: (state, action: PayloadAction<QuerySummary>) => {
             state.selectedQuery = action.payload
         },
         updateBlocklyQuery: (state, action: PayloadAction<string>) => {
             const newSelectedQuery = {...state.selectedQuery} as QuerySummary;
             newSelectedQuery.xmlDocument = action.payload;
             state.selectedQuery = newSelectedQuery;
         },
        setNrcCode: (state, action: PayloadAction<string>) => {
             state.nrcQuery = action.payload;
        }
    },
    extraReducers: builder => {
        builder.addCase(api.fetchQueryListSummary.fulfilled, (state, action: PayloadAction<QuerySummary[]>) => {
            state.queryListSummary = action.payload;
        });
        builder.addCase(api.fetchSelectedQuery.fulfilled, (state, action: PayloadAction<QuerySummary>) => {
            state.selectedQuery = action.payload
        });
        builder.addCase(api.sendStandardNrcCode.fulfilled, (state, action: PayloadAction<string>) => {
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

export const {SetSelectedQuery, createNewQuery , updateBlocklyQuery, setNrcCode} = querySlice.actions;

export default querySlice.reducer;