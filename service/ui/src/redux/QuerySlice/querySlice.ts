import {createSlice, PayloadAction} from "@reduxjs/toolkit";
import {QuerySummary, NewQuery, QueryResponse, ShreddedResponse,StandardResponse, NotepadResponse} from "../../utils/Public_Interfaces";
import * as api from './thunkQueryApiCalls';
import {RawNodeDatum} from "react-d3-tree/lib/types/common";

/**
 * Defined a type for the query slice type
 */
interface QueryState {
    queryListSummary: QuerySummary[];
    selectedQuery: QuerySummary | undefined;
    responseQuery: NewQuery | undefined;
    nrcQuery: string;
    standardPlan: RawNodeDatum | undefined;
    shreddedResponse: ShreddedResponse | undefined;
    notepadUrl: string;

}

/**
 * Defined the initial state using type
 */
const initialState: QueryState = {
    queryListSummary: [],
    selectedQuery: undefined,
    responseQuery: undefined,
    nrcQuery: "",
    shreddedResponse: undefined,
    standardPlan: undefined,
    notepadUrl: ""

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
        builder.addCase(api.sendStandardNrcCode.fulfilled, (state, action: PayloadAction<QueryResponse|undefined>) => {
            if(action.payload){
                if(action.payload.nrc.length > 0) {
                    state.responseQuery = action.payload.nrc[0];
                }
                // this no longer happens
                // console.log("{action.payload.standard_plan}", action.payload.standard_plan)
                // if(action.payload.standard_plan.length > 0){
                //     state.standardPlan = action.payload.standard_plan[0];
                // }
            }
        });
        builder.addCase(api.runStandardPlan.fulfilled, (state, action: PayloadAction<NotepadResponse|undefined>) => {
            if(action.payload){
                    state.notepadUrl = action.payload.nodepad_url;
            }
        });
        builder.addCase(api.runShredPlan.fulfilled, (state, action: PayloadAction<NotepadResponse|undefined>) => {
            if(action.payload){
                    state.notepadUrl = action.payload.nodepad_url;
            }
        });
        builder.addCase(api.blocklyCreateNew.fulfilled, (state, action: PayloadAction<QuerySummary>) => {
            const newQuerySummary: QuerySummary = {
                name: action.payload.name,
                xmlDocument: action.payload.xmlDocument
            }

            state.selectedQuery = action.payload
            state.queryListSummary = [...state.queryListSummary, newQuerySummary]

        });
        builder.addCase(api.getStandardPlan.fulfilled, (state, action:PayloadAction<StandardResponse|undefined> ) => {
            if(action.payload){
                if(action.payload.standard_plan.length > 0){
                    state.standardPlan = action.payload.standard_plan[0];
                }
            }
        });
        builder.addCase(api.getShreddedPlan.fulfilled, (state, action:PayloadAction<ShreddedResponse| undefined> ) => {
            if(action.payload){
                state.shreddedResponse = action.payload;
            }
        });

    }
});

export const {SetSelectedQuery, createNewQuery , updateBlocklyQuery, setNrcCode} = querySlice.actions;

export default querySlice.reducer;