import {QuerySummaryList, TempTable} from "../../utils/Public_Interfaces";
import {createSlice, PayloadAction} from "@reduxjs/toolkit";
import * as api from './thunkTranceObjectApiCalls';

/**
 * Define a type for the tranceObject slice Type
 */
interface TranceObjectState {
    objects: TempTable[];
    loading: "idle" | "loading" | "error";
    error: string
}

/**
 * Initial state
 */
const initialState: TranceObjectState = {
    objects: [],
    loading: "idle",
    error: ""
}

/**
 * Reducer slice used to manage anything related to the tranceObject Type
 */
export const tranceObjectSlice = createSlice({
    name: 'tranceObjectReducer',
    initialState,
    reducers: {},
    extraReducers: builder => {
        builder.addCase(api.fetchTranceObjectList.fulfilled, (state, action: PayloadAction<TempTable[]>) => {
            state.objects = action.payload;
        });
        // builder.addCase(api.fetchTranceObjectList.rejected, (state, action: PayloadAction<String>) => {
        //     state.error = action.payload;
        // });
    }
})

export default tranceObjectSlice.reducer
