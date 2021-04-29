/**
 * File to seperate the Thunk processes such as API calls from the QuerySlice class to easier maintenance.
 * please use this file if you would like do api called and ref them back into the querySlice.ts as a extra reducers
 */
import {createAsyncThunk} from "@reduxjs/toolkit";
import axiosInstance, {trancePlayInstance} from "../../AxiosConfig";
import {rows, querySelected} from "./tempData";
import {BlocklyNrcCode, Query, QuerySummaryList} from "../../utils/Public_Interfaces";


export const fetchQueryListSummary = createAsyncThunk(
    "query/fetchQuerySummaryList", async (arg, thunkAPI) => {
        try{
            const response = await axiosInstance.get("/");

            //TODO: Replace with Rest call
            console.log("[Api call fetchQueryListSummary]", response);
            return rows as QuerySummaryList[];
        } catch (error){
            console.log("[Error Occured fetchQueryListSummary]" , error)
            return thunkAPI.rejectWithValue({error: error.message})
        }

    }
);

export const fetchSelectedQuery = createAsyncThunk(
    "query/fetchSelectedQuery", async (arg: QuerySummaryList, thunkAPI) => {
        try{
            const response = await axiosInstance.get("/");

            //TODO: Replace with Rest call
            console.log("[Api call fetchSelectedQuery]", response);
            return querySelected as Query;
        }  catch (error){
            console.log("[Error Occured fetchSelectedQuery]" , error);
            return thunkAPI.rejectWithValue({error: error.message});
        }
    }
)

export const sendStandardNrcCode = createAsyncThunk(
    "query/sendStandardNrcCode", async  (arg: BlocklyNrcCode, thunkAPI) => {
        try{
            const response = await  trancePlayInstance.post("/nrccode", {...arg});
            return response.data as string;
        } catch (error){
            console.log("[Error Occurred sendStandardNrcCode]", error.message)
            return thunkAPI.rejectWithValue({error: error.message})
        }
    }
)