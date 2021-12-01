import {createAsyncThunk} from "@reduxjs/toolkit";
import {trancePlayInstance} from '../../AxiosConfig'
import {TempTable} from "../../utils/Public_Interfaces";

export const fetchTranceObjectList = createAsyncThunk(
    "tranceObject/fetchTranceObjectList", async  (arg,thunkAPI) => {
        try{
            const response = await trancePlayInstance.get("/tranceObject")
            console.log("[Api call fetchQueryListSummary]", response.data);
            return response.data as TempTable[];
        } catch (error){
            console.log("[Error Occured fetchTranceObjectList]", error)
            return thunkAPI.rejectWithValue(error.message);
        }
    }
);