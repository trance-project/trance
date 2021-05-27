/**
 * File to separate the Thunk processes such as API calls from the QuerySlice class to easier maintenance.
 * please use this file if you would like do api called and ref them back into the querySlice.ts as a extra reducers
 */
import {createAsyncThunk} from "@reduxjs/toolkit";
import {trancePlayInstance} from "../../AxiosConfig";
import {BlocklyNrcCode, QuerySummary} from "../../utils/Public_Interfaces";
import {RootState} from '../store'


export const fetchQueryListSummary = createAsyncThunk(
    "query/fetchQuerySummaryList", async (arg, thunkAPI) => {
        try{
            const response = await trancePlayInstance.get("/blockly");

            console.log("[Api call fetchQueryListSummary]", response);
            return response.data as QuerySummary[];
        } catch (error){
            console.log("[Error Occured fetchQueryListSummary]" , error)
            return thunkAPI.rejectWithValue({error: error.message})
        }

    }
);

export const blocklyCreateNew = createAsyncThunk(
    "blockly/new", async(arg: QuerySummary, thunkAPI) => {
        try {
            const response = await trancePlayInstance.post("/blockly", {name: arg.name, xmlDocument: arg.xmlDocument})
            console.log("[Api call blocklyCreateNew]", response);
            if(response.status === 201){
                console.log("[Api call blocklyCreateNew Successful]", response);
            }

            return arg
        }catch (error){
            console.log("[Error Occured fetchSelectedQuery]" , error);
            return thunkAPI.rejectWithValue({error: error.message});
        }
    }
)

export const fetchSelectedQuery = createAsyncThunk(
    "query/fetchSelectedQuery", async (arg: QuerySummary, thunkAPI) => {
        try{
            const response = await trancePlayInstance.get(`/blockly/${arg._id}`);
            console.log("[Api call fetchSelectedQuery]", response);
            if(response.status === 200){
                console.log("[Api call fetchSelectedQuery Successful]", response.data);
            }

            return response.data as QuerySummary;
        }  catch (error){
            console.log("[Error Occured fetchSelectedQuery]" , error);
            return thunkAPI.rejectWithValue({error: error.message});
        }
    }
)

export const updateBlocklyQuery = createAsyncThunk(
    "query/updateBlocklyQuery", async (arg: void, thunkAPI) => {
        const state = thunkAPI.getState() as RootState
        const selectedState = state.query.selectedQuery;
        console.log("[ DEBUG Api call updateBlocklyQuery]", selectedState);
        try{
            if(selectedState){
                const response = await trancePlayInstance.patch(`/blockly/${selectedState._id}`,selectedState);
                console.log("[Api call updateBlocklyQuery]", response);
                if(response.status === 201){
                    console.log("[Api call updateBlocklyQuery Successful]");
                }

                return selectedState;
            }

        } catch (error){
            console.log("[Error Occurred updateBlocklyQuery]", error.message)
            return thunkAPI.rejectWithValue({error: error.message})
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