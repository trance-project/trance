import {TempTable} from "../../utils/Public_Interfaces";
import {createSlice, PayloadAction} from "@reduxjs/toolkit";
import * as api from './thunkTranceObjectApiCalls';
import Blockly from "blockly";

/**
 * Define a type for the tranceObject slice Type
 */
interface TranceObjectState {
    objects: TempTable[];
    selectedObjects: TempTable[];
    loading: "idle" | "loading" | "error";
    error: string
}

/**
 * Initial state
 */
const initialState: TranceObjectState = {
    objects: [],
    selectedObjects: [],
    loading: "idle",
    error: ""
}

/**
 * Reducer slice used to manage anything related to the tranceObject Type
 */
export const tranceObjectSlice = createSlice({
    name: 'tranceObjectReducer',
    initialState,
    reducers: {
        // use the PayloadAction type to declare the contents of the action.payload
        addToSelectedObjects: (state, action: PayloadAction<TempTable>) => {
            state.selectedObjects = [...state.selectedObjects, action.payload]
        },
        modifySelectedObjectKeyValue: (state, action:PayloadAction<TempTable>) => {
            // remove Temp object from selected list to replace it for new modify one
            const selectedObjects = state.objects.filter(o => o._id !== action.payload._id)
            selectedObjects.push(action.payload);
            state.objects = selectedObjects;
        }
    },
    extraReducers: builder => {
        builder.addCase(api.fetchTranceObjectList.fulfilled, (state, action: PayloadAction<TempTable[]>) => {
            state.objects = action.payload;
            Blockly.Extensions.register('dynamic_menu_extension', function (){
                // @ts-ignore
                this.getInput('DROPDOWN_PLACEHOLDER')
                    .appendField(new Blockly.FieldDropdown(
                        ()=>{
                            const options: any[] = [];
                            options.push(["select a object", "null"])
                            if(action.payload){
                                action.payload.forEach(t => {
                                    if(!t.isCollectionType)
                                    options.push([t.name, t.name]);
                                })
                            }
                            return options
                        }
                    ), 'ATTRIBUTE_VALUE');
            })
        });
        // builder.addCase(api.fetchTranceObjectList.rejected, (state, action: PayloadAction<String>) => {
        //     state.error = action.payload;
        // });
    }
})

export const {addToSelectedObjects, modifySelectedObjectKeyValue} = tranceObjectSlice.actions;
export default tranceObjectSlice.reducer
