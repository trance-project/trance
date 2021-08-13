/**
 * File to separate the Thunk processes such as API calls from the QuerySlice class to easier maintenance.
 * please use this file if you would like do api called and ref them back into the querySlice.ts as a extra reducers
 */
import {createAsyncThunk} from "@reduxjs/toolkit";
import {trancePlayInstance} from "../../AxiosConfig";
import {
    BlocklyNrcCode,
    NewQuery,
    QuerySummary,
    QueryResponse,
    ShreddedResponse,
    StandardResponse,
    NotepadResponse
} from "../../utils/Public_Interfaces";
import {RootState} from '../store'
import {RawNodeDatum} from "react-d3-tree/lib/types/common";



export const fetchQueryListSummary = createAsyncThunk(
    "query/fetchQuerySummaryList", async (arg, thunkAPI) => {
        try{
            const response = await trancePlayInstance.get("/blockly");
            if(response)

            console.log("[Api call fetchQueryListSummary]", response);
            return response.data as QuerySummary[];
        } catch (error){
            console.log("[Error Occured fetchQueryListSummary]" , error)
            return thunkAPI.rejectWithValue(error.message);
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

export const blocklyDelete = createAsyncThunk("blockly/delete", async(arg: QuerySummary, thunkAPI) =>{
    try {
        if(arg._id){
            const response = await trancePlayInstance.delete(`/blockly/${arg._id}`)
            console.log("[Api call blocklyDelete]", response);
            if(response.status === 200){
                console.log("[Api call blocklyDelete Successful]", response);
            }
        }
        return "";
    }catch (error){
        console.log("[Error Occured blocklyDelete]" , error);
        return thunkAPI.rejectWithValue({error: error.message});
    }
});

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
            return thunkAPI.rejectWithValue("error occured");
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
        const newQuerySelected = {
            name: "QuerySimple",
            key: "For s in samples Union ",
            labels: [{
                name: "{( sample",
                key: "s.bcr_patient_uuid"
            }, {
                name: "mutations",
                key: "For o in occurrences Union If (s.bcr_patient_uuid = o.donorId) Then ",
                labels: [{
                    name: " {(mutId",
                    key: "o.oid",
                },{
                    name : "scores",
                    key : "ReduceByKey[gene], [score], For t in o.transcript_consequences Union For c in copynumber Union If (t.gene_id = c.cn_gene_id AND c.cn_aliquot_uuid = s.bcr_aliquot_uuid) Then ",
                    labels : [{
                        name: "{( gene",
                        key : "t.gene_id"
                    }, {
                        name : "score",
                        key : "((c.cn_copy_number + 0.01) * If (t.impact = HIGH) Then 0.8 Else If (t.impact = MODERATE) Then 0.5 Else If (t.impact = LOW) Then 0.3 Else 0.01   ) })})})"
                    } ]
                }]
            }]
        } as NewQuery
        try{
            const response = await  trancePlayInstance.post("/nrccode", {...arg});
            if(response.status === 201){
                    return response.data as QueryResponse;
            }else if(response.status === 500){
                throw new Error ("Invalid Query format");
            }else{
                // console.log("pass Here then delete debug status anytime")
                return undefined;
            }
        } catch (error){
            console.log("[Error Occurred sendStandardNrcCode]", error.message)
            // return newQuerySelected;
            return thunkAPI.rejectWithValue(error.message)
        }
    }
)

export const getStandardPlan = createAsyncThunk(
    "query/getStandardPlan", async (arg:BlocklyNrcCode, thunkAPI) => {
        const treeDiagramData:RawNodeDatum ={
            name: '',
            attributes: {
                newLine: 'sample, mutations',
                level: '1',
                planOperator: 'PROJECT'
            },
            children: [
                {
                    name: '',
                    attributes: {
                        newLine: 'mutId, candidates, sID, sample',
                        level: '2',
                        planOperator:'NEST'
                    },
                    children: [
                        {
                            name: '',
                            attributes: {
                                newLine: 'gene, score, sID, sample, mutId',
                                level: '2',
                                planOperator:'NEST'
                            },
                            children: [
                                {
                                    name: '', //impact*(cnum+0.01)*sift*poly',
                                    attributes: {
                                        newLine:'sample, gene, label, mutId, sID',
                                        level: '3',
                                        planOperator:'SUM'
                                    },
                                    children:[
                                        {
                                            name:'',
                                            attributes: {
                                                newLine: 'sample, gene',
                                                level: '3',
                                                planOperator:'OUTERJOIN'
                                            },
                                            children:[
                                                {
                                                    name:'',
                                                    attributes: {
                                                        newLine: 'candidates',
                                                        level: '3',
                                                        planOperator:'OUTERUNNEST'
                                                    },
                                                    children:[
                                                        {
                                                            name:'',
                                                            attributes: {
                                                                newLine: 'sample',
                                                                level: '2',
                                                                planOperator:'OUTERJOIN'
                                                            },
                                                            children:[
                                                                {name: 'Occurrences',
                                                                    attributes: {
                                                                        level: '1',
                                                                    }},
                                                                {name:'Samples',
                                                                    attributes: {
                                                                        level: '1',
                                                                    }}
                                                            ]
                                                        }
                                                    ]
                                                },
                                                {
                                                    name:'CopyNumber',
                                                    attributes: {
                                                        level: '3',
                                                    }
                                                }
                                            ]
                                        }
                                    ]
                                },
                            ],
                        },
                    ],
                },
            ],
        };
        try{
            const response = await trancePlayInstance.post("/nrccode/standard", {...arg});
            if(response.status === 201){
                console.log("[getStandardPlan]", response.data);
                return response.data as StandardResponse;
                // return treeDiagramData ;
            }
            return undefined;
        }catch (error){
            console.log("[Error Occurred getStandardPlan]", error.message)
            // return treeDiagramData;
            return thunkAPI.rejectWithValue(error.message)
        }
    }
)

export const runStandardPlan = createAsyncThunk(
    "query/runStandardPlan", async (arg:BlocklyNrcCode, thunkAPI) => {
        const treeDiagramData:RawNodeDatum ={
            name: '',
            attributes: {
                newLine: 'sample, mutations',
                level: '1',
                planOperator: 'PROJECT'
            },
            children: [
                {
                    name: '',
                    attributes: {
                        newLine: 'mutId, candidates, sID, sample',
                        level: '2',
                        planOperator:'NEST'
                    },
                    children: [
                        {
                            name: '',
                            attributes: {
                                newLine: 'gene, score, sID, sample, mutId',
                                level: '2',
                                planOperator:'NEST'
                            },
                            children: [
                                {
                                    name: '', //impact*(cnum+0.01)*sift*poly',
                                    attributes: {
                                        newLine:'sample, gene, label, mutId, sID',
                                        level: '3',
                                        planOperator:'SUM'
                                    },
                                    children:[
                                        {
                                            name:'',
                                            attributes: {
                                                newLine: 'sample, gene',
                                                level: '3',
                                                planOperator:'OUTERJOIN'
                                            },
                                            children:[
                                                {
                                                    name:'',
                                                    attributes: {
                                                        newLine: 'candidates',
                                                        level: '3',
                                                        planOperator:'OUTERUNNEST'
                                                    },
                                                    children:[
                                                        {
                                                            name:'',
                                                            attributes: {
                                                                newLine: 'sample',
                                                                level: '2',
                                                                planOperator:'OUTERJOIN'
                                                            },
                                                            children:[
                                                                {name: 'Occurrences',
                                                                    attributes: {
                                                                        level: '1',
                                                                    }},
                                                                {name:'Samples',
                                                                    attributes: {
                                                                        level: '1',
                                                                    }}
                                                            ]
                                                        }
                                                    ]
                                                },
                                                {
                                                    name:'CopyNumber',
                                                    attributes: {
                                                        level: '3',
                                                    }
                                                }
                                            ]
                                        }
                                    ]
                                },
                            ],
                        },
                    ],
                },
            ],
        };
        try{
            const response = await trancePlayInstance.post("/nrccode/run ", {...arg});
            if(response.status === 201){
                console.log("[runStandardPlan]", response.data);
                return  response.data as NotepadResponse;
            }
            return undefined;
        }catch (error){
            console.log("[Error Occurred getStandardPlan]", error.message)
            // return treeDiagramData;
            return thunkAPI.rejectWithValue(error.message)
        }
    }
)


export const getShreddedPlan = createAsyncThunk(
    "query/ShreddedPlan", async (arg:BlocklyNrcCode, thunkAPI) => {
        const treeDiagramData:RawNodeDatum[] =[
            {
                name: '',
                attributes: {
                    newLine: 'sample,mutations:=Newlabel(sample)',
                    level:'1',
                    planOperator:'PROJECT'
                },
                children:[
                    {
                        name: 'MatSamples',
                        attributes: {
                            level: '1'
                        },
                    }
                ]
            },{
                name: '',
                attributes: {
                    newLine: 'mutId, scores:= NewLabel(sample)',
                    level:'2',
                    planOperator:'PROJECT'
                },
                children:[
                    {
                        name: 'MatOccurences',
                        attributes: {
                            level: '2'
                        },
                    }
                ]
            },{
                name:'',
                attributes:{
                    newLine: 'label,gene,score',
                    level:'3',
                    planOperator:'PROJECT'
                },
                children:[{
                    name: '',
                    attributes:{
                        //newLine: 'impact*(cnum+0.01)*sift*poly',
                        newLine: 'sample, label, gene',
                        level:'3',
                        planOperator:'SUM'
                    },
                    children:[{
                        name:'',
                        attributes:{
                            newLine: 'sample, gene',
                            level:'3',
                            planOperator:'JOIN'
                        },
                        children:[
                            {
                                name:'',
                                attributes:{
                                    newLine: 'label',
                                    level:'3',
                                    planOperator:'JOIN'
                                },
                                children:[
                                    {
                                        name:'LabelDomain', //_mutations_scores',
                                        attributes:{
                                            level:'3'
                                        }
                                    },
                                    {
                                        name:'MatOccurences_mutations',
                                        attributes:{
                                            level:'3'
                                        }
                                    }
                                ]
                            },
                            {
                                name:'CopyNumber',
                                attributes:{
                                    level:'3'
                                }
                            }
                        ]
                    }]
                }]
            }
        ];
        try{
            const response = await trancePlayInstance.post("/nrccode/shred", {...arg});
            if(response.status === 201){
                console.log("[getShreddedPlan]", response.data);

                return response.data as ShreddedResponse ;
            }
            return undefined;
        }catch (error){
            console.log("[Error Occurred getShreddedPlan]", error.message)
            // return treeDiagramData;
            return thunkAPI.rejectWithValue(error.message)
        }
    }
)



// {
//     "nrc": [{
//     "name": "Easy_query",
//     "key": "For s in samples Union",
//     "labels ": [{
//         "name ": "id ",
//         "key ": "s.bcr_patient_uuid "
//     }]
// }],
//     "standard_plan ": [{
//     "name ": "Easy_query ",
//     "plan ": {
//         "name": "",
//         "attributes": {
//             "planOperator": "PROJECT",
//             "level": 0,
//             "attrs": "TODO",
//             "newLine": ["bcr_patient_uuid"]
//         },
//         "children": [{
//             "todo": "TODO"
//         }]
//     }
// }]
// }