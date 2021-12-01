import React from "react";
import {Button, Grid, Typography} from "@material-ui/core";
import {Table} from '../../../../../../Interface/Public_Interfaces';
import AddIcon from '@material-ui/icons/Add';

import ChipSelect from "./ChipSelect/ChipSelect";
import JoinElement from "./JoinElement/JoinElement";
import {Associations} from '../../../../../../Interface/Public_Interfaces';




const tableData:Table[] = [
    {name: "Sample",
        abr:"s",
        id: 110,
        columns:
            [
                {id: 111,name: "sample", enable: true, children:[]},
                {id: 112,name: "turmorsite", enable: true, children:[]}
            ]
    },
    {name: "CopyNumber",
        abr:'c',
        id: 113,
        columns:
            [
                {id: 114,name: "sample", enable: true, children:[]},
                {id: 115,name: "gene", enable: true, children:[]} ,
                {id: 116,name: "cnum", enable: true, children:[]},
            ]
    },
    {name: "Occurrences",
        id: 117,
        abr:'o',
        columns:
            [
                {id: 118,name: "sample", enable: true, children:[]},
                {id: 119,name: "contig", enable: true, children:[]} ,
                {id: 120,name: "end", enable: true, children:[]},
                {id: 121,name: "reference", enable: true, children:[]},
                {id: 122,name: "alternate", enable: true, children:[]},
                {id: 123,name: "mutid", enable: true, children:[]},
                {id: 124,name: "gene", enable: true, children:[]},
                {id: 125,name: "impact", enable: true, children:[]},
                {id: 126,name: "sift", enable: true, children:[]},
                {id: 127,name: "poly", enable: true, children:[]},
                {id: 128,name: "conseq", enable: true, children:[]},
            ]
    },
];


interface _JoinConfigProps{
    associations: Associations[];
    tables: string[];
    onAssociation: (objectAssociations:string[]) => void;
    onAssociationDelete: (key:number)=>void;
}


const JoinConfig = (props: _JoinConfigProps) => {
    const [tableJoinsState, setTableJoinsState] = React.useState<string[]>([]);
    const [SampleColumn, setSampleColumn] = React.useState<string[]>([]);
    const [copyColumn, setCopyColumn] = React.useState<string[]>([]);
    const [occurrencesColumn, setOccurrencesColumn] = React.useState<string[]>([]);

    const handleSampleColumnChange = (event: React.ChangeEvent<{ value: unknown }>) => {
        const columns = event.target.value as string[];
        setSampleColumn(columns);
        columns.forEach(column=>setTableJoinsState([...tableJoinsState,tableData[0].abr+"."+column]));
    };

    const handleCopyColumnChange = (event: React.ChangeEvent<{ value: unknown }>) => {
        const columns = event.target.value as string[];
        setCopyColumn(columns);
        columns.forEach(column=>setTableJoinsState([...tableJoinsState,tableData[1].abr+"."+column]));
    };

    const handleOccurrencesColumnChange = (event: React.ChangeEvent<{ value: unknown }>) => {
        const columns = event.target.value as string[];
        setOccurrencesColumn(columns);
        columns.forEach(column=> {
            let abr_column;
            if(column === "gene"){
                abr_column="t."+column;
            }else{
                abr_column=tableData[2].abr + "." + column
            }
            return setTableJoinsState([...tableJoinsState, abr_column]);
        });
    };


    const handleAddJoin = () => {
        const associations = [...tableJoinsState];
        setTableJoinsState([]);
        setOccurrencesColumn([]);
        setCopyColumn([]);
        setSampleColumn([]);
        props.onAssociation(associations);
    }

    const chipSelect = () => {
        const length = props.tables.length;

        return props.tables.map(el => {
            const object = returnTable(el)!;
            const columnObject=columnChangeHandler(el)!;
            return (
                <Grid key={object.name} item xs={length > 2 ? 3 : 4}>
                    <ChipSelect table={object} columns={columnObject.columns} setColumns={columnObject.onChangeMethod}/>
                </Grid>
            )
        })
    }

    const columnChangeHandler = (objectName:string) => {
        switch (objectName){
            case "Sample":
                return {
                    columns:SampleColumn,
                    onChangeMethod:handleSampleColumnChange
                };
            case "Occurrences":
                return {
                    columns:occurrencesColumn,
                    onChangeMethod:handleOccurrencesColumnChange
                };
            case "CopyNumber":
                return {
                    columns:copyColumn,
                    onChangeMethod:handleCopyColumnChange
                };
        }
    }

    const returnTable = (tableName:string) => {
        return tableData.find(tableEl => tableEl.name === tableName);
    }





    return(
        <React.Fragment>
            <Grid container spacing={3}>
                {chipSelect()}
                <Grid item xs={props.tables.length>2?3:4}>
                    <Button variant={"contained"} endIcon={<AddIcon/>} onClick={handleAddJoin}>Add</Button>
                </Grid>
            </Grid>
            <Typography variant={"h6"}></Typography>
            <Grid container spacing={3} direction={"row"}>
                {props.associations.map(el =>
                    <Grid item xs={3} >
                        <JoinElement key={el.key + Math.random().toString()} keyElement={el.key} label={el.label} delete={props.onAssociationDelete} />
                    </Grid>
                )}
            </Grid>
        </React.Fragment>
    )
}

export default JoinConfig;