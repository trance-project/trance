import React from "react";
import {Button, Grid, Typography} from "@material-ui/core";
import {Table} from '../../../../../../Interface/Public_Interfaces';
import AddIcon from '@material-ui/icons/Add';

import ChipSelect from "./ChipSelect/ChipSelect";
import JoinElement from "./JoinElement/JoinElement";


const tableData:Table[] = [
    {name: "Sample",
        id: 110,
        columns:
            [
                {id: 111,name: "sample", enable: true, children:[]},
                {id: 112,name: "turmorsite", enable: true, children:[]}
            ]
    },
    {name: "Copy",
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

interface ChipData {
    key: number;
    label: string[];
}


const JoinConfig = () => {
    const [tableJoinsState, setTableJoinsState] = React.useState<string[]>([]);
    const [SampleColumn, setSampleColumn] = React.useState<string[]>([]);
    const [copyColumn, setCopyColumn] = React.useState<string[]>([]);
    const [occurrencesColumn, setOccurrencesColumn] = React.useState<string[]>([]);
    const [chipData, setChipData] = React.useState<ChipData[]>([
        { key: 0, label: ['Sample.sample', "Occurrences.sample"] },
        { key: 1, label: ['Occurrences.Gene', "Copy.Gene"] },
    ]);

    const handleSampleColumnChange = (event: React.ChangeEvent<{ value: unknown }>) => {
        const columns = event.target.value as string[];
        setSampleColumn(columns);
        columns.forEach(column=>setTableJoinsState([...tableJoinsState,"Sample."+column]));
    };

    const handleCopyColumnChange = (event: React.ChangeEvent<{ value: unknown }>) => {
        const columns = event.target.value as string[];
        setCopyColumn(columns);
        columns.forEach(column=>setTableJoinsState([...tableJoinsState,"Copy."+column]));
    };


    const handleOccurrencesColumnChange = (event: React.ChangeEvent<{ value: unknown }>) => {
        const columns = event.target.value as string[];
        setOccurrencesColumn(columns);
        columns.forEach(column=>setTableJoinsState([...tableJoinsState,"Occurrences."+column]));
    };


    const handleAddJoin = () => {
        const data = [...chipData]
        data.push({key: data.length+1, label: tableJoinsState})
        setTableJoinsState([]);
        setSampleColumn([]);
        setCopyColumn([]);
        setOccurrencesColumn([]);
        setChipData(data);
    };

    const handleDelete = (key:number) => {
        const data = [...chipData]
        const newData = data.filter(el=> el.key !== key);
        setChipData(newData);
    };

    return(
        <React.Fragment>
            <Typography>Select a Table with Column you want to join two</Typography>
            <Grid container spacing={3}>
                <Grid item xs={3}>
                    <ChipSelect table={tableData[0]} columns={SampleColumn} setColumns={handleSampleColumnChange}/>
                </Grid>
                <Grid item xs={3}>
                    <ChipSelect table={tableData[1]} columns={copyColumn} setColumns={handleCopyColumnChange}/>
                </Grid>
                <Grid item xs={3}>
                    <ChipSelect table={tableData[2]} columns={occurrencesColumn} setColumns={handleOccurrencesColumnChange}/>
                </Grid>
                <Grid item xs={3}>
                    <Button variant={"contained"} endIcon={<AddIcon/>} onClick={handleAddJoin}>Join</Button>
                </Grid>
            </Grid>
            <Typography variant={"h6"}>Query Joins</Typography>
            <Grid container spacing={3} direction={"row"}>
                {chipData.map(el =>
                    <Grid item xs={3} >
                        <JoinElement key={el.key + Math.random().toString()} keyElement={el.key} label={el.label} delete={handleDelete} />
                    </Grid>
                )}
            </Grid>
        </React.Fragment>
    )
}

export default JoinConfig;