import React from "react";
import {Button, Grid, Typography} from "@material-ui/core";
import {Table} from '../../../../../../Interface/Public_Interfaces';
import AddIcon from '@material-ui/icons/Add';

import {joinConfigThemeStyle} from './JoinConfigThemeStyle';
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
            ]
    },
];


const JoinConfig = () => {
    const [personName, setPersonName] = React.useState<string[]>([]);

    const handleChange = (event: React.ChangeEvent<{ value: unknown }>) => {
        setPersonName(event.target.value as string[]);
    };

    const handleChangeMultiple = (event: React.ChangeEvent<{ value: unknown }>) => {
        const { options } = event.target as HTMLSelectElement;
        const value: string[] = [];
        for (let i = 0, l = options.length; i < l; i += 1) {
            if (options[i].selected) {
                value.push(options[i].value);
            }
        }
        setPersonName(value);
    };
    return(
        <React.Fragment>
            <Typography>Select a Table with Column you want to join two</Typography>
            <Grid container spacing={3}>
                <Grid item xs={3}>
                    <ChipSelect table={tableData[0]}/>
                </Grid>
                <Grid item xs={3}>
                    <ChipSelect table={tableData[1]}/>
                </Grid>
                <Grid item xs={3}>
                    <ChipSelect table={tableData[3]}/>
                </Grid>
                <Grid item xs={3}>
                    <Button variant={"contained"} endIcon={<AddIcon/>}>Join</Button>
                </Grid>
            </Grid>
            <Typography variant={"h6"}>Query Joins</Typography>
            <Grid container spacing={3}>
                <Grid item xs={3} direction={"row"}>
                    <JoinElement/>
                </Grid>
                <Grid item xs={3} direction={"row"}>
                    <JoinElement/>
                </Grid>
                <Grid item xs={3} direction={"row"}>
                    <JoinElement/>
                </Grid>
                <Grid item xs={3} direction={"row"}>
                    <JoinElement/>
                </Grid>
                <Grid item xs={3} direction={"row"}>
                    <JoinElement/>
                </Grid>
                <Grid item xs={3} direction={"row"}>
                    <JoinElement/>
                </Grid>
            </Grid>
        </React.Fragment>
    )
}

export default JoinConfig;