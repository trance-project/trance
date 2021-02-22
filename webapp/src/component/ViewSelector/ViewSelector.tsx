import React from "react";
import Grid from "@material-ui/core/Grid";
import Paper from "@material-ui/core/Paper";

import CardSelector from "./CardSelector/CardSelector";
import {Typography} from "@material-ui/core";
import {Table} from "../../Interface/Public_Interfaces";

interface _ViewSelectorProps{
    gridXS: boolean | "auto" | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 | 11 | 12 | undefined;
    gridMD: boolean | "auto" | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 | 11 | 12 | undefined;
    gridLG: boolean | "auto" | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 | 11 | 12 | undefined;
    style: {};
    tables: Table[];
    clicked: (table:Table) => void;

}

const ViewSelector = (props:_ViewSelectorProps) => {

    const tableCard = (
        props.tables.map((el,index) => {
            return (
                <Grid item key={index}>
                    <CardSelector table={el} clicked={props.clicked} />
                </Grid>
            )
        })
    );

    return (
        <Grid item xs={props.gridXS} md={props.gridMD} lg={props.gridLG}>
            <Paper style={props.style}>
                <Typography variant={"h5"}>Table View</Typography>
                <Grid container direction={"row"} spacing={2}>
                    {tableCard}
                </Grid>
            </Paper>
        </Grid>
    );
}

export default ViewSelector;