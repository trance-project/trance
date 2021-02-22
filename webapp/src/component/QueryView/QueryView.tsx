import React from 'react';
import Grid from "@material-ui/core/Grid";
import {Typography} from "@material-ui/core";
import Paper from "@material-ui/core/Paper";
import {Query, Table} from "../../Interface/Public_Interfaces";

interface _QueryViewProps {
    gridXS: boolean | "auto" | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 | 11 | 12 | undefined;
    gridMD: boolean | "auto" | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 | 11 | 12 | undefined;
    gridLG: boolean | "auto" | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 | 11 | 12 | undefined;
    style: {};
    query:Query | undefined;
}

const QueryView = (props:_QueryViewProps) => {
    let columnsSelect: string[] = [];
    let statement = "";
    let filter= "";

    const queryParentSelect = (table:Table) => {
        for(const column of(table.columns)){
            if(column.children.length > 0){
                for(const table of(column.children)){
                    queryParentSelect(table);
                }
            }
            if(column.enable){
                if(column.children.length===0)
                    columnsSelect.push(column.name);
            }
        }
    }


    if(props.query) {
        for(const table of props.query.tables){
            queryParentSelect(table);
        }

        statement = `for t in ${props.query.tables[0].name}(${columnsSelect.join(',')}) union`;
        filter = "if t.column1 < 60 && t.column2 > 60 then t";
    }



    return (
        <Grid item xs={props.gridXS} md={props.gridMD} lg={props.gridLG}>
            <Paper style={props.style}>
                <Typography variant={"h5"}>Query View</Typography>
                <p>{statement}</p>
                <p>{filter}</p>
            </Paper>
        </Grid>
    );
};

export default QueryView;