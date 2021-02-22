import React, {useState} from "react";
import Grid from "@material-ui/core/Grid";
import Paper from "@material-ui/core/Paper";

import ViewSelector from "../../component/ViewSelector/ViewSelector";
import QueryConfig from "../../component/QueryConfig/QueryConfig";
import QueryView from "../../component/QueryView/QueryView";
import {Column, Query, Table} from "../../Interface/Public_Interfaces";
import testData from "./testData";

const QueryBuilder =()=>{

    const [tablesState] = useState<Table[]>(testData)

    const [queryState, setQueryState] = useState<Query | undefined>();

    const createQueryHandler = (table:Table) => {
        setQueryState({
            tables :[table],
            groupBy: "",
            Where: "",
        })
    }


    const formatData = (table: Table, column: Column) => {
        table.columns.forEach(col => {
            if(col.children.length > 0){
                col.children.forEach(t => {
                    formatData(t,column);
                })
            }
            if (col.name === column.name) {
                col.enable = !col.enable
            }
        });

    }

    const ableDisableColumnHandler =(column: Column)=>{
        if(queryState) {
            const querylocal = JSON.parse(JSON.stringify(queryState)) as Query;
            querylocal.tables.forEach(el => {
                formatData(el, column)
            })
            setQueryState(querylocal);
        }
    }



    return(
        <React.Fragment>
            <Grid container spacing={3}>
                <ViewSelector gridXS={12} gridMD={4} gridLG={3} style={{"height":"450px"}} tables={tablesState} clicked={createQueryHandler}/>
                <QueryView gridXS={12} gridMD={8} gridLG={9} style={{"height":"450px"}} query={queryState}/>
                <Grid item xs={12}>
                    <Paper style={{"height":"450px"}}>
                        <QueryConfig query={queryState} config={ableDisableColumnHandler}/>
                    </Paper>
                </Grid>
            </Grid>
        </React.Fragment>
    );

}

export default QueryBuilder;