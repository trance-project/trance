import React, {useState} from "react";
import Grid from "@material-ui/core/Grid";
import Paper from "@material-ui/core/Paper";
import Button from "@material-ui/core/Button";
import SaveIcon from '@material-ui/icons/Save';

import ViewSelector from "../../component/ViewSelector/ViewSelector";
import QueryConfig from "../../component/Query/QueryBuilderComponents/QueryConfig/QueryConfig";
import {Column, Query, Table,customTabElement} from "../../Interface/Public_Interfaces";
import testData from "./testData";
import StringIdGenerator from "../../classObjects/stringIdGenerator";
import {ButtonGroup, IconButton, Typography} from "@material-ui/core";
import CustomTabs from "../../component/ui/CustomTabs/CustomTabs";
import Materialization from "../../component/Query/QueryBuilderComponents/QueryShredding/Materialzation/Materialization";
import {queryBuilderThemeStyle} from './queryBuilderThemeStyle';
import ModelMessage from "../../component/ui/ModelMessage/ModelMessage";
import PlanResult from "../../component/PlanResults/PlanResults";
import AccountTreeIcon from "@material-ui/icons/AccountTree";
import StandardCompilationBuilder
    from "../../component/Query/QueryBuilderComponents/StandardCompilationBuilder/StandardCompilationBuilder";
import PaperWithHeader from "../../component/ui/Paper/PaperWithHeader/PaperWithHeader";

const QueryBuilder =()=>{
    const classes = queryBuilderThemeStyle();
    const stringIdGen = StringIdGenerator.getInstance()!;


    const [tablesState] = useState<Table[]>(testData);
    const [showModalState, setShowModalState] = useState(false);
    const [requestLoadingState, setRequestLoadingState] = useState(false);
    const [showModalPlanState, setShowModalPlanState] = useState(false);
    const [queryState, setQueryState] = useState<Query | undefined>({
        tables :testData,
        groupBy: "",
        Where: "",
        selectedColumns:[]
    });



    const handleOpenModalState = () => {
        setShowModalState(true);
        setRequestLoadingState(true);
        setTimeout(()=> {
            setRequestLoadingState(false)
        },2000);
    }

    const handleCloseModalState = () => {
        setShowModalState(false);
    }

    const handleOpenModalPlanState = () => {
        setShowModalPlanState(true);
        // setRequestLoadingState(true);
        // setTimeout(()=> {
        //     setRequestLoadingState(false)
        // },2000);
    }

    const handleCloseModalPlanState = () => {
        setShowModalPlanState(false);
    }

    const createQueryHandler = (table:Table) => {
        const selectedColumnsDefault:Column[] = [];
        //copy table for new table to prevent object reference conflict
        const newTable = JSON.parse(JSON.stringify(table)) as Table;
        newTable.abr = stringIdGen.next();

        setQueryState({
            tables :[newTable],
            groupBy: "",
            Where: "",
            selectedColumns:selectedColumnsDefault
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
                <Grid item xs={12} md={4} lg={3}>
                    <PaperWithHeader heading={'Inputs'} height={450}>
                        <ViewSelector tables={tablesState} clicked={createQueryHandler}/>
                    </PaperWithHeader>
                </Grid>
                <Grid item xs={12} md={8} lg={9}>
                    <PaperWithHeader height={450} heading={"Query Builder"}>
                        <StandardCompilationBuilder
                            query={queryState}
                        />
                        <ButtonGroup className={classes.queryBtnGroup} color={"primary"} aria-label={"Contained primary button group"}>
                            <Button variant={"contained"} onClick={handleOpenModalState} endIcon={<SaveIcon/>}>Save</Button>
                        </ButtonGroup>
                    </PaperWithHeader>
                </Grid>
                <Grid item xs={12}>
                    <Paper style={{"height":"450px"}}>
                        <QueryConfig query={queryState} config={ableDisableColumnHandler}/>
                    </Paper>
                </Grid>
            </Grid>
            <ModelMessage open={showModalState} close={handleCloseModalState}/>
            {/*<PlanResult open={showModalPlanState} close={handleCloseModalPlanState} successful={requestLoadingState}/>*/}
        </React.Fragment>
    );

}

export default QueryBuilder;