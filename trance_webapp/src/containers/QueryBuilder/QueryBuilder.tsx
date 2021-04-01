import React, {useState} from "react";
import Grid from "@material-ui/core/Grid";
import Paper from "@material-ui/core/Paper";
import Button from "@material-ui/core/Button";
import SaveIcon from '@material-ui/icons/Save';

import ViewSelector from "../../component/Query/QueryBuilderComponents/ViewSelector/ViewSelector";
import QueryConfig from "../../component/Query/QueryBuilderComponents/QueryConfig/QueryConfig";
import {Column, Query, Table} from "../../utils/Public_Interfaces";
import testData from "./testData";
import {ButtonGroup} from "@material-ui/core";
import {queryBuilderThemeStyle} from './queryBuilderThemeStyle';
import ModelMessage from "../../component/ui/ModelMessage/ModelMessage";
import StandardCompilationBuilder
    from "../../component/Query/QueryBuilderComponents/StandardCompilationBuilder/StandardCompilationBuilder";
import PaperWithHeader from "../../component/ui/Paper/PaperWithHeader/PaperWithHeader";

const QueryBuilder =()=>{
    const classes = queryBuilderThemeStyle();

    const [selectedNodeState, setSelectedNodeState] = useState("1");
    const [selectedSubQueryState, setSelectedSubQueryState] = useState<Query>();
    const [tablesState] = useState<Table[]>(testData);
    const [showModalState, setShowModalState] = useState(false);
    const [requestLoadingState, setRequestLoadingState] = useState(false);
    const [showModalPlanState, setShowModalPlanState] = useState(false);
    const [queryState, setQueryState] = useState<Query>({
        name: "Demo_Query",
        level:'1',
        type:"select",
        table : testData[0],
        Where: "",
        selectedColumns:[],
        associations:[]
    });

   const handleSelectedNodeStateChanged = (nodeId:string)=>{
       setSelectedNodeState(nodeId);
       setSelectedQuery(queryState);
   }

    const setSelectedQuery = (query:Query) => {
        if(query.level === selectedNodeState){
            setSelectedSubQueryState(query);
        }else{
            if(query.children) {
                setSelectedQuery(query.children)
            }
        }
        return query;

    }

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



    //TODO refactor to use redux
    const createQueryHandler = (table:Table) => {

    }

    //TODO refactor to use redux
    const createJoinHandler = (joinTable:Table) => {

    }

    //TODO refactor to use redux
    const toggleEnableColumn = (query:Query , column:Column) => {

    }

    const ableDisableColumnHandler =(column: Column)=>{
        if(queryState) {
            const immutableQuery = JSON.parse(JSON.stringify(queryState)) as Query;
            if(selectedSubQueryState?.level === "1"){
                toggleEnableColumn(immutableQuery, column);
            }else if(selectedSubQueryState?.level === "2"){
                console.log("[debug level two call]")
                toggleEnableColumn(immutableQuery.children!, column);
            }else if(selectedSubQueryState?.level === "3"){
                toggleEnableColumn(immutableQuery.children?.children!, column);
            }
            setQueryState(immutableQuery);
        }
    }

    const handleQueryAssociation = (objectAssociations:string[]) => {
        // const immutableQuery = JSON.parse(JSON.stringify(queryState)) as Query
        //
        // if(selectedNodeState==="2"){
        //     const query = immutableQuery.children;
        //     query?.children?.associations.push({key: immutableQuery.associations.length+1, label: objectAssociations});
        //     immutableQuery.children=query;
        // }else{
        //     immutableQuery.children?.associations.push({key: immutableQuery.associations.length+1, label: objectAssociations})
        // }
        // setQueryState(immutableQuery);
    };

    const handleDeleteQueryAssociation = (key:number) => {
        const immutableQuery = JSON.parse(JSON.stringify(queryState)) as Query
        if(immutableQuery.children){
            let data = immutableQuery.children.associations!
            // data.filter(el=> el.key !== key);
        }
        setQueryState(immutableQuery);
    };

    console.log("[query object]", queryState);
    console.log("[query NodeId]", selectedNodeState);
    console.log("[selected Query]", selectedSubQueryState);

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
                            joinAction={createJoinHandler}
                            onAssociationDelete={handleDeleteQueryAssociation}
                            onAssociation={handleQueryAssociation}
                            associations={queryState}
                            onNodeClick={handleSelectedNodeStateChanged}
                            selectedNode={selectedNodeState}
                        />
                        <ButtonGroup className={classes.queryBtnGroup} color={"primary"} aria-label={"Contained primary button group"}>
                            <Button variant={"contained"} onClick={handleOpenModalState} endIcon={<SaveIcon/>}>Save</Button>
                        </ButtonGroup>
                    </PaperWithHeader>
                </Grid>
                <Grid item xs={12}>
                    <Paper style={{"height":"450px"}}>
                        <QueryConfig query={queryState} config={ableDisableColumnHandler} selectedNode={selectedNodeState}/>
                    </Paper>
                </Grid>
            </Grid>
            <ModelMessage open={showModalState} close={handleCloseModalState}/>
            {/*<PlanResult open={showModalPlanState} close={handleCloseModalPlanState} successful={requestLoadingState}/>*/}
        </React.Fragment>
    );

}

export default QueryBuilder;