import React, {useState} from "react";
import {Grid, Paper} from "@material-ui/core";
import Button from "@material-ui/core/Button";
import {standardCompilationViewThemeStyle} from './StandardCompilationView/standardCompilationViewThemeStyle'
import NoteIcon from '@material-ui/icons/Note';
import ForwardIcon from '@material-ui/icons/Forward';
import {customTabElement, Query} from "../../../Interface/Public_Interfaces";
import StandardCompilationView from "./StandardCompilationView/StandardCompilationView";
import Materialization from "../QueryBuilderComponents/QueryShredding/Materialzation/Materialization";
import CustomTabs from "../../ui/CustomTabs/CustomTabs";
import ShreddedPlanDiagram from "../../Plan/ShreddedPlan/ShreddedPlanDiagram/ShreddedPlanDiagram";
import ModelMessage from "../../ui/ModelMessage/ModelMessage";
import StandardCompilationDiagram
    from "./StandardCompilationView/StandardCompilationDiagram/StandardCompilationDiagram";
import PlanResults from "../../PlanResults/PlanResults";
import ModalPromt from "../../ModalPromt/ModelPromt";


const QueryView = () => {
    const classes = standardCompilationViewThemeStyle();
    const [showHoverMaterializationState, setShowHoverMaterializationState] = useState(-1);
    const [queryTreeDiagramState, setQueryTreeDiagramState] = useState<boolean>(false);
    const [queryState, setQueryState] = useState<Query | undefined>();
    const [requestLoadingState, setRequestLoadingState] = useState(false);
    const [showModalState, setShowModalState] = useState(false);
    const [hasCompileState, setHasCompileState] = useState(false);
    let hoverTimeout: NodeJS.Timeout;

    const handleHoverMaterializationLvl = (index:number)=>{
        hoverTimeout = setTimeout(()=>setShowHoverMaterializationState(index),1000);
    }

    const abortHoverHandler = () => {
        clearTimeout(hoverTimeout);
    }
    const closeHoverMaterializationLvl = ()=>{
        setShowHoverMaterializationState(-1)
    }

    const handleQueryTreeDiagramClose = () => {
        setQueryTreeDiagramState(false);
    }

    const handleQueryTreeDiagramOpen = () => {
        setQueryTreeDiagramState(true);
    }

    const handleOpenModalState = () => {
        setShowModalState(false);
        setRequestLoadingState(true);
        setTimeout(()=> {
            setRequestLoadingState(false);
            setHasCompileState(true);
        },2000);
    }

    const handleOpenCompilationDialogState = () => {
        setShowModalState(true);
    }
    const handleCloseCompilationDialogState = () => {
        setShowModalState(false);
    }

    const handleCloseModalState = () => {
        setShowModalState(false);
    }


    const queryViewTabs:customTabElement[] = [
        {
            tabLabel:"Source NRC",
            jsxElement: (
                <React.Fragment>
                    <StandardCompilationView
                        query={queryState}
                        showDiagram={queryTreeDiagramState}
                        closeDiagram={handleQueryTreeDiagramClose}
                        hoverMaterializationLvl={showHoverMaterializationState}
                        hoverMaterializationLvlClose={closeHoverMaterializationLvl}
                        hoverMaterializationLvlOpen={handleHoverMaterializationLvl}
                        abortHover={abortHoverHandler}
                    />

                    <Button className={classes.queryBtnGroup} variant={"outlined"} color={"primary"} endIcon={<NoteIcon />} onClick={()=> window.location.href = "http://localhost:8085/#/notebook/2FK1WGZDP"}>Notebook</Button>
                    <Button className={classes.queryBtnGroup} variant={"contained"} color={"primary"} onClick={handleOpenCompilationDialogState} endIcon={<ForwardIcon/>}>Compile</Button>
                </React.Fragment>
            )
        },
        {
            tabLabel:"Shredded NRC",
            jsxElement: (
                <Materialization/>
            ),
            disable:!hasCompileState
        },
        {
            tabLabel: "Standard Plan",
            jsxElement:<StandardCompilationDiagram/>,
            disable: !hasCompileState
        },
        {
            tabLabel:"Shredded Plan",
            jsxElement: <ShreddedPlanDiagram/>,
            disable:!hasCompileState
        },
        {
            tabLabel:"Shredded Plan & NRC",
            jsxElement: <PlanResults/>,
            disable:!hasCompileState
        },
    ]

    return (
        <React.Fragment>
            <Grid container spacing={1}>
                <Grid item  xs={12}>
                    <Paper style={{"height": 800}}>
                        <CustomTabs tabsElement={queryViewTabs} scrollable />
                    </Paper>
                </Grid>
            </Grid>
            <ModelMessage open={requestLoadingState} close={handleCloseModalState}/>
            <ModalPromt open={showModalState} close={handleCloseCompilationDialogState} openIsLoading={handleOpenModalState}/>
        </React.Fragment>
    )
}

export default QueryView;