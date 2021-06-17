import React, {useState} from "react";
import {Grid, Paper} from "@material-ui/core";
import Button from "@material-ui/core/Button";
import NoteIcon from '@material-ui/icons/Note';
import ForwardIcon from '@material-ui/icons/Forward';

import {standardCompilationViewThemeStyle} from '../../component/CompilerViewComponent/StandardCompilationView/standardCompilationViewThemeStyle';
import {customTabElement} from "../../utils/Public_Interfaces";
import StandardCompilationView from "../../component/CompilerViewComponent/StandardCompilationView/StandardCompilationView";
import ShreddedCompilationView
    from "../../component/CompilerViewComponent/ShreddedCompilationView/ShreddedCompilationView";
import CustomTabs from "../../component/ui/CustomTabs/CustomTabs";
import ShreddedPlan from "../../component/CompilerViewComponent/ShreddedPlan/ShreddedPlan";
import ModelMessage from "../../component/ui/ModelMessage/ModelMessage";
import StandardPlan from "../../component/CompilerViewComponent/StandardPlan/StandardPlan";
import ModalPrompt from "../../component/CompilerViewComponent/ModalPrompt/ModelPrompt";
import {useAppSelector} from '../../redux/Hooks/hooks';
import {newQuerySelected} from '../../redux/QuerySlice/tempData'

const CompilerView =()=> {
    const classes = standardCompilationViewThemeStyle();

    //set State here
    const [requestLoadingState, setRequestLoadingState] = useState(false);
    const [showModalState, setShowModalState] = useState(false);
    const [hasCompileState, setHasCompileState] = useState(false);
    const [showHoverMaterializationState, setShowHoverMaterializationState] = useState(-1);
    const [queryTreeDiagramState, setQueryTreeDiagramState] = useState<boolean>(false);

    const query = useAppSelector(state => state.query.selectedQuery);


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
                        query={newQuerySelected}
                        showDiagram={queryTreeDiagramState}
                        closeDiagram={handleQueryTreeDiagramClose}
                        hoverMaterializationLvl={showHoverMaterializationState}
                        hoverMaterializationLvlClose={closeHoverMaterializationLvl}
                        hoverMaterializationLvlOpen={handleHoverMaterializationLvl}
                        abortHover={abortHoverHandler}
                    />

                    <Button className={classes.queryBtnGroup} variant={"outlined"} color={"primary"} endIcon={<NoteIcon />} onClick={()=> window.location.href = "http://localhost:8085/#/notebook/2FK1WGZDP"}>Notebook</Button>
                    <Button className={classes.queryBtnGroup} variant={"contained"} color={"primary"} onClick={handleOpenCompilationDialogState} endIcon={<ForwardIcon/>}>Compile & Run</Button>
                </React.Fragment>
            )
        },
        {
            tabLabel:"Shredded NRC",
            jsxElement: (
                <ShreddedCompilationView/>
            ),
            disable:!hasCompileState
        },
        {
            tabLabel: "Standard Plan",
            jsxElement:<StandardPlan/>,
            disable: !hasCompileState
        },
        {
            tabLabel:"Shredded Plan",
            jsxElement: <ShreddedPlan/>,
            disable:!hasCompileState
        },
        // TODO: reused the shreddedCompilationView component and shreddedPlan component for this tab
        // {
        //     tabLabel:"Shredded Plan & NRC",
        //     jsxElement: <PlanResults/>,
        //     disable:!hasCompileState
        // },
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
            <ModalPrompt open={showModalState} close={handleCloseCompilationDialogState} openIsLoading={handleOpenModalState}/>
        </React.Fragment>
    )
}

export default CompilerView;