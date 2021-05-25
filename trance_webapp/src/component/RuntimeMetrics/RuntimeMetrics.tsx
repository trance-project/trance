import React from "react";
import { TransitionProps } from '@material-ui/core/transitions';
import Button from '@material-ui/core/Button';
import Dialog from '@material-ui/core/Dialog';
import AppBar from '@material-ui/core/AppBar';
import Toolbar from '@material-ui/core/Toolbar';
import IconButton from '@material-ui/core/IconButton';
import Typography from '@material-ui/core/Typography';
import CloseIcon from '@material-ui/icons/Close';
import Slide from '@material-ui/core/Slide';

import {RuntimeMetricsThemeStyle} from './RuntimeMetricsThemeStyle';
import {Grid} from "@material-ui/core";
import SimpleAreaGraphVShredded from "../ui/charts/SimpleBarChart/SimpleAreaGraphVShredded";
import StarBorderIcon from "@material-ui/icons/StarBorder";
import {ordered_merged} from '../ui/charts/SimpleBarChart/ordered_merged';
import PaperWithHeader from "../ui/Paper/PaperWithHeader/PaperWithHeader";




const Transition = React.forwardRef(function Transition(
    props: TransitionProps & { children?: React.ReactElement },
    ref: React.Ref<unknown>,
) {
    return <Slide direction="up" ref={ref} {...props} />;
});

interface _RuntimeMetricsProps{
    open: boolean;
    close:()=>void;
}

const RuntimeMetrics = (props: _RuntimeMetricsProps) => {
    const classes = RuntimeMetricsThemeStyle();
    return(
        <div>
            <Dialog fullScreen open={props.open} onClose={props.close} TransitionComponent={Transition}>
                <AppBar className={classes.appBar}>
                    <Toolbar>
                        <IconButton edge="start" color="inherit" onClick={props.close} aria-label="close">
                            <CloseIcon />
                        </IconButton>
                        <Typography variant="h6" className={classes.title}>
                            Runtime metrics
                        </Typography>
                    </Toolbar>
                </AppBar>
                <Grid container spacing={4} >
                    <Grid item xs={12} >
                        <PaperWithHeader heading={"Partition Size Distirbution"} className={classes.paper}>
                            <SimpleAreaGraphVShredded data={ordered_merged}/>
                        </PaperWithHeader>
                    </Grid>
                </Grid>
                <Button className={classes.btn} variant={"contained"} style={{'backgroundColor':'#d66123'}} onClick={()=> window.location.href = "http://localhost:18080"} endIcon={<StarBorderIcon/>}>Metrics</Button>
            </Dialog>
        </div>
    );
}

export default RuntimeMetrics;