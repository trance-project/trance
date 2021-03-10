import React from "react";
import {Grid, IconButton, Paper, Button} from "@material-ui/core";
import clsx from 'clsx';

import {planOutputThemeStyle} from './PlanOutputThemeStyle';
import PlanOutputTable from "../../component/PlanOutputTable/PlanOutputTable";
import SimpleBarGraph from "../../component/ui/Charts/SimpleBarChart/SimpleBarGraph";
import StarBorderIcon from '@material-ui/icons/StarBorder';
import ShreddedVStandard from "../../component/PlanResults/ShreddedVStandard/ShreddedVStandard";

const PlanOutput = () => {
    const classes = planOutputThemeStyle();
    const fixedHeightPaper = clsx(classes.paper, classes.fixedHeight);
    const [open, setOpen] = React.useState(false);

    const handleClickOpen = () => {
        setOpen(true);
    };

    const handleClose = () => {
        setOpen(false);
    };
return (
    <React.Fragment>
        <Button className={classes.iconView} variant={"contained"} style={{'backgroundColor':'#d66123'}} onClick={handleClickOpen} endIcon={<StarBorderIcon/>}>Metrics</Button>
        <Grid container spacing={3}>
            <Grid item xs={12}>
                <Paper className={fixedHeightPaper}>
                    <SimpleBarGraph/>
                </Paper>
            </Grid>
            <Grid item xs={12}>
                <Paper className={classes.paper}>
                    <PlanOutputTable/>
                </Paper>
            </Grid>
        </Grid>
        <ShreddedVStandard open={open} close={handleClose}/>
    </React.Fragment>
)
}

export default PlanOutput;