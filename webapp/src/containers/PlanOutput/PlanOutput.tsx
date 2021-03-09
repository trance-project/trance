import React from "react";
import {Grid, IconButton, Paper} from "@material-ui/core";
import clsx from 'clsx';

import {planOutputThemeStyle} from './PlanOutputThemeStyle';
import PlanOutputTable from "../../component/PlanOutputTable/PlanOutputTable";
import SimpleBarGraph from "../../component/ui/Charts/SimpleBarChart/SimpleBarGraph";
import StarBorderIcon from '@material-ui/icons/StarBorder';

const PlanOutput = () => {
    const classes = planOutputThemeStyle();
    const fixedHeightPaper = clsx(classes.paper, classes.fixedHeight);
return (
    <React.Fragment>
        <IconButton className={classes.iconView}><StarBorderIcon/></IconButton>
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
    </React.Fragment>
)
}

export default PlanOutput;