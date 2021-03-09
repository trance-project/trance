import React from "react";
import {Grid, Paper} from "@material-ui/core";
import clsx from 'clsx';

import {planOutputThemeStyle} from './PlanOutputThemeStyle';
import LineGraph from "../../component/ui/Charts/LineChart/LineGraph";
import PieGraph from "../../component/ui/Charts/PieChart/PieGraph";
import PlanOutputTable from "../../component/PlanOutputTable/PlanOutputTable";
import SimpleBarGraph from "../../component/ui/Charts/SimpleBarChart/SimpleBarGraph";

const PlanOutput = () => {
    const classes = planOutputThemeStyle();
    const fixedHeightPaper = clsx(classes.paper, classes.fixedHeight);
return (
    <Grid container spacing={3}>
        <Grid item xs={12} md={8} lg={9}>
            <Paper className={fixedHeightPaper}>
                <SimpleBarGraph/>
            </Paper>
        </Grid>
        <Grid item xs={12} md={4} lg={3}>
            <Paper className={fixedHeightPaper}>
                <PieGraph/>
            </Paper>
        </Grid>
        <Grid item xs={12}>
            <Paper className={classes.paper}>
                <PlanOutputTable/>
            </Paper>
        </Grid>
    </Grid>
)
}

export default PlanOutput;