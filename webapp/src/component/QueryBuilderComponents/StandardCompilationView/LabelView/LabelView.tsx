import React from "react";

import classes from './LabelView.module.css';
import {Typography} from "@material-ui/core";

interface _LabelViewProps{
    tableName: string;
    tableEl:string;
    joinString?:string;
    columns?: String[];
    sumBy?: boolean;

}

const LabelView = (props:_LabelViewProps) => {

    const sumByEl = props.sumBy? <Typography variant={"body1"}> sumBy<Typography component={"span"} style={{fontSize:'10px'}}>score gene(</Typography></Typography> : null;
     return(
        <div className={classes.LabelView} >
            {sumByEl}
            <Typography variant={"body1"}><Typography component={"span"}>for</Typography> {props.tableEl} <Typography component={"span"}> in </Typography> {props.tableName} <Typography component={"span"}> union </Typography></Typography>

            {props.joinString?<Typography variant={"body1"}><Typography component={"span"}>if</Typography> {props.joinString} <Typography component={"span"}>then</Typography> </Typography>:null}

            {props.columns?<Typography variant={"body1"}>{"{("} {props.columns.join(" , ")}</Typography>:null}
        </div>
    );
}

export default LabelView;