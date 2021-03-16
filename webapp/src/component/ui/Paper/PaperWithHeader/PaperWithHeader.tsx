import React from "react";

import {paperWithHeaderThemeStyle} from './PaperWithHeaderThemeStyle';
import Paper from "@material-ui/core/Paper";
import { Divider, Typography} from "@material-ui/core";

interface _PaperWithHeaderProps {
    heading:string;
    height?: number;
    children?:React.ReactNode;
    className?:string;
    width?:number;
}

const PaperWithHeader = (props:_PaperWithHeaderProps) => {
    const classes = paperWithHeaderThemeStyle();
    return (
        <Paper style={{"height":props.height, "width": props.width}} className={props.className}>
            <Typography className={classes.header} variant={"h6"}>{props.heading}</Typography>
            <Divider className={classes.divider}/>
            {props.children}
        </Paper>
    )
}

export default PaperWithHeader;