import React from "react";
import Paper from '@material-ui/core/Paper';
import Typography from '@material-ui/core/Typography';

import cardSelectorThemeStyle from "./CardSelectorThemeStyle";
import {Table} from "../../../../../utils/Public_Interfaces";

interface CardSelectorProps{
    table: Table;
    clicked: (table:Table) => void;
}

const CardSelector = (props:CardSelectorProps) => {
    const classes = cardSelectorThemeStyle();

    return (
        <Paper className={classes.root} elevation={6} onClick={() => props.clicked(props.table)}>
                <Typography className={classes.title} color="textSecondary" gutterBottom>
                    {props.table.name}
                </Typography>
        </Paper>
    );
}

export default CardSelector;