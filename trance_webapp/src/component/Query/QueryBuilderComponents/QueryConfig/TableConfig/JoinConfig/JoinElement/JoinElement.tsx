import React from 'react';
import Chip from '@material-ui/core/Chip';
import Paper from '@material-ui/core/Paper';

import {joinElementThemeStyle} from './JoinElementThemeStyle';

interface _ChipDataProps {
    keyElement: number;
    label: string[];
    delete:(i:number)=>void;
}

const JoinElement = (props:_ChipDataProps)  => {
    const classes = joinElementThemeStyle();



    return (
        <Paper component="ul" className={classes.root}>
            {props.label.map((el) => {
                return (
                    <li key={props.keyElement}>
                        <Chip
                            label={el}
                            onDelete={()=>props.delete(props.keyElement)}
                            className={classes.chip}
                        />
                    </li>
                );
            })}
        </Paper>
    );
}

export default JoinElement;