import React from 'react';

import {modelMessageThemeStyle} from './modelMessageThemeStyle';
import {Backdrop, CircularProgress, Dialog, DialogActions, DialogContent, DialogTitle,Button} from "@material-ui/core";

interface _ModelMessageProps {
    open: boolean;
    close: () => void;
    successful: boolean
    message: {title:string,content:string} ;
}

const ModelMessage = (props: _ModelMessageProps) => {
    const classes = modelMessageThemeStyle();
    return(
        <div>
                <Backdrop className={classes.backdrop} open={props.open} onClick={props.close}>
                    <CircularProgress color={"inherit"}/>
                </Backdrop>
        </div>
    );
};

export default ModelMessage;