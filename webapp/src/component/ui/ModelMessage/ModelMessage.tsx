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
            {props.successful?
                <Backdrop className={classes.backdrop} open={props.open} onClick={props.close}>
                    <CircularProgress color={"inherit"}/>
                </Backdrop>
                :
                <Dialog
                    open={props.open}
                    onClose={props.close}
                    aria-labelledby="alert-dialog-title"
                    aria-describedby="alert-dialog-description">
                    <DialogTitle id={"alert-dialog-title"}>{props.message.title}</DialogTitle>
                    <DialogContent>{props.message.content}</DialogContent>
                    <DialogActions>
                        <Button onClick={props.close}>Okay</Button>
                    </DialogActions>
                </Dialog>
            }
        </div>
    );
};

export default ModelMessage;