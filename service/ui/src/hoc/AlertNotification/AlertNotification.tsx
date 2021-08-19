import React from 'react';
import IconButton from '@material-ui/core/IconButton';
import {Alert} from "@material-ui/lab";
import {Grid} from "@material-ui/core";
import CloseIcon from '@material-ui/icons/Close';

import {useAppSelector,useAppDispatch} from "../../redux/Hooks/hooks";
import {clearMessage} from '../../redux/RestApiErrorHandlerSlice/restApiErrorHandlerSlice';
import NoteIcon from "@material-ui/icons/Note";
import {config} from "../../Constants";
import Button from "@material-ui/core/Button";

interface _AlertNotificationProps {
    children: React.ReactNode;
}

const AlertNotification = (props: _AlertNotificationProps) => {

    const dispatch = useAppDispatch();
    const alertMessage = useAppSelector(state => state.restErrorHandle.alertMessage);
    const notepadURL = useAppSelector(state => state.query.notepadUrl);
    let notification = <div></div>;

    const closeMessageHandler = () =>{
        dispatch(clearMessage());
    }

    if(alertMessage.message.length > 0 && alertMessage.alertType !== "none"){
        notification = (
            <Grid container spacing={3}>
                <Grid item xs={12}>
                    <Alert severity={alertMessage.alertType}
                        action={
                            (<React.Fragment>
                                    <Button variant={"outlined"} color={"inherit"} endIcon={<NoteIcon />} onClick={()=> window.open(config.zepplineURL+`${notepadURL}`,"_blank")}>Notebook</Button>
                                    <IconButton
                                        aria-label={"close"}
                                        color={"inherit"}
                                        size={"small"}
                                        onClick={closeMessageHandler}
                                        >
                                            <CloseIcon fontSize={"inherit"}/>
                                    </IconButton>
                            </React.Fragment>
                                )
                        }
                    >
                        {alertMessage.message}
                    </Alert>
                </Grid>
            </Grid>
        )
    }
    return (
        <React.Fragment>
            {notification}
            {props.children}
        </React.Fragment>
    )
}

export default AlertNotification