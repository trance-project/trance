import React from 'react';
import Button from "@material-ui/core/Button";

import {useAppDispatch} from '../../../../../../redux/Hooks/hooks';
import {BlocklyNrcCode} from "../../../../../../utils/Public_Interfaces";
import {sendStandardNrcCode} from "../../../../../../redux/QuerySlice/thunkQueryApiCalls";

interface SendNrcCodeButtonProps {
    nrc : BlocklyNrcCode
}

const SendNrcCodeButton = (props : SendNrcCodeButtonProps) => {
    const dispatch = useAppDispatch();

    return (
        <Button variant="contained" color="primary" onClick={() => dispatch(sendStandardNrcCode(props.nrc))}>
            Send Code
        </Button>
    )
}

export default SendNrcCodeButton