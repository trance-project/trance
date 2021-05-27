import React from 'react';
import Button from "@material-ui/core/Button";

import {useAppDispatch} from '../../../../../../redux/Hooks/hooks';
import {BlocklyNrcCode} from "../../../../../../utils/Public_Interfaces";
import {updateBlocklyQuery} from "../../../../../../redux/QuerySlice/thunkQueryApiCalls";

interface SendNrcCodeButtonProps {

}

const SendNrcCodeButton = (props : SendNrcCodeButtonProps) => {
    const dispatch = useAppDispatch();

    return (
        <Button variant="contained" color="primary" onClick={() => dispatch(updateBlocklyQuery())}>
            Save
        </Button>
    )
}

export default SendNrcCodeButton