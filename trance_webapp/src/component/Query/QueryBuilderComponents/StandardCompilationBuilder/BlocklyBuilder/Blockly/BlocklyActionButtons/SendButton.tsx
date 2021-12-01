import React from 'react';
import Button from "@material-ui/core/Button";

import {useAppDispatch, useAppSelector} from '../../../../../../../redux/Hooks/hooks';
import {sendStandardNrcCode} from "../../../../../../../redux/QuerySlice/thunkQueryApiCalls";

const SendButton = () => {
    const selectedQuery = useAppSelector(state => state.query.selectedQuery);
    const nrcCode = useAppSelector(state => state.query.nrcQuery);
    const dispatch = useAppDispatch();

    return (
        <Button
            variant="contained"
            color="primary"
            disabled={!(nrcCode.length > 0)}
            onClick={() => {dispatch(sendStandardNrcCode(
                {
                    body: nrcCode,
                    title: selectedQuery!.name
                }
            ))}}>
            Send
        </Button>
    )
}

export default SendButton