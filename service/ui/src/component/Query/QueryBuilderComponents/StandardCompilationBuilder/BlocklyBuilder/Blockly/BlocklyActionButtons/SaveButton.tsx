import React from 'react';
import Button from "@material-ui/core/Button";

import {useAppDispatch} from '../../../../../../../redux/Hooks/hooks';
import {updateBlocklyQuery} from "../../../../../../../redux/QuerySlice/thunkQueryApiCalls";

const SaveButton = () => {
    const dispatch = useAppDispatch();

    return (
        <Button variant="contained" color="primary"  onClick={() => dispatch(updateBlocklyQuery())}>
            Save
        </Button>
    )
}

export default SaveButton