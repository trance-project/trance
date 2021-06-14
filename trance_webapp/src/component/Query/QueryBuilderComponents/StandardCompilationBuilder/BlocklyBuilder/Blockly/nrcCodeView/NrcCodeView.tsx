import React, {ChangeEvent, useState, useEffect} from 'react';
import PaperWithHeader from "../../../../../../ui/Paper/PaperWithHeader/PaperWithHeader";
import TextField from "@material-ui/core/TextField";

import {useStyles} from './NrcCodeViewThemeStyle';
import {useAppSelector, useAppDispatch} from '../../../../../../../redux/Hooks/hooks';
import {setNrcCode} from '../../../../../../../redux/QuerySlice/querySlice';

const NrcCodeView = () => {
    const nrcCode = useAppSelector(state => state.query.nrcQuery);
    const classes = useStyles();
    const dispatch = useAppDispatch();


    const onUserChange = (value: ChangeEvent<HTMLTextAreaElement | HTMLInputElement>) => {
        console.log(value.target.value);
        dispatch(setNrcCode(value.target.value))

    }
    return (
        <PaperWithHeader className={"fill-height"} heading={"NRC Code"}>
            <TextField
                id="outlined-multiline-static"
                multiline
                rows={28}
                value={nrcCode}
                fullWidth
                style={{ padding: '10px'}}
                className={classes.nrcCodeView}
                onChange={onUserChange}
            />
        </PaperWithHeader>
    )
}

export default NrcCodeView