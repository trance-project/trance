import React from 'react';
import PaperWithHeader from "../../../../../../ui/Paper/PaperWithHeader/PaperWithHeader";
import TextField from "@material-ui/core/TextField";

import {useStyles} from './NrcCodeViewThemeStyle'



interface _NrcCodeViewProps {
    nrcCode: string;
}
const NrcCodeView = (props: _NrcCodeViewProps) => {
    const classes = useStyles();
    return (
        <PaperWithHeader className={"fill-height"} heading={"NRC Code"}>
            <TextField
                id="outlined-multiline-static"
                multiline
                rows={28}
                value={props.nrcCode}
                fullWidth
                style={{ padding: '10px'}}
                className={classes.nrcCodeView}
            />
        </PaperWithHeader>
    )
}

export default NrcCodeView