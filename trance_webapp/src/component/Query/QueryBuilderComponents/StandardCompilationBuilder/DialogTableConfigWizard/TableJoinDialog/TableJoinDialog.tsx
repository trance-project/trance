import React from 'react';
import Button from '@material-ui/core/Button';
import Dialog from '@material-ui/core/Dialog';
import DialogActions from '@material-ui/core/DialogActions';
import DialogContent from '@material-ui/core/DialogContent';
import DialogContentText from '@material-ui/core/DialogContentText';
import DialogTitle from '@material-ui/core/DialogTitle';
import PaperWithHeader from "../../../../../ui/Paper/PaperWithHeader/PaperWithHeader";
import ViewSelector from "../../../ViewSelector/ViewSelector";
import testData from "../../../../../../containers/QueryBuilder/testData";
import {Table} from "../../../../../../utils/Public_Interfaces";
import TextField from '@material-ui/core/TextField';


interface _TableJoinDialogProps {
    open:boolean;
    close:()=>void;
    joinOnClock:(table:Table)=>void;
    associationKeyChange: (input: React.ChangeEvent<HTMLInputElement>) => void;
    keyValue: string;
}

 const TableJoinDialog = (props:_TableJoinDialogProps) => {

    return (
        <div>
            <Dialog open={props.open} onClose={props.close} aria-labelledby="form-dialog-title" >
                <DialogTitle id="form-dialog-title">Association Object</DialogTitle>
                <DialogContent>
                    <DialogContentText>
                        <TextField id="filled-basic" label="Association Key" variant="filled" fullWidth  onChange={props.associationKeyChange} value={props.keyValue}/>
                    </DialogContentText>
                    <PaperWithHeader heading={'Inputs'} height={450}>
                        <ViewSelector tables={testData} clicked={props.joinOnClock}/>
                    </PaperWithHeader>
                </DialogContent>
                <DialogActions>
                    <Button onClick={props.close} color="primary" variant={"outlined"}>
                        Cancel
                    </Button>
                </DialogActions>
            </Dialog>
        </div>
    );
}

export default TableJoinDialog;