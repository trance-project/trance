import React from 'react';
import Button from '@material-ui/core/Button';
import TextField from '@material-ui/core/TextField';
import Dialog from '@material-ui/core/Dialog';
import DialogActions from '@material-ui/core/DialogActions';
import DialogContent from '@material-ui/core/DialogContent';
import DialogTitle from '@material-ui/core/DialogTitle';

interface _queryEditDialogProps{
    open:boolean;
    close:()=>void;
    value: string;
    onChangeEdit: (input: React.ChangeEvent<HTMLInputElement>) => void;
}

const QueryEditDialog = (props:_queryEditDialogProps) => {

    return (
        <div>
            <Dialog open={props.open} onClose={props.close} aria-labelledby="form-dialog-title-edit" fullWidth>
                <DialogTitle id="form-dialog-title-edit">Edit</DialogTitle>
                <DialogContent>
                    <TextField
                        autoFocus
                        id="query"
                        label="Query"
                        type="query"
                        fullWidth
                        value={props.value}
                        onChange={props.onChangeEdit}
                    />
                </DialogContent>
                <DialogActions>
                    <Button onClick={props.close} color="primary" variant={"outlined"}>
                        Cancel
                    </Button>
                    <Button onClick={props.close} color="primary" variant={"contained"}>
                        Okay
                    </Button>
                </DialogActions>
            </Dialog>
        </div>
    );
}

export default QueryEditDialog;