import React from 'react';
import Button from '@material-ui/core/Button';
import TextField from '@material-ui/core/TextField';
import Dialog from '@material-ui/core/Dialog';
import DialogActions from '@material-ui/core/DialogActions';
import DialogContent from '@material-ui/core/DialogContent';
import DialogContentText from '@material-ui/core/DialogContentText';
import DialogTitle from '@material-ui/core/DialogTitle';

interface _NewQueryDialogProps{
    open:boolean;
    close:()=>void;
    onClickEvent:(input:string)=>void;
}

const NewQueryDialog = (props:_NewQueryDialogProps) => {

    const [newQueryNameState, setNewQueryState] = React.useState("");

    const handleOnChange = (event: React.ChangeEvent<HTMLInputElement>) => {
        setNewQueryState(event.target.value);
    }

    return (
        <div>
            <Dialog open={props.open} onClose={props.close} aria-labelledby="form-dialog-title">
                <DialogTitle id="form-dialog-title">New Query</DialogTitle>
                <DialogContent>
                    <TextField
                        autoFocus
                        margin="dense"
                        id="new_query"
                        label="New Query"
                        type="input"
                        fullWidth
                        onChange={handleOnChange}
                        value={newQueryNameState}
                    />
                </DialogContent>
                <DialogActions>
                    <Button onClick={props.close} color="primary">
                        Cancel
                    </Button>
                    <Button onClick={() => props.onClickEvent(newQueryNameState)} color="primary" variant={"contained"}>
                        Okay
                    </Button>
                </DialogActions>
            </Dialog>
        </div>
    );
}

export default NewQueryDialog;