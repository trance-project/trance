import React from 'react';
import Button from '@material-ui/core/Button';
import TextField from '@material-ui/core/TextField';
import Dialog from '@material-ui/core/Dialog';
import DialogActions from '@material-ui/core/DialogActions';
import DialogContent from '@material-ui/core/DialogContent';
import DialogContentText from '@material-ui/core/DialogContentText';
import DialogTitle from '@material-ui/core/DialogTitle';
import PaperWithHeader from "../../../../../ui/Paper/PaperWithHeader/PaperWithHeader";
import JoinConfig from "../../../QueryConfig/TableConfig/JoinConfig/JoinConfig";
import GroupByConfig from "../../../QueryConfig/GroupBy/GroupByConfig";

interface _groupByDialogProps {
    open: boolean;
    close: () => void;
    groupByKey?: string;
    onChangeGroupBy?: (input: React.ChangeEvent<HTMLInputElement>) => void;
}

const GroupByDialog = (props:_groupByDialogProps) => {

    return (
        <div>
            <Dialog open={props.open} onClose={props.close} aria-labelledby="form-dialog-title-edit" fullWidth>
                <DialogTitle id="form-dialog-title-edit">Group by</DialogTitle>
                <DialogContent>
                    <TextField
                        autoFocus
                        id="Group_By"
                        label="key Attribute"
                        type="groupBy"
                        fullWidth
                        value={props.groupByKey}
                        onChange={props.onChangeGroupBy}
                    />
                    <PaperWithHeader heading={'Group By Attributes'} height={450}>
                        <GroupByConfig/>
                    </PaperWithHeader>
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

export default GroupByDialog;