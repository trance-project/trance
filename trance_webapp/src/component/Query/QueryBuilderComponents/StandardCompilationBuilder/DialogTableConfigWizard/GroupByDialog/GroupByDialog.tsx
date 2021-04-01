import React from 'react';
import Button from '@material-ui/core/Button';
import Dialog from '@material-ui/core/Dialog';
import DialogActions from '@material-ui/core/DialogActions';
import DialogContent from '@material-ui/core/DialogContent';
import DialogTitle from '@material-ui/core/DialogTitle';
import GroupByConfig from "../../../QueryConfig/GroupBy/GroupByConfig";
import {Grid, Paper} from "@material-ui/core";
import Radio from '@material-ui/core/Radio';
import RadioGroup from '@material-ui/core/RadioGroup';
import FormControlLabel from '@material-ui/core/FormControlLabel';
import FormControl from '@material-ui/core/FormControl';

interface _groupByDialogProps {
    open: boolean;
    close: () => void;
}

const GroupByDialog = (props:_groupByDialogProps) => {
    const [value, setValue] = React.useState('Group_By');

    const handleChange = (event: React.ChangeEvent<HTMLInputElement>) => {
        setValue((event.target as HTMLInputElement).value);
    };

    return (
        <div>
            <Dialog open={props.open} onClose={props.close} aria-labelledby="form-dialog-title-edit" fullWidth>
                <DialogTitle id="form-dialog-title-edit">Group by</DialogTitle>
                <DialogContent>
                    <FormControl component="fieldset" fullWidth>
                        <RadioGroup aria-label="gender" name="gender1" value={value} onChange={handleChange}>
                            <Grid container spacing={1}>
                                <Grid item xs={6}>
                                    <FormControlLabel value="Group_By" control={<Radio />} label="Group By" />
                                </Grid>
                                <Grid item xs={6}>
                                    <FormControlLabel value="Sum_By" control={<Radio />} label="Sum By" />
                                </Grid>
                            </Grid>
                        </RadioGroup>
                    </FormControl>
                    <Paper style={{"height":450}} elevation={4}>
                        <GroupByConfig onClickGroup={props.close}/>
                    </Paper>
                </DialogContent>
                <DialogActions>
                    <Button onClick={props.close} color="primary" variant={"outlined"}>
                        Cancel
                    </Button>
                    {/*<Button onClick={props.close} color="primary" variant={"contained"}>*/}
                    {/*    Okay*/}
                    {/*</Button>*/}
                </DialogActions>
            </Dialog>
        </div>
    );
}

export default GroupByDialog;