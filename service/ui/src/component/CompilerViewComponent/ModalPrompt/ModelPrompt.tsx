import React from 'react';
import { makeStyles, Theme, createStyles } from '@material-ui/core/styles';
import Button from '@material-ui/core/Button';
import Dialog from '@material-ui/core/Dialog';
import DialogActions from '@material-ui/core/DialogActions';
import DialogContent from '@material-ui/core/DialogContent';
import DialogContentText from '@material-ui/core/DialogContentText';
import Slide from '@material-ui/core/Slide';
import { TransitionProps } from '@material-ui/core/transitions';
import FormLabel from '@material-ui/core/FormLabel';
import FormControl from '@material-ui/core/FormControl';
import FormControlLabel from '@material-ui/core/FormControlLabel';
import Checkbox from '@material-ui/core/Checkbox';

import {useAppDispatch, useAppSelector} from '../../../redux/Hooks/hooks';
import {getStandardPlan, getShreddedPlan} from '../../../redux/QuerySlice/thunkQueryApiCalls';

const useStyles = makeStyles((theme: Theme) =>
    createStyles({
        root: {
            display: 'flex',
        },
        formControl: {
            margin: theme.spacing(3),
        },
    }),
);

const Transition = React.forwardRef(function Transition(
    props: TransitionProps & { children?: React.ReactElement<any, any> },
    ref: React.Ref<unknown>,
) {
    return <Slide direction="up" ref={ref} {...props} />;
});

interface _ModalPromtProps{
   open: boolean,
   close: () => void;
   openIsLoading: ()=> void;
}

const ModalPromt = (props: _ModalPromtProps) => {
    const classes = useStyles();
    const [state, setState] = React.useState({
        standard: false,
        shredded: false,
    });
    const dispatch = useAppDispatch();
    const nrcCode = useAppSelector(state => state.query.nrcQuery);
    const selectedQuery = useAppSelector(state => state.query.selectedQuery);



    const handleChange = (event: React.ChangeEvent<HTMLInputElement>) => {
        setState({ ...state, [event.target.name]: event.target.checked });
    };

    const handleFormSubmit = () => {
        if(nrcCode.length === 0){
            return null;
        }
        if(state.standard){
                dispatch(getStandardPlan({
                    _id: selectedQuery!._id,
                    body: nrcCode,
                    title: selectedQuery!.name
                }));
        }
        if(state.shredded){
            dispatch(getShreddedPlan({
                _id: selectedQuery!._id,
                body: nrcCode,
                title: selectedQuery!.name
            }))
        }
        props.openIsLoading();
    }

    const { standard, shredded} = state;
    const error = [standard, shredded].filter((v) => v).length === 0;
    return (
            <Dialog
                open={props.open}
                TransitionComponent={Transition}
                keepMounted
                onClose={props.close}
                aria-labelledby="alert-dialog-slide-title"
                aria-describedby="alert-dialog-slide-description"
            >
                <DialogContent>
                    <DialogContentText id="alert-dialog-slide-description">
                        <FormControl required error={error} component={"span"} className={classes.formControl}>
                            <FormLabel>Pick at least one</FormLabel>
                                <FormControlLabel
                                    control={<Checkbox checked={standard} onChange={handleChange} name="standard" />}
                                    label="Standard"
                                />
                                <FormControlLabel
                                    control={<Checkbox checked={shredded} onChange={handleChange} name="shredded" />}
                                    label="Shredded"
                                />
                        </FormControl>
                    </DialogContentText>
                </DialogContent>
                <DialogActions>
                    <Button onClick={props.close} color="primary" variant={"outlined"} >
                        Cancel
                    </Button>
                    <Button onClick={handleFormSubmit} color="primary" variant={"contained"} disabled={error}>
                        Okay
                    </Button>
                </DialogActions>
            </Dialog>
    );
}

export default ModalPromt