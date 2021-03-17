import React from 'react';
import { makeStyles, Theme, createStyles } from '@material-ui/core/styles';
import Button from '@material-ui/core/Button';
import Dialog from '@material-ui/core/Dialog';
import DialogActions from '@material-ui/core/DialogActions';
import DialogContent from '@material-ui/core/DialogContent';
import DialogContentText from '@material-ui/core/DialogContentText';
import DialogTitle from '@material-ui/core/DialogTitle';
import Slide from '@material-ui/core/Slide';
import { TransitionProps } from '@material-ui/core/transitions';
import FormLabel from '@material-ui/core/FormLabel';
import FormControl from '@material-ui/core/FormControl';
import FormGroup from '@material-ui/core/FormGroup';
import FormControlLabel from '@material-ui/core/FormControlLabel';
import FormHelperText from '@material-ui/core/FormHelperText';
import Checkbox from '@material-ui/core/Checkbox';

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

    const handleChange = (event: React.ChangeEvent<HTMLInputElement>) => {
        setState({ ...state, [event.target.name]: event.target.checked });
    };

    const { standard, shredded} = state;
    const error = [standard, shredded].filter((v) => v).length === 0;
    return (
        <div>
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
                        <FormControl required error={error} component="fieldset" className={classes.formControl}>
                            <FormLabel component="legend">Pick at least one</FormLabel>
                            <FormGroup>
                                <FormControlLabel
                                    control={<Checkbox checked={standard} onChange={handleChange} name="standard" />}
                                    label="Standard"
                                />
                                <FormControlLabel
                                    control={<Checkbox checked={shredded} onChange={handleChange} name="shredded" />}
                                    label="Shredded"
                                />
                            </FormGroup>
                        </FormControl>
                    </DialogContentText>
                </DialogContent>
                <DialogActions>
                    <Button onClick={props.close} color="primary" variant={"outlined"} >
                        Cancel
                    </Button>
                    <Button onClick={props.openIsLoading} color="primary" variant={"contained"} disabled={error}>
                        Okay
                    </Button>
                </DialogActions>
            </Dialog>
        </div>
    );
}

export default ModalPromt