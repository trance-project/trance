import React from 'react';
import Grid from "@material-ui/core/Grid";
import Paper from "@material-ui/core/Paper";
import Container from "@material-ui/core/Container";
import {makeStyles} from "@material-ui/core/styles";
import Button from '@material-ui/core/Button';
import DragHandleIcon from '@material-ui/icons/DragHandle';
import ArrowBackIosIcon from '@material-ui/icons/ArrowBackIos';
import ArrowForwardIosIcon from '@material-ui/icons/ArrowForwardIos';
import TextField from '@material-ui/core/TextField';

const useStyles = makeStyles((theme) => ({
    appBarSpacer: theme.mixins.toolbar,
    content: {
        flexGrow: 1,
        height: '100vh',
        overflow: 'auto',
    },
    container: {
        paddingTop: theme.spacing(4),
        paddingBottom: theme.spacing(4),
    },
    paper: {
        padding: theme.spacing(2),
        display: 'flex',
        overflow: 'auto',
        flexDirection: 'column',
    },
    fixedHeight: {
        height: 40,
    },
    button: {
        margin: theme.spacing(1),
    }
}))

const ConditionContrainsts = () => {
    const classes = useStyles();
    return (
        <React.Fragment>
        <Container maxWidth={"lg"} className={classes.container}>
            <Grid container spacing={3}>
                <Grid item xs={12} md={4} lg={3}>
                        <Button variant="contained"
                        color={'primary'}
                        className={classes.button}
                        endIcon={<DragHandleIcon/>}
                        >
                            Equals
                        </Button>
                </Grid>
                <Grid item xs={12} lg={4}>
                    <Button variant="contained"
                            color={'primary'}
                            className={classes.button}
                            endIcon={<ArrowBackIosIcon/>}
                    >
                        Less Than
                    </Button>
                </Grid>
                <Grid item xs={12} lg={4}>
                    <Button variant="contained"
                            color={'primary'}
                            className={classes.button}
                            endIcon={<ArrowForwardIosIcon/>}
                    >
                        Greater Than
                    </Button>
                </Grid>
            </Grid>
        </Container>
        <Container maxWidth={"xs"} className={classes.container}>
            <Grid container>
                <Grid item xs={12}>
                  <Paper>
                      <TextField id="standard-basic" label="Column" />
                      <DragHandleIcon/>
                      <TextField id="standard-basic" label="Value" />
                  </Paper>
                </Grid>
                <Grid item xs={12}>
                    <Paper>
                        <TextField id="standard-basic" label="Column" />
                        <DragHandleIcon/>
                        <TextField id="standard-basic" label="Value" />
                    </Paper>
                </Grid>
                <Grid item xs={12}>
                    <Paper>
                        <TextField id="standard-basic" label="Column" />
                        <ArrowForwardIosIcon/>
                        <TextField id="standard-basic" label="Value" />
                    </Paper>
                </Grid>
            </Grid>
        </Container>
        </React.Fragment>
    );
}

export default ConditionContrainsts;