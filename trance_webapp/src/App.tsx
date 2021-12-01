import React from 'react';
import Modal from '@material-ui/core/Modal';
import CircularProgress from '@material-ui/core/CircularProgress';
import { makeStyles, Theme, createStyles } from '@material-ui/core/styles';
import Fade from '@material-ui/core/Fade';
import Button from "@material-ui/core/Button";
import Backdrop from '@material-ui/core/Backdrop';

import './App.css';
import {tranceTheme} from "./hoc/TranceTheme/TranceTheme";
import {ThemeProvider} from "@material-ui/core/styles";
import TranceRouter from "./TranceRouter";
import {useAppSelector, useAppDispatch} from './redux/Hooks/hooks';
import {clearMessage} from "./redux/RestApiErrorHandlerSlice/restApiErrorHandlerSlice"

/**
 * css style for CircularProgress for be aligned in the middle of the user page.
 * this will be often be found in it's own file {filename}ThemeStyle.ts
 */
const useStyles = makeStyles((theme: Theme) =>
    createStyles({
        modal: {
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
        },
        paper: {
            backgroundColor: theme.palette.background.paper,
            border: '2px solid #000',
            boxShadow: theme.shadows[5],
            padding: theme.spacing(2,4,3),
            margin: 'auto'
        }
    }),
);


function App() {
    const dispatch = useAppDispatch();
    const classes = useStyles();

    const loading = useAppSelector(state => state.restErrorHandle.loading);
    const errorMessage = useAppSelector(state => state.restErrorHandle.error);

    const closeModel = () => {
        dispatch(clearMessage());
    }

    const showModel = (loading : "idle" | "loading" | "error") => {
        switch (loading){
            case "idle":
                return false;
                break;
            case "error":
                return true;
                break;
            case "loading":
                return true;
                break;
            default:
                return false;
        }
    }

    const displayErrorMessage = loading === "error"?(
        <div className={classes.paper} >
            <h2 id="transition-modal-title">Error</h2>
            <p id="transition-modal-description">{errorMessage}</p>
            <Button variant={"contained"} onClick={closeModel} >Close</Button>
        </div>
    ) :<CircularProgress/>
  return (
      <ThemeProvider theme={tranceTheme}>
        <TranceRouter/>
          <Modal
              open={showModel(loading)}
              aria-labelledby="modal-for-api-request"
              aria-describedby="modal-used-to-load-request-with-success-or-failed"
              className={classes.modal}
              BackdropComponent={Backdrop}
              BackdropProps={{timeout:500}}
              closeAfterTransition

          >
              <Fade in={showModel(loading)}>
                  {displayErrorMessage}
              </Fade>
          </Modal>
      </ThemeProvider>
  );
}

export default App;
