import {makeStyles} from '@material-ui/core/styles';

export const labelViewThemeStyle = makeStyles(theme => ({
    container:{
        display: "flex",
        justifyContent: "space-between",
    },
    root:{
        textAlign:"left",
        fontFamily: 'Ubuntu Mono',
        '& p': {
            margin:0,
            fontFamily:'inherit'
        },
        '& span': {
            color:'#d1711b',
            fontFamily:'inherit',
            fontWeight:700,
            }
    },
    actionButton: {
        '& > *': {
            margin: theme.spacing(0.5),
        },
    },
    actionDisableButton: {
        display:"none",
    }

    }))