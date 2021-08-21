import {makeStyles} from '@material-ui/core/styles';

export const RuntimeMetricsThemeStyle = makeStyles(theme => ({
    appBar:{
        position:'relative'
    },
    title: {
        marginLeft: theme.spacing(2),
        flex:1
    },
    heading: {
        fontSize: theme.typography.pxToRem(15),
        fontWeight:theme.typography.fontWeightRegular,
        height:20
    },
    paper: {
        padding: theme.spacing(2),
        display: 'flex',
        overflow: 'auto',
        flexDirection: 'column',
        height: '80vh',
    },
    btn:{
        margin:"auto",
        color:'#fff'
    }
}));