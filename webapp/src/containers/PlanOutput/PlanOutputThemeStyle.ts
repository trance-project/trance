import { makeStyles } from '@material-ui/core/styles';

export const planOutputThemeStyle = makeStyles((theme) => ({
    paper: {
        padding: theme.spacing(2),
        display: 'flex',
        overflow: 'auto',
        flexDirection: 'column',
    },
    fixedHeight: {
        height: 240,
    },
    iconView:{
        float:"right",
        display:"inline-block",
        position:'relative',
        marginLeft:'96%',
        color: '#d66123'
    }
}));