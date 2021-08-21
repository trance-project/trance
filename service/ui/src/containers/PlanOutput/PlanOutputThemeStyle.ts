import { makeStyles } from '@material-ui/core/styles';

export const planOutputThemeStyle = makeStyles((theme) => ({
    paper: {
        padding: theme.spacing(2),
        display: 'flex',
        overflow: 'auto',
        flexDirection: 'column',
    },
    fixedHeight: {
        height: 300,
    },
    iconView:{
        float:"right",
        position:'relative',
        color: '#fff',
        marginBottom:5,
        marginLeft:"auto"
    }
}));