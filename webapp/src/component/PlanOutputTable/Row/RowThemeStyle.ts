import { makeStyles } from '@material-ui/core/styles';

export const rowThemeStyle = makeStyles({
    root: {
        '& > *': {
            borderBottom: 'unset',
        },
    },
    tableCell:{
        padding: 5
    }
});