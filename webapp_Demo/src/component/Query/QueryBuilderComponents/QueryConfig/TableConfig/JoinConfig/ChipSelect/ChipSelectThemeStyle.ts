import {makeStyles} from '@material-ui/core/styles';

export const chipSelect= makeStyles(theme => ({
    formControl: {
        margin: theme.spacing(1),
        minWidth: 300,
        maxWidth: 600,
    },
    chips: {
        display: 'flex',
        flexWrap: 'wrap',
    },
    chip: {
        margin: 2
    },
    noLabel: {
        marginTop: theme.spacing(3),
    },
}))