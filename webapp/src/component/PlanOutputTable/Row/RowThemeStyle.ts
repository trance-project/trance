import { makeStyles } from '@material-ui/core/styles';

export const rowThemeStyle = makeStyles(theme=>({
    root: {
        '& > *': {
            borderBottom: 'unset',
        },
    },
    noExpand:{
        transition: theme.transitions.create(["background", "background-color"], {duration: theme.transitions.duration.complex}),
        '&:hover': {
            backgroundColor: 'rgb(181,48,48,0.3)' ,
        }
    },
    expand:{
        transition: theme.transitions.create(["background", "background-color"], {duration: theme.transitions.duration.complex}),
        '&:hover':{
            backgroundColor: 'rgb(43,155,9,0.3)'
        }
    },
    tableCell:{
        padding: 5
    }
}));