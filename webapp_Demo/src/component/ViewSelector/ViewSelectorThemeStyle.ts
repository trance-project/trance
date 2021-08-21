import {makeStyles} from '@material-ui/core/styles';

export const viewSelectorThemeStyle = makeStyles(theme => ({
    root:{
        width: '100%',
        height:'auto',
        maxWidth:300,
        backgroundColor: theme.palette.background.paper
    }
}));