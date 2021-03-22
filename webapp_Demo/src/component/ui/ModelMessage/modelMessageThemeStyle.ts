import {makeStyles} from '@material-ui/core/styles/'

export const modelMessageThemeStyle = makeStyles(theme =>({
    backdrop:{
        zIndex: theme.zIndex.drawer+1,
        color:'#fff'
    }
}));