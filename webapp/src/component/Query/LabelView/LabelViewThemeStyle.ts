import {makeStyles} from '@material-ui/core/styles';

export const labelViewThemeStyle = makeStyles(theme => ({
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
    }
}))