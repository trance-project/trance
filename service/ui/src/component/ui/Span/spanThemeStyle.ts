import {createStyles,makeStyles} from '@material-ui/core/styles';

const spanThemeStyle = makeStyles(theme =>
    createStyles({
        spanHighLight: {
            color: '#d1711b',
            fontFamily: 'Ubuntu Mono',
            fontWeight:700,
        },
        spanShrink:{
            fontSize: theme.typography.pxToRem(11),
            fontFamily: 'Ubuntu Mono',
        },
        spanAccent: {
            fontSize: theme.typography.pxToRem(11),
            verticalAlign: 'text-top',
            fontFamily: 'Ubuntu Mono',
        }
    })
)

export default spanThemeStyle