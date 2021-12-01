import {createStyles,makeStyles} from '@material-ui/core/styles';

const shreddedCompilationViewThemeStyle = makeStyles(theme =>
    createStyles({
        root: {
            width: '100%',
            height: 720,
            overflowY: 'auto',
        },
        heading: {
            fontSize: theme.typography.pxToRem(19),
            fontWeight:theme.typography.fontWeightMedium,
            fontFamily: 'Ubuntu Mono',
            height:20
        },
        body:{
            textAlign:'left',
            '& p':{
                fontSize: theme.typography.pxToRem(19),
                fontWeight:theme.typography.fontWeightMedium,
                fontFamily: 'Ubuntu Mono',
            }
        },
        accordion: {
            marginBottom: '5px'
        },
        accordion1lvl: {
            backgroundColor: 'rgba(141,158,145,0.3)'
        },
        accordion2lvl: {
            backgroundColor: 'rgba(0, 140, 212, 0.3)' //'rgba(139,122,140,0.3)'
        },
        accordion3lvl: {
            backgroundColor: 'rgba(179,130,181,0.3)'
        },
        accordionWhite: {
            backgroundColor: 'rgba(255,255,255,0.3)'
        },
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

export default shreddedCompilationViewThemeStyle