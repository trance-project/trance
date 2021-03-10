import {createStyles,makeStyles} from '@material-ui/core/styles';

const materializationThemeStyle = makeStyles(theme =>
    createStyles({
        root: {
            width: '100%',
            height: 720,
            overflowY: 'auto'
        },
        heading: {
            fontSize: theme.typography.pxToRem(15),
            fontWeight:theme.typography.fontWeightRegular,
            height:20
        },
        body:{
            textAlign:'left',
            '& p':{
                fontSize: theme.typography.pxToRem(12),
            }
        },

        accordion1lvl: {
            backgroundColor: 'rgba(141,158,145,0.8)'
        },
        accordion2lvl: {
            backgroundColor: 'rgba(139,122,140,0.8)'
        },
        accordion3lvl: {
            backgroundColor: 'rgba(179,130,181,0.8)'
        },
        accordionWhite: {
            backgroundColor: 'rgba(255,255,255,0.3)'
        },
        spanHighLight: {
            color: '#2000FF'
        },
        spanShrink:{
            fontSize: theme.typography.pxToRem(11)
        },
        spanAccent: {
            fontSize: theme.typography.pxToRem(11),
            verticalAlign: 'text-top'
        }
    })
)

export default materializationThemeStyle