import {makeStyles} from '@material-ui/core/styles'

export const paperWithHeaderThemeStyle = makeStyles(theme=>({
    header:{
        // marginRight:'auto',
        textAlign:"left",
        padding:10
    },
    divider:{
        marginBottom:10
    }
}))