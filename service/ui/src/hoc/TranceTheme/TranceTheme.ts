import {createMuiTheme} from '@material-ui/core/styles';

/**
 * Theme file for the overall project, allows you to set style and color scheme on most of the material ul jsx elements
 */

export const tranceTheme = createMuiTheme({
    typography: {
        fontFamily:[
            'Lato'
        ].join('.'),
        fontSize:13,
        fontWeightRegular:500,
        h6:{
            fontWeight:400,
            textTransform:'capitalize',
            fontSize:'120%'
        },
        h3:{
            fontWeight:400,
            textTransform:'capitalize',
            fontSize:'210%'
        }
    },
    palette:{
        primary:{
            main:'#74b9ff',
        }
    },

});