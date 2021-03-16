import {createMuiTheme} from '@material-ui/core/styles';

export const tranceTheme = createMuiTheme({
    typography: {
        fontFamily:[
            'Lato'
        ].join('.'),
        fontSize:13,
        fontWeightRegular:300,
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