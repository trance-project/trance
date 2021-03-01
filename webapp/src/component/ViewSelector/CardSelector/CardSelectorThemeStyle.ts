import {makeStyles} from '@material-ui/core/styles';



const cardSelectorThemeStyle = makeStyles((theme) => ({
    root: {
        minWidth: 250,
        margin: "0 20px",
        height: 30,

    },
    title: {
        fontSize: 16,
    }
}));

export default cardSelectorThemeStyle;