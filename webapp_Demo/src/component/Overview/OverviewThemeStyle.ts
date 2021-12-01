import {makeStyles} from "@material-ui/core/styles";

const overviewThemeStyle = makeStyles((theme) => ({
    seeMore: {
        marginTop: theme.spacing(3)
    },
    paper: {
        padding: theme.spacing(2),
        display: 'flex',
        overflow: 'auto',
        flexDirection: 'column',
        height: 640
    }
}));

export default overviewThemeStyle;