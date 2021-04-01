import {makeStyles, createStyles} from "@material-ui/core/styles";

const tableConfigThemeStyle = makeStyles(
    createStyles({
        root: {
            height: 264,
            flexGrow: 1,
            maxWidth: 1000,
        },
    }),
);


export default tableConfigThemeStyle;