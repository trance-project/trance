import { makeStyles, createStyles } from '@material-ui/core/styles';

const planOutputTableThemeStyle = makeStyles((theme) =>
    createStyles({
        root: {
            '& > *': {
                marginTop: theme.spacing(2),
            },
            margin: 'auto'
        },
    }),
);

export default planOutputTableThemeStyle