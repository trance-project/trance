import React from 'react';
import Link from '@material-ui/core/Link';
import { makeStyles } from '@material-ui/core/styles';
import Typography from '@material-ui/core/Typography';

const preventDefault = (event: React.MouseEvent<HTMLAnchorElement, MouseEvent>) => event.preventDefault();

const useStyle = makeStyles({
    depositContext: {
        flex: 1
    }
})

const Deposits = () => {
    const classes = useStyle();

    return (
        <React.Fragment>
            <h2>Recent Deposits</h2>
            <Typography component="p" variant="h4">
                $3,024.00
            </Typography>
            <Typography color="textSecondary" className={classes.depositContext}>
                on 15 March, 2019
            </Typography>
            <div>
                <Link color="primary" href="#" onClick={preventDefault}>
                    View balance
                </Link>
            </div>
        </React.Fragment>
    )
}

export default Deposits