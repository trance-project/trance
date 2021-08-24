import React from "react";
import Typography from "@material-ui/core/Typography";
import Link from "@material-ui/core/Link";
import Grid from "@material-ui/core/Grid";


const CopyRight = () => (
    <Grid container>
        <Grid item xs={12} md={4}>
            <Typography variant={"body2"} color={"textSecondary"} align={"center"}>
                <Link color={"inherit"} onClick={()=> window.open(`http://www.cs.ox.ac.uk/projects/trance/`,"_blank")}>
                    Trance project page
                </Link>
            </Typography>
        </Grid>
        <Grid item xs={12} md={4}>
            <Typography variant={"body2"} color={"textSecondary"} align={"center"}>
                {'Copyright © '}
                <Link color={"inherit"} href={"/"}>
                    TraNCE
                </Link>{' '}
                {new Date().getFullYear()}
                {'.'}
            </Typography>
        </Grid>
        <Grid item xs={12} md={4}>
            <Typography variant={"body2"} color={"textSecondary"} align={"center"}>
                <Link color={"inherit"} onClick={()=> window.open(`https://github.com/jacmarjorie/trance`,"_blank")}>
                    Trance on github
                </Link>
            </Typography>
        </Grid>
    </Grid>
);


export default CopyRight;