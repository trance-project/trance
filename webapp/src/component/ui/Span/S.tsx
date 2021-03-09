import React from "react";
import materializationThemeStyle
    from "../../Query/QueryBuilderComponents/QueryShredding/Materialzation/materializationThemeStyle";
import {Typography} from "@material-ui/core";

interface _spanInterface{
    children: React.ReactNode,
    variant: "highlight" | "shrink" | "accent"
}
const S = (props: _spanInterface) => {
    const classes = materializationThemeStyle();
    switch (props.variant) {
        case "highlight":
            return <Typography className={classes.spanHighLight} component={"span"}>{props.children}</Typography>
        case "shrink":
            return <Typography className={classes.spanShrink} component={"span"}>{props.children}</Typography>
        case "accent":
            return <Typography className={classes.spanAccent} component={"span"}>{props.children}</Typography>
    }
}

export default S