import React from "react";
import {AccordionDetails, Typography} from "@material-ui/core";


import {labelViewThemeStyle} from './LabelViewThemeStyle';


type LabelType = {
    for: string;
    tuple: string[];
    association: string;
    groupBy: string
}

interface _LabelViewProps {
    labelView : LabelType;
    selectNode?: ()=>void;
    hoverEvent?: () => void;
    abortHover?:()=>void;
    isSelected?:boolean;
    openJoinAction?: () => void;
    openEdit?: () => void;
    openGroupBy?: () => void;

}

const NewLabelView = (props:_LabelViewProps) => {
    const classes = labelViewThemeStyle();
    const styleString = (label: LabelType) => {
        const jsxElement: JSX.Element[]= [];
         const dubbedNrcFor = label.for.replaceAll('Union', 'Union;$;')
        const nrcFor = dubbedNrcFor.split(';$;');
        jsxElement.push(
            <Typography variant={"body1"} key={label.groupBy.substring(label.groupBy.indexOf('ReduceByKey')+11, label.groupBy.length)}>
                <Typography component={"span"}>{label.groupBy.substring(label.groupBy.indexOf('ReduceByKey'), label.groupBy.indexOf('ReduceByKey')+11)}</Typography>
                {label.groupBy.substring(label.groupBy.indexOf('ReduceByKey')+11, label.groupBy.length)}
            </Typography>
        )
        //styles for loop
        nrcFor.forEach(s => {
            if(s.includes('For '))
                jsxElement.push((
                    <Typography variant={"body1"} key={`${s.substring(s.indexOf('For '), s.indexOf(' Union')+ 6)}_${s.length}`}>
                        <Typography component={"span"}>{s.substring(s.indexOf('For '), s.indexOf('For ')+4)}</Typography>
                        {s.substring(s.indexOf('For ')+4, s.indexOf(' in '))}
                        <Typography component={"span"}>{s.substring(s.indexOf(' in '), s.indexOf(' in ')+4)}</Typography>
                        {s.substring(s.indexOf(' in ')+4, s.indexOf(' Union'))}
                        <Typography component={"span"}>{s.substring(s.indexOf(' Union'), s.indexOf(' Union')+ 6)}</Typography>
                    </Typography>
                ));
            if(s.includes(' If '))
                jsxElement.push((
                    <Typography variant={"body1"} key={`${s.substring(s.indexOf(' If '), s.indexOf(' Then ')+ 6)}_${s.length}`}>
                        <Typography component={"span"}>{s.substring(s.indexOf(' If '), s.lastIndexOf(' If ')+4)}</Typography>
                        {s.substring(s.indexOf(' If ')+4, s.indexOf(' Then '))}
                        <Typography component={"span"}>{s.substring(s.indexOf(' Then '), s.indexOf(' Then ') + 6)}</Typography>
                    </Typography>
                ));
        })

            jsxElement.push(<Typography
                variant={"body1"}
                key={`${label.tuple.join(' , ')}_${label.tuple.length}`}>
                {label.tuple.join(' , ')}
            </Typography>)
        // <Typography variant={"body1"}><Typography component={"span"}>for</Typography> {props.tableEl} <Typography component={"span"}> in </Typography> {props.tableName} <Typography component={"span"}> union </Typography></Typography>
        return jsxElement
    }
    return(
         <div className={classes.container} onClick={props.selectNode}>
            <div className={classes.root} onMouseEnter={props.hoverEvent} onMouseLeave={props.abortHover}>
                {styleString(props.labelView)}
            </div>
         </div>
    );
};

export default NewLabelView;
