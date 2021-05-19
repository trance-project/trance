import React, {useEffect} from "react";
import {Grid, Paper, Button} from "@material-ui/core";
import clsx from 'clsx';

import {planOutputThemeStyle} from './PlanOutputThemeStyle';
import PlanOutputTable from "../../component/PlanOutputComponents/PlanOutputTable";
import SimpleBarGraph from "../../component/ui/charts/SimpleBarChart/SimpleBarGraph";
import StarBorderIcon from '@material-ui/icons/StarBorder';
import RuntimeMetrics from "../../component/RuntimeMetrics/RuntimeMetrics";
import {AbstractTable} from '../../utils/Public_Interfaces';
import {rows} from '../../component/PlanOutputComponents/DemoData';

const PlanOutput = () => {
    const classes = planOutputThemeStyle();
    const fixedHeightPaper = clsx(classes.paper, classes.fixedHeight);
    const [open, setOpen] = React.useState(false);
    const [tableColumnsState, setTableColumnsState] = React.useState<AbstractTable>()

    /**
     * useEffect for componentDidMount to calculate to table headers
     * and sub table headers
     */
    useEffect(()=>{
        const process_columns = (name: string,jsonObject: any) => {
            const tableInfo: AbstractTable = {
                name: name,
                columnNames: [],
            }
            for( let [key, value] of Object.entries(jsonObject)){
                if(Array.isArray(value)){
                    const newObject = jsonObject[key]
                    tableInfo.subTables = (process_columns(key,newObject[0]));
                }else{
                    tableInfo.columnNames.push(key);
                }
            }
            return tableInfo;
            // headers.push(tableInfo);
        }
        console.log('[processColumns]',process_columns("sample",rows[0]));
            setTableColumnsState(process_columns("sample",rows[0]));
    }, []);

    const handleClickOpen = () => {
        setOpen(true);
    };

    const handleClose = () => {
        setOpen(false);
    };
return (
    <React.Fragment>
        <Button className={classes.iconView} variant={"contained"} style={{'backgroundColor':'#d66123'}} onClick={handleClickOpen} endIcon={<StarBorderIcon/>}>Metrics</Button>
        <Grid container spacing={3}>
            <Grid item xs={12}>
                <Paper className={fixedHeightPaper}>
                    <SimpleBarGraph/>
                </Paper>
            </Grid>
            <Grid item xs={12}>
                <Paper className={classes.paper}>
                    <PlanOutputTable tableHeaders={tableColumnsState}/>
                </Paper>
            </Grid>
        </Grid>
        <RuntimeMetrics open={open} close={handleClose}/>
    </React.Fragment>
)
}

export default PlanOutput;