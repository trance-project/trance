import React, {useEffect} from "react";
import {Grid, Paper, Button} from "@material-ui/core";
import clsx from 'clsx';

import {planOutputThemeStyle} from './PlanOutputThemeStyle';
import PlanOutputTable from "../../component/PlanOutputComponents/PlanOutputTable";
import SimpleBarGraph from "../../component/ui/charts/SimpleBarChart/SimpleBarGraph";
import StarBorderIcon from '@material-ui/icons/StarBorder';
import RuntimeMetrics from "../../component/RuntimeMetrics/RuntimeMetrics";
import {AbstractTable,TableGraphMetaInfo} from '../../utils/Public_Interfaces';
import {rows} from '../../component/PlanOutputComponents/DemoData';
import Typography from "@material-ui/core/Typography";
import {Alert} from "@material-ui/lab";

const PlanOutput = () => {
    const classes = planOutputThemeStyle();
    const fixedHeightPaper = clsx(classes.paper, classes.fixedHeight);
    const [open, setOpen] = React.useState(false);
    const [tableColumnsState, setTableColumnsState] = React.useState<AbstractTable>()
    const [tableListState, setTableListState] = React.useState<TableGraphMetaInfo[]>()

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
        const tableInfo = process_columns("sample",rows[0])
        const tableGraphData = graphData(tableInfo, rows);
        console.log("[tableInfo]", tableInfo);
        console.log("[tableGraphData]", tableGraphData);
            setTableColumnsState(tableInfo);

    }, []);

    const handleClickOpen = () => {
        setOpen(true);
    };

    const handleClose = () => {
        setOpen(false);
    };

    const graphData = (tableInfo: AbstractTable, data: any[]) =>{
        if(tableInfo.subTables){
            // @ts-ignore
           const graphData = data.map((d,i) =>{
                let count = 0;
               // @ts-ignore
               if(Array.isArray(d[tableInfo.subTables!.name])){
                   // @ts-ignore
                   count = d[tableInfo.subTables!.name].length
               }
                   // @ts-ignore
               return {id: i, count: count}
            }) as TableGraphMetaInfo[]
            setTableListState(graphData)
        }
    }
return (
    <React.Fragment>
        <Grid container spacing={3}>
            <Grid item xs={12}>
                <Alert severity="warning">
                    Report displaying example data — <strong>for demo purposes</strong>
                </Alert>
            </Grid>
        </Grid>
        <Button className={classes.iconView} variant={"contained"} style={{'backgroundColor':'#d66123'}} onClick={handleClickOpen} endIcon={<StarBorderIcon/>}>Metrics</Button>
        <Grid container spacing={3}>
            <Grid item xs={12}>
                <Paper className={fixedHeightPaper}>
                    <Typography variant={"body1"}>Report of nested collections of plan output via index position</Typography>
                    <SimpleBarGraph height={250} data={tableListState}/>
                </Paper>
            </Grid>
            <Grid item xs={12}>
                <Paper className={classes.paper}>
                    <PlanOutputTable tableHeaders={tableColumnsState} onSelect={graphData}/>
                </Paper>
            </Grid>
        </Grid>
        <RuntimeMetrics open={open} close={handleClose}/>
    </React.Fragment>
)
}

export default PlanOutput;