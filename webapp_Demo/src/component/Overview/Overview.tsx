import React from 'react';
import Link from '@material-ui/core/Link';
import Table from '@material-ui/core/Table';
import TableBody from '@material-ui/core/TableBody';
import TableCell from '@material-ui/core/TableCell';
import TableHead from '@material-ui/core/TableHead';
import TableRow from '@material-ui/core/TableRow';
import {ButtonGroup} from "@material-ui/core";
import Button from "@material-ui/core/Button";
import Grid from "@material-ui/core/Grid";
import Paper from "@material-ui/core/Paper";

import overviewThemeStyle from "./OverviewThemeStyle";

const createData = (id: number, date: string, name:string, tables: string, groupedBy: string) => ({id, date, name, tables, groupedBy, });


const rows = [
    createData(0, '12 Mar, 2021', 'GeneLikelihoodPerMutation', 'Samples,Occurrences,CopyNumber', 'All'),
    createData(1, '12 Mar, 2021', 'GeneLikelihoodPerSample', 'Samples,GeneLikelihoodPerMutation', 'Shredded'),
    createData(2, '12 Mar, 2021', 'GeneImpactPerMutation', 'Samples,Occurrences', 'Standard'),
    createData(3, '12 Mar, 2021', 'GeneImpactPerSample', 'Samples,Occurrences', 'All'),
    createData(4, '12 Mar, 2021', 'HybridScoreMatrix', 'Occurrences,CopyNumber', 'Shredded'),
    createData(5, '11 Mar, 2021', 'EffectScoreMatrix', 'HyrbidScoreMatrix,Network', 'Shredded'),
    createData(6, '11 Mar, 2021', 'NetworkEffects', 'HybridScoreMatrix,GeneExpression', 'All'),
    createData(7, '11 Mar, 2021', 'GeneBurden', 'Genes,Variants', 'All'),
    createData(8, '11 Mar, 2021', 'PathwayBurden', 'GeneBurden', 'Standard'),
    createData(9, '11 Mar, 2021', 'OccurrenceBySample', 'Sample,Occurrences', 'Shredded'),
    createData(10, '10 Mar, 2021', 'HybridByMutation', 'Sample,Occurrences,CopyNumber', 'Standard'),
    createData(11, '10 Mar, 2021', 'HybridBySample', 'Sample,Occurrences,CopyNumber', 'Shredded'),
    createData(12, '10 Mar, 2021', 'GeneLikelihoodPerMutation', 'Samples,Occurrences,CopyNumber', 'All'),
    createData(13, '10 Mar, 2021', 'GeneLikelihoodPerSample', 'Samples,GeneLikelihoodPerMutation', 'Shredded'),
    createData(14, '10 Mar, 2021', 'GeneImpactPerMutation', 'Samples,Occurrences', 'Standard'),
    createData(15, '9 Mar, 2021', 'GeneImpactPerSample', 'Samples,Occurrences', 'All'),
    createData(16, '9 Mar, 2021', 'HybridScoreMatrix', 'Occurrences,CopyNumber', 'Shredded'),
    createData(17, '9 Mar, 2021', 'EffectScoreMatrix', 'HyrbidScoreMatrix,Network', 'Shredded'),
    createData(18, '9 Mar, 2021', 'NetworkEffects', 'HybridScoreMatrix,GeneExpression', 'All'),
    createData(19, '8 Mar, 2021', 'GeneBurden', 'Genes,Variants', 'All'),
    createData(20, '8 Mar, 2021', 'PathwayBurden', 'GeneBurden', 'Standard'),
    createData(21, '8 Mar, 2021', 'OccurrenceBySample', 'Sample,Occurrences', 'Shredded'),
    createData(22, '7 Mar, 2021', 'HybridByMutation', 'Sample,Occurrences,CopyNumber', 'Standard'),
    createData(23, '6 Mar, 2021', 'HybridBySample', 'Sample,Occurrences,CopyNumber', 'Shredded'),
];

const preventDefault = (event:  React.MouseEvent<HTMLAnchorElement, MouseEvent>) => event.preventDefault();


const Overview = () => {
    const classes = overviewThemeStyle();
    return (
        <Grid item xs={12} md={12} lg={12}>
            <Paper className={classes.paper}>
                <h2>Recent Queries</h2>
                <Table size="small">
                    <TableHead>
                        <TableRow>
                            <TableCell>Date</TableCell>
                            <TableCell>Name</TableCell>
                            <TableCell>Inputs</TableCell>
                            <TableCell>Compilation</TableCell>
                            <TableCell>Actions</TableCell>
                        </TableRow>
                    </TableHead>
                    <TableBody>
                        {rows.map((row) => (
                            <TableRow key={row.id}>
                                <TableCell>{row.date}</TableCell>
                                <TableCell>{row.name}</TableCell>
                                <TableCell>{row.tables}</TableCell>
                                <TableCell>{row.groupedBy}</TableCell>
                                <TableCell>
                                    <ButtonGroup color={"primary"} aria-label={"Contained primary button group"}>
                                        <Button variant={"contained"} style={{'backgroundColor':'#2980b9'}}>Edit</Button>
                                        <Button variant={"contained"} style={{'backgroundColor':'#2ecc71'}}>Execute</Button>
                                        <Button variant={"contained"} style={{'backgroundColor':'#e74c3c'}}>Delete</Button>
                                    </ButtonGroup>
                                </TableCell>
                            </TableRow>
                        ))}
                    </TableBody>
                </Table>
                <div className={classes.seeMore}>
                    <Link color="primary" href="#" onClick={preventDefault}>
                        See more orders
                    </Link>
                </div>
            </Paper>
        </Grid>
    );
}

export default Overview;