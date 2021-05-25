import React, {useRef} from "react";
import { BarChart, Bar, Cell, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer} from 'recharts';

interface _SimpleBarGraphVShreddedProps{
    data: any[];
}

const SimpleBarGraphVShredded = (props:_SimpleBarGraphVShreddedProps) => {

    return (
        <React.Fragment>
            <ResponsiveContainer width="100%" height="100%">
                <BarChart
                    width={500}
                    height={300}
                    data={props.data}
                    margin={{
                        top: 5,
                        right: 30,
                        left: 20,
                        bottom: 5,
                    }}
                >
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey={"pid"}/>
                    <YAxis />
                    <Tooltip/>
                    <Legend />
                    <Bar dataKey="read_rows" fill="#1abc9c"  name={"Read rows"}/>
                    <Bar dataKey="read_size" fill="#f1c40f"  name={"Read size"}/>
                    <Bar dataKey="write_rows" fill="#e74c3c"  name={"Write rows"}/>
                    <Bar dataKey="write_size" fill="#d35400"  name={"write size"}/>
                </BarChart>
            </ResponsiveContainer>
        </React.Fragment>
    );
}

export default SimpleBarGraphVShredded