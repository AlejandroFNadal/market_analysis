// TimeSeriesChart.js
"use client"
import React, { useState, useEffect } from 'react';
import { Line } from 'react-chartjs-2';
import 'chart.js/auto';
import styles from '../styles/TimeSeries.module.css';


const TimeSeriesChart = () => {
    const [chartData, setChartData] = useState({});
    useEffect(() => {
        fetch('http://localhost:3000/api/getHistory').then(res => {
            if (!res.ok) {
                throw Error("Could not fetch data for that resource");
            }
            return res.json()
        }).then(data => {
            console.log(data);
        });
    }, []);
    const data = {
        labels: ['January', 'February', 'March', 'April', 'May', 'June', 'July'],
        datasets: [
            {
                label: 'Gas Price in USD',
                fill: false,
                lineTension: 0.1,
                backgroundColor: 'rgba(75,192,192,0.4)',
                borderColor: 'rgba(75,192,192,1)',
                borderCapStyle: 'butt',
                borderDash: [],
                borderDashOffset: 0.0,
                borderJoinStyle: 'miter',
                pointBorderColor: 'rgba(75,192,192,1)',
                pointBackgroundColor: '#fff',
                pointBorderWidth: 1,
                pointHoverRadius: 5,
                pointHoverBackgroundColor: 'rgba(75,192,192,1)',
                pointHoverBorderColor: 'rgba(220,220,220,1)',
                pointHoverBorderWidth: 2,
                pointRadius: 1,
                pointHitRadius: 10,
                data: [65, 59, 80, 81, 56, 55, 40]
            }
        ]
    };

    return (
        <div className={styles.graph}>
            <Line data={data} />
        </div>
    );
};

export default TimeSeriesChart;

