/* Get the DynamoDB GasCostPerHour table, make it a json and return it */
import { DynamoDBClient, ScanCommand } from "@aws-sdk/client-dynamodb";
const client = new DynamoDBClient({ region: "us-east-1" });
const command = new ScanCommand({ TableName: "GasCostPerHour" });

export default async function handler(req, res) {
    let responseBody = "";
    let statusCode = 0;

    try {
        const data = await client.send(command); 
        // the structure of the obtained data is
        // {"DATE":{"S":"2023-11-23"},"average":{"N":"733861747267148.5"},"transactions":{"N":"268"},"HOUR":{"S":"05"}}
        // we want to convert it to:
        // [{"DATE":"2023-11-23T05:00:00", average: 73...}]
        const result = [];
        for (let i = 0; i < data.Items.length; i++) {
          result.push({
            DATE: new Date(data.Items[i].DATE.S + "T" + data.Items[i].HOUR.S + ":00:00"),
            average: data.Items[i].average.N,
          });
        }
        // sort the result by date
        result.sort((a, b) => a.DATE - b.DATE);
        const labels = [];
        const values = [];
        for (let i = 0; i < result.length; i++) {
          labels.push(result[i].DATE);
          values.push(result[i].average);
        }
        statusCode = 200;
        responseBody = JSON.stringify({labels: labels, values: values});
    } catch (err) {
        responseBody = `Unable to get history: ${err}`;
        statusCode = 403;
    }

    const response = {
        statusCode: statusCode,
        headers: {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, POST, OPTIONS, PUT, DELETE"
        },
        body: responseBody
    };
    res.status(response.statusCode).send(response.body);
}


