import express from "express";
import bodyParser from "body-parser";
import {
    BatchWriteItemCommand,
    CreateTableCommand,
    DeleteTableCommand,
    DynamoDBClient,
} from "@aws-sdk/client-dynamodb";
// import { fromEnv } from "@aws-sdk/credential-provider-env";
import serverless from "serverless-http";


// Create the DynamoDB service object
const dynamodb = new DynamoDBClient({
    // credentials: fromEnv(),
    region: "us-east-1"
});
const app = express();
// const port = 8080;

// Use body-parser middleware to parse incoming request bodies
app.use(bodyParser.json());

// POST endpoint to take graph and store it in DynamoDB
app.post('/', async (req, res) => {
    const { graph } = req.body;

    if (graph === undefined) {
        return res.status(400).json({ error: 'Graph must contain at least one vertex.' });
    }

    // Parse the graph and instantiate a new graph data structure
    const graphParsed = {};
    const vertices = new Set(); // represents cities

    graph.split(',').forEach(edge => {
        const [start, end] = edge.split('->');
        if (!graphParsed[start]) graphParsed[start] = {};
        graphParsed[start][end] = 1;

        // Create a separate object for reverse edges
        // if (!graphParsed[end]) graphParsed[end] = {};
        // graphParsed[end][start] = -1;

        vertices.add(start);
        vertices.add(end);
    });

    // Compute the shortest path between all pairs of vertices using Breadth First Search
    const result = calculateShortestDistances(graphParsed, vertices);
    // console.log("result: ", result)

    // Store the graph in DynamoDB table
    await storeGraphInDynamoDB(result).then(() => {
        res.json({ message: 'Graph stored in DynamoDB successfully.' });
    })
        .catch(err => {
            res.json({ message: 'Error storing graph in DynamoDB.', err });
        });
});

// Start the server
// app.listen(port, () => {
//     console.log(`Server is running at http://0.0.0.0:${port}`);
// });

// Function to store the graph in DynamoDB table
async function storeGraphInDynamoDB(graph) {
    // DynamoDB schema should have source, destination, and distance as attributes
    const params = {
        Item: {
            Source: { S: graph[0].source },
            Destination: { S: graph[0].destination },
            Distance: { N: graph[0].distance.toString() },
        },
        TableName: "Graphs",
    };
    // convert the graph to the format that DynamoDB expects
    let batchGraph = graph.map(item => {
        item.Source = { S: item.source };
        item.Destination = { S: item.destination };
        item.Distance = { N: item.distance.toString() };
        delete item.source;
        delete item.destination;
        delete item.distance;
        return item;
    })
    const batchParams = {
        RequestItems: {
            "Graphs": batchGraph.map(item => ({ PutRequest: { Item: item } })),
        },
    };
    console.log("params: ", params)
    console.log("batch graph: ", batchGraph)
    console.log("batch params: ", batchParams)
    // Clear the table and create a new one
    const createTableInput = { // CreateTableInput
        AttributeDefinitions: [ // AttributeDefinitions // required
            { // AttributeDefinition
                AttributeName: "Source", // required
                AttributeType: "S" // required
            },
            { // AttributeDefinition
                AttributeName: "Destination", // required
                AttributeType: "S" // required
            },
        ],
        TableName: "Graphs", // required
        KeySchema: [ // KeySchema // required
            { // KeySchemaElement
                AttributeName: "Source", // required
                KeyType: "HASH", // required
            },
            { // KeySchemaElement
                AttributeName: "Destination", // required
                KeyType: "RANGE", // required
            },
        ],
        ProvisionedThroughput: { // required
            ReadCapacityUnits: 1, // required
            WriteCapacityUnits: 1, // required
        },
    };
    dynamodb.send(new DeleteTableCommand({ TableName: "Graphs" }))
        .catch((err) => {
            if (err.name === "ResourceNotFoundException") {
                console.log("Table does not exist. Creating a new table.")
                dynamodb.send(new CreateTableCommand(createTableInput)).catch((err) => {
                    console.error("Error creating table: ", err);
                }).then(() => {
                    console.log("Table created successfully.");
                    setTimeout(() => {
                        console.log("10 seconds have passed.")
                        // dynamodb.send(new PutItemCommand(params))
                        dynamodb.send(new BatchWriteItemCommand(batchParams))
                            .catch((err) => {
                                console.error("Error writing to table a second time: ", err);
                            }).then(() => {
                                console.log("Graph stored in DynamoDB successfully, on the retry.");
                            });
                    }, 10000);
                });
            }
            if (err.name === "ResourceInUseException") {
                console.log("Table already exists. Deleting and creating a new table.")
            }
        }).then(() => {
            console.log("Table deleted successfully. Waiting 4 seconds before creating a new table...");
            setTimeout(() => {
                console.log("4 seconds have passed.")
                dynamodb.send(new CreateTableCommand(createTableInput)).then(() => {
                    console.log("Table created successfully. Waiting a few seconds before writing to table...");
                    setTimeout(() => {
                        console.log("8 seconds have passed.")
                        // dynamodb.send(new PutItemCommand(params))
                        dynamodb.send(new BatchWriteItemCommand(batchParams))
                            .then(() => {
                                console.log("Graph stored in DynamoDB successfully, on the retry.");
                            }).catch((err) => {
                                console.error("Error writing to table: ", err);
                            });
                    }, 8000);
                }).catch((err) => {
                    console.error("Error creating table: ", err);
                })
            }, 4000);
        });
}


// Function to compute the shortest path between all pairs of vertices using Dijkstra's algorithm
function calculateShortestDistances(graph, allVertices) {
    const result = [];

    allVertices.forEach(startNode => {
        const distances = {};
        const visited = new Set();
        const priorityQueue = new PriorityQueue();

        allVertices.forEach(vertex => {
            distances[vertex] = vertex === startNode ? 0 : Infinity;
            priorityQueue.enqueue(vertex, distances[vertex]);
        });

        while (!priorityQueue.isEmpty()) {
            const currentNode = priorityQueue.dequeue().element;
            visited.add(currentNode);

            for (const neighbor in graph[currentNode]) {
                const newDistance = distances[currentNode] + graph[currentNode][neighbor];

                if (newDistance < distances[neighbor]) {
                    distances[neighbor] = newDistance;
                    priorityQueue.updatePriority(neighbor, newDistance);
                }
            }
        }

        // Store the result for each pair of vertices
        allVertices.forEach(destination => {
            result.push({
                source: startNode,
                destination: destination,
                distance: distances[destination],
            });
        });
    });

    // Go through and change Infinity to -1 and remove the 0 distance from the result
    result.forEach(item => {
        if (item.distance === Infinity) {
            item.distance = -1;
        }
    });

    return result.filter(item => item.distance !== 0);
}

// Priority Queue implementation for Dijkstra's algorithm
class PriorityQueue {
    constructor() {
        this.elements = [];
    }

    enqueue(element, priority) {
        this.elements.push({ element, priority });
        this.sort();
    }

    dequeue() {
        if (this.isEmpty()) return null;
        return this.elements.shift();
    }

    updatePriority(element, newPriority) {
        this.elements.forEach(item => {
            if (item.element === element) {
                item.priority = newPriority;
            }
        });
        this.sort();
    }

    sort() {
        this.elements.sort((a, b) => a.priority - b.priority);
    }

    isEmpty() {
        return this.elements.length === 0;
    }
}


module.exports.handler = serverless(app);