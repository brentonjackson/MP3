import json
# import boto3
import math
import heapq
#That's the lambda handler, you can not modify this method
# the parameters from JSON body can be accessed like deviceId = event['deviceId']
def lambda_handler(event, context):
    # Instanciating connection objects with DynamoDB using boto3 dependency
    dynamodb = boto3.resource('dynamodb')
    client = boto3.client('dynamodb')
    bodyContent = event['body']
    inputGraph = json.loads(bodyContent)['graph']
    
    # Getting the table the table Temperatures object
    try:
        graphTable = dynamodb.Table('Graphs')
        delete_table_items(graphTable)
        tableItems = convertInputToItems(inputGraph)
        print(tableItems)
        for item in tableItems:
            graphTable.put_item(
              Item={
                    'source': item['source'],
                    'destination': item['destination'],
                    'distance': int(item['distance']),
                }
            )
        # graphTable.put_item(
        #   Item={
        #         'source': "Chicago",
        #         'destination': "Urbana",
        #         'distance': 1,
        #     }
        # )
        
        return {
            'statusCode': 200,
            'body': json.dumps('Succesfully inserted mapping!')
        }
    except Exception as err:
        print('Closing lambda function')
        print(err)
        return {
                'statusCode': 400,
                'body': json.dumps("An error occurred" + str(err))
        }
        
        
def delete_table_items(table):
    scan = table.scan()
    with table.batch_writer() as batch:
        for each in scan['Items']:
            batch.delete_item(
                Key={
                    'source': each['source'],
                    'destination': each['destination']
                }
            )
            
def convertInputToItems(inputGraph):
    graph_parsed = makeParsedGraph(inputGraph)
    shortest_paths = compute_shortest_paths(graph_parsed)
    tableItems = []
    for vertex in shortest_paths:
        for destination in shortest_paths[vertex]:
            tableItems.append({
                'source': vertex,
                'destination': destination,
                'distance': shortest_paths[vertex][destination]
            })
    return tableItems
    
def makeParsedGraph(inputGraph):
    graph_parsed = {}
    vertices = set()  # represents cities

    # first add everything to vertices
    for edge in inputGraph.split(','):
        start, end = edge.split('->')
        vertices.add(start)
        vertices.add(end)

    # next create parsed graph of edges and set all distances to Infinity but 0 for the vertex itself
    for vertex in vertices:
        graph_parsed[vertex] = {v: 0 if v == vertex else float('inf') for v in vertices}

    # finally, update the distances for the edges based on the input
    for edge in inputGraph.split(','):
        start, end = edge.split('->')
        graph_parsed[start][end] = 1
    
    return graph_parsed

def compute_shortest_paths(graph):
    shortest_paths = {}
    
    for vertex in graph:
        shortest_paths[vertex] = dijkstra(graph, vertex)

    # replace infinities with -1
    for vertex in shortest_paths:
        for destination in shortest_paths[vertex]:
            if shortest_paths[vertex][destination] == math.inf:
                shortest_paths[vertex][destination] = -1
    
    return shortest_paths

def dijkstra(graph, start):
    # Initialize distances and visited set
    distances = {vertex: math.inf for vertex in graph}
    distances[start] = 0
    visited = set()

    # Priority queue to store vertices and their distances
    priority_queue = [(0, start)]

    while priority_queue:
        current_distance, current_vertex = heapq.heappop(priority_queue)

        # Skip if already visited
        if current_vertex in visited:
            continue

        visited.add(current_vertex)

        for neighbor, weight in graph[current_vertex].items():
            distance = current_distance + weight

            # Update distance if a shorter path is found
            if distance < distances[neighbor]:
                distances[neighbor] = distance
                heapq.heappush(priority_queue, (distance, neighbor))

    return distances
    

# Test


inputGraph = "Chicago->Urbana,Urbana->Springfield,Chicago->Lafayette,Lafayette->Urbana"
tableItems = convertInputToItems(inputGraph)
print("Table Items: ")
for item in tableItems:
    print(item)