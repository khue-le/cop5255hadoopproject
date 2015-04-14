Team Members
Nitin Gujral
Mayank Saini

Single Source Shortest Path in Huge Graph with Hadoop Map Reduce in Hadoop on Cluster.



In this project, we will implement Single Source Shortest path using Hadoop Map Reduce.

We will develop a map and reduce function to identify shortest path between source and destination node pair (identified by ID) in a very large undirected graph. The idea of this project was taken from Google Presentations http://code.google.com/edu/submissions/mapreduce-minilecture/lec5-pagerank.ppt

This is directly applicable to large social networks like facebook, twitter etc.
Graph will be realized using dynamic adjacency list with each node identified by a specific unique ID. Shortest path between two given nodes will be found which has the minimum total weight between source and destination.

Construction of Graph - Sample graph would be generated using another Map reduce function which would generate graphs given no. of edges to be formed. This would ensure consistency of the input and verification on huge graphs (having millions of nodes/edges).

Input to Map Reduce - Graphs would be represented by a dynamic adjacency list representation. Eg. If node1 is connected to node2 and have weight W then the input file would contain

Node1     Node2(W)
Node2     Node1(W)

Since the graph is undirected, both the nodes would have connection to each other to be consistent. The dynamic adjacency list is a comma separated list. Eg:   A     A(W1), B(W2).....