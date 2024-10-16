class Vertex:
    def __init__(self, key):
        self.id = key
        self.connected_to = {}

    def add_neighbor(self, nbr, weight=0):
        self.connected_to[nbr] = weight

    def __str__(self):
        return str(self.id) + ' connectedTo: ' + str([x.id for x in self.connected_to])

class Graph:
    def __init__(self):
        self.vert_list = {}
        self.num_vertices = 0

    def add_vertex(self, key):
        self.num_vertices += 1
        new_vertex = Vertex(key)
        self.vert_list[key] = new_vertex
        return new_vertex

    def get_vertex(self, n):
        if n in self.vert_list:
            return self.vert_list[n]
        else:
            return None

    def __contains__(self, n):
        return n in self.vert_list

    def add_edge(self, f, t, cost=0):
        if f not in self.vert_list:
            self.add_vertex(f)
        if t not in self.vert_list:
            self.add_vertex(t)
        self.vert_list[f].add_neighbor(self.vert_list[t], cost)

    def __iter__(self):
        return iter(self.vert_list.values())


# Example data, replace this with your actual data source
data = [
    {'parent_id': 'A', 'start_after_id': 'B'},
    {'parent_id': 'A', 'start_after_id': 'C'},
    {'parent_id': 'B', 'start_after_id': 'D'},
    {'parent_id': 'C', 'start_after_id': 'D'},
    {'parent_id': 'D', 'start_after_id': 'E'}
]

# Create the graph instance
graph = Graph()

# Populate the graph with vertices
for connection in data:
    graph.add_vertex(connection['parent_id'])
    graph.add_vertex(connection['start_after_id'])

# Populate the graph with edges
for connection in data:
    parent = connection['parent_id']
    child = connection['start_after_id']
    # Adding the edge with default weight, you can customize it if necessary
    graph.add_edge(parent, child)

# To display the graph
for vertex in graph:
    print(vertex)