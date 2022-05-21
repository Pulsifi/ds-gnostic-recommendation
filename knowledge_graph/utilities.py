from itertools import islice

def create_index(graph, name, label, attrs, fulltext=False, debug=False):
    
    if fulltext:
        cypher = f"""
        CREATE FULLTEXT INDEX {name} IF NOT EXISTS
        FOR (n:{label}) ON EACH
        [{",".join([f'n.{x}' for x in attrs])}]
        """
    else:
        cypher = f"""
        CREATE INDEX {name} IF NOT EXISTS
        FOR (n:{label}) ON
        ({",".join([f'n.{x}' for x in attrs])})
        """
    if debug:
        print(cypher)
    
    graph.run(cypher)

def periodic_input(graph, data, labels, batch_size=1000):
    stream = iter(data)
    while True:
        batch = list(islice(stream, batch_size))
        if len(batch) > 0:
            create_nodes(graph.auto(), batch, labels=labels)
        else:
            break