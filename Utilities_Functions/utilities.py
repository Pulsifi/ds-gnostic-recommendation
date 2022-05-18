#########################################
# Neo4J Setup #
#########################################


class EnvironmentVariableNotFound(Exception):
    pass

def getenv(name):
    import os
    val = os.getenv(name)
    if not val:
        raise EnviromentVariableNotFound(name)
    return val


#########################################
# Data Import to Neo4J #
#########################################


def reset(graph):
    graph.run("MATCH (n) DETACH DELETE n")
    
    
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
    from itertools import islice
    from py2neo.bulk import create_nodes
    stream = iter(data)
    
    while True:
        batch = list(islice(stream, batch_size))
        if len(batch) > 0:
            create_nodes(graph.auto(), batch, labels=labels)
        else:
            break
            
            
#########################################
# Data Cleaning #
#########################################
import re

def remove_html_tags(text):
    """Remove html tags and new line from a string"""
    clean = re.compile('<.*?>|\n')
    return re.sub(clean, ' ', text)

def normalize(xs):
    return [" ".join(x.lower().split()) for x in xs]


def remove_special_cha(text):
    """Remove special characters from a string"""
    clean = re.compile('[|^&+\-%*/=>():"#$“”]')
    return re.sub(clean, ' ', text)


def language_preprocess(text):
    text = re.sub("English", "en", text)
    text = re.sub("German", "de", text)
    return text