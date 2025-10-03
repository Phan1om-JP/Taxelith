// Show all nodes and relationship
MATCH (n)-[r]->(m)
RETURN n, r, m;

// Show all nodes but relationship
MATCH (n)
RETURN n;

// Show Schema (Labels + relation type)
CALL db.schema.visualization();

// Sample down return
MATCH (n)-[r]->(m)
RETURN n, r, m
LIMIT 200; //limit to 200 nodes