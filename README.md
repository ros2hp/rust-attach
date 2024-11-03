## What is Attach?
Attach is the second in the sequence of five programs that constitute the GoGraph RDF load process.  GoGraph is a rudimentary graph database, developed originally in Go principally as a way to learn the language and now refactored in Rust for a similar reason. GoGraph employees the Tokio asynchronous runtime to implement a highly concurrent and asynchronous design. The database design enables scaling to internet size data volumes. GoGraph currently supports AWS's Dynamodb, although other hyper scalable databases, such as Google's Spanner, is also in development. 



## GoGraph RDF Load Programs

The table below lists the sequence of programs that load a RDF file into the target database, Dynamodb. There is no limit to the size of the RDF file. 

MySQL is used as an intermediary storage facility providing both query and sort capabilties to each of the load programs. Unlike the Go implementation, the Rust version does not support restartability for any of the load programs. This is left as a future enhancement. It is therefore recommended to take a backup of the database after running each of the load programs. 

| Load Program           |  Repo       |  Task                                                   |  Data Source           | Target Database |
|-----------------------:|-------------|---------------------------------------------------------|------------------------|-----------------|
|  RDF-Loader            |    ldr      |  Load a RDF file into Dynamodb and MySQL                |   RDF file             |  Dynamodb, MySQL|
|  _Attach_                | _rust-attach_ | _Link child nodes to parent nodes_                        |   _MySQL_                | _Dynamodb_        |
|  Scalar Propagation    |    sp       | Propagate child scalar data into parent node and  generate reverse edge data     |  MySQL        | Dynamodb     |
|  Double Propagation    |   dp        | Propagate grandchild scalar data into grandparent node* |  MySQL           | Dynamodb        |
|  ElasticSearch         |   es        | Load data into ElasticSearch                            |  MySQL          | Dynamodb        |


* for 1:1 relationships between grandparent and grandchild nodes

## GoGraph Design Guide ##

[A detailed description of GoGraph's database design, type system and data model are described in this document](docs/GoGraph-Design-Guide.pdf)

## Ldr Highlights ##

* Stupidly parallel design

## Ldr Schematic ##

A simplified view of SP is presented in the two schematics below. The first schematic describes the generation of reverse edge data  (child to parent as opposed to he more usual parent-child) which uses a dedicated cache to aggregate the reverse edges and a LRU algorithm to manage the persistance of data to Dynamodb.  The second schematic shows the simpler scalar propagation load.  Parent-child edges are held in MySQL while the scalar data is queried from Dynamodb for each child node and then saved back into Dynamodb where it is associated with the parent node. No cache is required to propagate the child data.

              -----------
             |  MySQL    |
              -----------      
                   |
                   V

                  Main                                    ( Passes each parent node for an edge  )
                                                          ( to Attach which reads child nodes from MySQL)
                   |                                                )
                   |
            ----------- . . ---
           |       |           |
           V       V           V
                                                 ---------     
         Attach  Attach . .  Attach  --- < > ---|  MySQL  |   ( MySQL holds id mappings and parent-child data )
                                                 ---------    ( used by all load programs)
           |       |           |
           V       V           V
      ==============================
     |          Dynamodb            |     
      ==============================
       
        Fig 1.  Attach Schematic  




