import networkx as nx
import pandas as pd
import copy
import arcpy
import csv
import sys

def CreateGraph(df_Edges_total, Nodes):

    ##test: remove all nodes from the orographically right / left hand side of the river
    Nodes = pd.read_excel(Nodes,header=0).drop(columns="Z")
    Nodes_copy = copy.deepcopy(Nodes)
    #Nodes_copy = Nodes_copy.set_index("NODE_ID")
    #Position = Nodes_copy.to_dict(orient="index")
    dictionary = Nodes_copy.set_index('NODE_ID').T.apply(tuple).to_dict()

    # for v in Position.values():
    #     for x in v.values():
    #         x = x / 1000000
    #         print(x)

    ##Create graph and add nodes for the corresponding side of the river to the graph / network
    Node_ID_list = Nodes['NODE_ID'].tolist()
    Graph = nx.Graph()
    #Graph.add_nodes_from(Node_ID_list)
    Graph.add_nodes_from(dictionary.keys())

    ##test whether the nodes in pandas dataframe exist in the graph. If so, add an edge between them
    for index, row in df_Edges_total.iterrows():
        if Graph.has_node(row['Node_1']) and Graph.has_node(row['Node_2']):
            Graph.add_edge(row['Node_1'], row['Node_2'])
            print("Edge between the nodes " + str(row["Node_1"]) + " and " + str(row["Node_2"]) + " was added.")
        else:
            print("Either " + str(row["Node_1"]) + " or " + str(
                row["Node_2"]) + " is not in the Graph. No edge will be added.")
    return Graph

def AppendDepthFileToGraph (Depth_df, Graph):
    ##append depth-file to graph
    dict_of_MaxDepth={}

    dict_of_MaxDepth = Depth_df['MaxDepth'].to_dict()
    nx.set_node_attributes(Graph, dict_of_MaxDepth, 'MaxDepth')
    print('All good')
    print(nx.info(Graph))
    return dict_of_MaxDepth

def AppendBuildingsToGraph(Graph, dict_of_MaxDepth, DF_BuildingsNodes, InputBuildings, NodesShp, buildings_firstjoin, buildings_secondjoin):
    ##append buildings to graph
    buildings_df = None

    # check if temporary data already exist:
    files_to_delete = [buildings_firstjoin, buildings_secondjoin]
    for file in files_to_delete:
        if arcpy.Exists(file):
            print(str(file) + " will be deleted")
            try:
                arcpy.Delete_management(file)
            except Exception:
                e = sys.exc_info()[1]
                print(e.args[0])
                arcpy.AddError(e.args[0])
        else:
            print (str(file) + " does not exist yet")

    drop_columns = ['Wert', 'X_centroid', 'Y_centroid']
    DF_Buildings_modified = DF_BuildingsNodes.drop(drop_columns, axis=1)

    Prepare_Buildings = pd.DataFrame(DF_Buildings_modified.Node_IDs.str.split(',').tolist(),
                                     index=DF_Buildings_modified.V25OBJECTI).stack()
    Prepare_Buildings.columns = ['Node_ID']
    Buildings = Prepare_Buildings.reset_index(level=Prepare_Buildings.index.names)
    del Buildings['level_1']
    Buildings.columns = ['Buildings_ID', 'Node_ID']

    Buildings_columns = Buildings.columns.tolist()
    order = [1, 0]
    new_Buildings_columns = [Buildings_columns[i] for i in order]

    Buildings = Buildings[new_Buildings_columns]
    Buildings = Buildings.set_index('Node_ID')
    Buildings_df = Buildings.to_dict()
    Buildings_df = Buildings_df['Buildings_ID']

    Buildings_df = {int(k): int(v) for k, v in Buildings_df.items()}

    # if not len(Buildings_df) == len(dict_of_MaxDepth):
    #     i = 1
    #     while i <= len(dict_of_MaxDepth):
    #         if i not in Buildings_df.keys():
    #             Buildings_df[i] = 0
    #             i = i + 1
    #         else:
    #             i = i + 1

    nx.set_node_attributes(Graph, Buildings_df, 'Buildings_ID')
    print(nx.info(Graph))

    ##create sub graph from wet nodes only
    list_of_wet_nodes = [x for x, y in Graph.nodes(data=True) if y['MaxDepth'] > 0]
    print('I finished')

    Sub_Graph = Graph.subgraph(list_of_wet_nodes)
    Wet_Graph = Sub_Graph.copy()
    #print(nx.info(Wet_Graph))
    return Wet_Graph, Buildings_df

def NetworkCreationOfWetNodes(Current_lv, LeveeFailures_Nodes, Buildings_df, Wet_Graph, csv_buildings):
    ##open .txt-file where levee failures are assigned to the Node IDs
    Levee_Failures = {}
    with open(LeveeFailures_Nodes) as f:
        for line in f:
            #(key, val) = re.split(r'/t+', line.rstrip('/t'))
            (key, val) = line.split()
            Levee_Failures[int(key)] = val
            list = val.split(',')
            li =[]
            for i in list:
                li.append(int(i))
            Levee_Failures[int(key)]=li

    print ('okey...')

    ##check if nodes of levee failures are connected to buildings
    only_Buildings_df = {k: v for k, v in Buildings_df.items() if v}

    # affected_buildings=[]
    # for i in Levee_Failures[Current_LF]:
    #     print (i)
    #     for key, value in only_Buildings_df.items():
    #         print (key, value)
    #         if not value in affected_buildings:
    #             try:
    #                 nx.has_path(Wet_Graph, i, key)
    #                 affected_buildings.append(value)
    #                 print(affected_buildings)
    #             except nx.exception.NodeNotFound:
    #                 print ('building is not affected')

    affected_buildings=[]
    for i in Levee_Failures[Current_lv]:
        print (i)
        for key, value in only_Buildings_df.items():
            print (key, value)
            if not value in affected_buildings:
                try:
                    s = nx.shortest_path(Wet_Graph, i, key)
                    affected_buildings.append(value)
                    print(affected_buildings)
                    del s
                except nx.exception.NetworkXNoPath:
                    print ('building is not affected')
                except nx.exception.NodeNotFound:
                    print('building is not affected')
                except nx.NodeNotFound:
                    print('building is not affected')

    # if nx.has_path(Wet_Graph, 21420, 17027):
    #     print ("found path")
    # else:
    #     print ("path not found")

    print ("Hang on...")

    #print(nx.shortest_path(Wet_Graph, source=2931, target=28704))
    #paths = nx.all_simple_paths(Wet_Graph, source=2931, target=28704)



    with open(csv_buildings, "w") as output:
        #writer = csv.writer(output, lineterminator='/n')
        writer = csv.writer(output, lineterminator=',')
        for val in affected_buildings:
            writer.writerow([val])

    print ('connections verified')
    return affected_buildings