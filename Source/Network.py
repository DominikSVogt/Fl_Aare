import pandas as pd
import networkx as nx
import ReadInputs as ri
import arcpy
import csv
import copy
import matplotlib.pyplot as plt
import Help
import sys

def CreateNetwork(filename):

    df_Edges = None # panda dataframe

    ##prepare 2dm file for creating a graph. Loading data to pandas df and merge columns to two for beeing able to create
    ##graph from edges in columns
    with open(filename) as geom:
        nodes_string = [line.strip() for line in geom if line.startswith("E3T")]

    nodes_list = []
    for elements in nodes_string:
        nodes_list.append(elements.split())

    print("List containing nodes sucessfully created")

    header = ["Type", "Element_ID", "Node_ID_1", "Node_ID_2", "Node_ID_3", "Unknown"]
    df_Edges = pd.DataFrame(nodes_list, columns=header)
    df_Edges = df_Edges.drop(columns=["Type", "Unknown"])
    # Pandas rounds the last number behind comma. Guess it can only handle 5 digits after the comma.
    df_Edges = df_Edges.astype({"Element_ID": int, "Node_ID_1": int, "Node_ID_2": int, "Node_ID_3": int})
    df_Edges = df_Edges.drop(columns='Element_ID')

    #Conversion of pandas df for beeing able to have only two columns left for the edges.
    column_names=["Node_1", "Node_2"]
    df_Edges_1 = copy.deepcopy(df_Edges).drop(columns="Node_ID_3")
    df_Edges_1.columns=column_names
    df_Edges_2 = copy.deepcopy(df_Edges).drop(columns="Node_ID_1")
    df_Edges_2.columns = column_names
    df_Edges_3 = copy.deepcopy(df_Edges).drop(columns="Node_ID_2")
    df_Edges_3.columns = column_names
    df_Edges_total=df_Edges_1.append(df_Edges_2, ignore_index=True)
    df_Edges_total = df_Edges_total.append(df_Edges_3, ignore_index=True)

    ##create graph from edges in pandas df
    Graph = nx.from_pandas_edgelist(df_Edges_total, source="Node_1", target="Node_2")

    #print (nx.info(Graph))
    ##test: remove all nodes from the orographically right / left hand side of the river
    Nodes_left = pd.read_excel("C:/Users/Dominik/OneDrive/Dokumente/UniBe/Master/Masterarbeit/99_Test/FlAare_202001/Input_Data/HasliAare_nodes_left.xls", header=0).drop(columns="Z")
    Nodes_right = pd.read_excel("C:/Users/Dominik/OneDrive/Dokumente/UniBe/Master/Masterarbeit/99_Test/FlAare_202001/Input_Data/HasliAare_nodes_right.xls", header=0).drop(columns="Z")
    Nodes_right_copy = copy.deepcopy(Nodes_right)
    Nodes_right_copy= Nodes_right_copy.set_index("NODE_ID")
    pos_right = Nodes_right_copy.to_dict(orient="index")

    for v in pos_right.values():
        for x in v.values():
            x=x/1000000
            print (x)
        #v[0]=v[0]/1000000
        #v[1]=v[1]/1000000

    # for key, value in pos_right:
    #     print (key)
    #     #value=(value[2],value[3])

    Nodes_to_delete=[]
    Rest_of_nodes=[]
    i=0
    k=0
    for nodes in Graph:
        if nodes in Nodes_right['NODE_ID']:
            print ("Node will NOT be removed")
            i=i+1
            Rest_of_nodes.append(nodes)
        else:
            print ("Node WILL later be removed")
            k=k+1
            Nodes_to_delete.append(nodes)

    ##
    Test =Help.getIndexes(Nodes_right, 4)
    print('Index positions of 81 in Dataframe : ')
    for i in range(len(Test)):
        print('Position ', i, ' (Row index , Column Name) : ', Test[i])
    ##

    if 4 in pos_right.keys():
        print ("yes")
    else:
        print ("no")


    if 4 in Nodes_to_delete:
        print ("is in list")
    elif 4 in Rest_of_nodes:
        print ("node is left in graph")
    else:
        print ("is not in list")
    #print(nx.info(Graph))

    nx.Graph.remove_nodes_from(Graph, Nodes_to_delete)
    #print (nx.info(Graph))
    ##test ende

    ##append depth-file to graph
    b = ri.BasementInputs.DepthFile(
        "C:/Users/Dominik/OneDrive/Dokumente/UniBe/Master/Masterarbeit/99_Test/FlAare_202001/Input_Data/.sol-files/Hasliaare_nds_depth_LV1.sol")

    dict_of_MaxDepth=b['MaxDepth'].to_dict()
    nx.set_node_attributes(Graph, dict_of_MaxDepth, 'MaxDepth')
    print ('All good')

    ##append buildings to graph
    InputBuildings="C:/Users/Dominik/OneDrive/Dokumente/UniBe/Master/Masterarbeit/99_Test/FlAare_202001/Input_Data/Buildings_HasliAare.shp"
    #InputBuildings="C:/Users/Dominik/OneDrive/Dokumente/UniBe/Master/Masterarbeit/99_Test/FlAare_201912/Input_Data/Buildings_lowerAare.shp"
    #NodesShp = "C:/Users/Dominik/OneDrive/Dokumente/UniBe/Master/Masterarbeit/99_Test/FlAare_201912/Input_Data/lowerAare_nodes.shp"
    NodesShp = "C:/Users/Dominik/OneDrive/Dokumente/UniBe/Master/Masterarbeit/99_Test/FlAare_201912/Input_Data/HasliAare_nodes.shp"
    NodesShp_right = "C:/Users/Dominik/OneDrive/Dokumente/UniBe/Master/Masterarbeit/99_Test/FlAare_201912/Input_Data/HasliAare_nodes_right.shp"
    buildings_firstjoin = "C:/Users/Dominik/OneDrive/Dokumente/UniBe/Master/Masterarbeit/99_Test/FlAare_202001/Temp_Data/Temp1.shp"
    buildings_secondjoin = "C:/Users/Dominik/OneDrive/Dokumente/UniBe/Master/Masterarbeit/99_Test/FlAare_202001/Temp_Data/Temp2.shp"

    # check if temporary data already exist:
    files_to_delete = [buildings_firstjoin, buildings_secondjoin]
    for file in files_to_delete:
        if arcpy.Exists(file):
            print(file + " will be deleted")
            arcpy.Delete_management(file)

    DF_BuildingsNodes = ri.shpInputs.NearestNodes(InputBuildings, NodesShp, buildings_firstjoin,
                                                  buildings_secondjoin)

    drop_columns=['Wert', 'X_centroid', 'Y_centroid']
    DF_Buildings_modified=DF_BuildingsNodes.drop(drop_columns, axis=1)

    Prepare_Buildings = pd.DataFrame(DF_Buildings_modified.Node_IDs.str.split(',').tolist(), index=DF_Buildings_modified.V25OBJECTI).stack()
    Prepare_Buildings.columns = ['Node_ID']
    Buildings=Prepare_Buildings.reset_index(level=Prepare_Buildings.index.names)
    del Buildings['level_1']
    Buildings.columns=['Buildings_ID', 'Node_ID']

    Buildings_columns=Buildings.columns.tolist()
    order=[1,0]
    new_Buildings_columns=[Buildings_columns[i] for i in order]

    Buildings = Buildings[new_Buildings_columns]
    Buildings=Buildings.set_index('Node_ID')
    Buildings_df = Buildings.to_dict()
    Buildings_df = Buildings_df['Buildings_ID']

    Buildings_df = {int(k): int(v) for k, v in Buildings_df.items()}

    if not len(Buildings_df) == len(dict_of_MaxDepth):
        i=1
        while i <= len(dict_of_MaxDepth):
            if i not in Buildings_df.keys():
                Buildings_df[i]=0
                i=i+1
            else:
                i=i+1

    nx.set_node_attributes(Graph, Buildings_df, 'Buildings_ID')

    # ##remove all nodes from the orographically right / left hand side of the river
    # Nodes_left = pd.read_excel("C:/Users/Dominik/OneDrive/Dokumente/UniBe/Master/Masterarbeit/99_Test/FlAare_202001/Input_Data/HasliAare_nodes_left.xlsx", header=0)
    # Nodes_right = pd.read_excel("C:/Users/Dominik/OneDrive/Dokumente/UniBe/Master/Masterarbeit/99_Test/FlAare_202001/Input_Data/HasliAare_nodes_right.xlsx", header=0)
    #
    # #for index, row in Nodes_right:
    #
    # Nodes_to_delete=[]
    # for nodes in Graph:
    #     if nodes in Nodes_right['Node_ID']:
    #         print ("Node will NOT be removed")
    #     else:
    #         print ("Node WILL later be removed")
    #         Nodes_to_delete.append(nodes)
    #
    # nx.Graph.remove_nodes_from(Graph, Nodes_to_delete)

    ##create sub graph from wet nodes only
    list_of_wet_nodes = [x for x, y in Graph.nodes(data=True) if y['MaxDepth'] > 0]
    print ('I finished')

    Wet_Graph = Graph.subgraph(list_of_wet_nodes)

    ##test
    try:
        nx.has_path(Wet_Graph, 45176, 43185)
        print ("Well done")
    except nx.exception.NodeNotFound:
        print ("We'll move on...")
    ##test

    ##open .txt-file where levee failures are assigned to the Node IDs
    Levee_Failures = {}
    with open("U:/Masterarbeit/AareComplete_LeveeFailures_201911_inkl_Node_ID_V2.txt") as f:
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

    affected_buildings=[]
    for i in Levee_Failures[1]:
        print (i)
        for key, value in only_Buildings_df.items():
            print (key, value)
            if not value in affected_buildings:
                try:
                    nx.has_path(Wet_Graph, i, key)
                    affected_buildings.append(value)
                    print(affected_buildings)
                except nx.exception.NodeNotFound:
                    print ('building is not affected')

    try:
        Wet_Graph.has_node(4)
        print("Should not be the case")
    except nx.exception.NodeNotFound:
        print("That's how it is supposed to be")

    #nx.draw_networkx_nodes(Wet_Graph, pos=pos_right)
    nx.draw(Wet_Graph, pos=pos_right)
    plt.show()
    #nx.draw_networkx(Wet_Graph, pos=pos_right)

    with open("C:/Users/Dominik/OneDrive/Dokumente/UniBe/Master/Masterarbeit/99_Test/FlAare_202001/Temp_Data/AffectedBuildings.csv", "w") as output:
        writer = csv.writer(output, lineterminator='/n')
        for val in affected_buildings:
            writer.writerow([val])

    print ('Connections durchgeschaut')

#CreateNetwork("C:/Users/Dominik/OneDrive/Dokumente/UniBe/Master/Masterarbeit/99_Test/FlAare_202001/Input_Data/HasliAare.2dm")

print ("test")

##Variante 2
def CreateNetworkV2(filename_2dm):

    df_Edges = None # panda dataframe

    ##prepare 2dm file for creating a graph. Loading data to pandas df and merge columns to two for beeing able to create
    ##graph from edges in columns
    with open(filename_2dm) as geom:
        nodes_string = [line.strip() for line in geom if line.startswith("E3T")]

    nodes_list = []
    for elements in nodes_string:
        nodes_list.append(elements.split())

    print("List containing nodes sucessfully created")

    header = ["Type", "Element_ID", "Node_ID_1", "Node_ID_2", "Node_ID_3", "Unknown"]
    df_Edges = pd.DataFrame(nodes_list, columns=header)
    df_Edges = df_Edges.drop(columns=["Type", "Unknown"])
    # Pandas rounds the last number behind comma. Guess it can only handle 5 digits after the comma.
    df_Edges = df_Edges.astype({"Element_ID": int, "Node_ID_1": int, "Node_ID_2": int, "Node_ID_3": int})
    df_Edges = df_Edges.drop(columns='Element_ID')

    #Conversion of pandas df for beeing able to have only two columns left for the edges.
    column_names=["Node_1", "Node_2"]
    df_Edges_1 = copy.deepcopy(df_Edges).drop(columns="Node_ID_3")
    df_Edges_1.columns=column_names
    df_Edges_2 = copy.deepcopy(df_Edges).drop(columns="Node_ID_1")
    df_Edges_2.columns = column_names
    df_Edges_3 = copy.deepcopy(df_Edges).drop(columns="Node_ID_2")
    df_Edges_3.columns = column_names
    df_Edges_total=df_Edges_1.append(df_Edges_2, ignore_index=True)
    df_Edges_total = df_Edges_total.append(df_Edges_3, ignore_index=True)

    ##ok

    ##test: remove all nodes from the orographically right / left hand side of the river
    Nodes_left = pd.read_excel("C:/Users/Dominik/OneDrive/Dokumente/UniBe/Master/Masterarbeit/99_Test/FlAare_202001/Input_Data/HasliAare_nodes_left.xls", header=0).drop(columns="Z")
    Nodes_right = pd.read_excel("C:/Users/Dominik/OneDrive/Dokumente/UniBe/Master/Masterarbeit/99_Test/FlAare_202001/Input_Data/HasliAare_nodes_right.xls", header=0).drop(columns="Z")
    Nodes_right_copy = copy.deepcopy(Nodes_right)
    Nodes_right_copy= Nodes_right_copy.set_index("NODE_ID")
    pos_right = Nodes_right_copy.to_dict(orient="index")

    for v in pos_right.values():
        for x in v.values():
            x=x/1000000
            print (x)

   ##Create graph and add nodes for the corresponding side of the river to the graph / network
    Node_ID_list = Nodes_right['NODE_ID'].tolist()
    G = nx.Graph()
    G.add_nodes_from(Node_ID_list)

    ##test whether the nodes in pandas dataframe exist in the graph. If so, add an edge between them
    for index, row in df_Edges_total.iterrows():
        if G.has_node(row['Node_1']) and G.has_node(row['Node_2']):
            G.add_edge(row['Node_1'], row['Node_2'])
            print ("Edge between the nodes " + str(row["Node_1"]) + " and " + str(row["Node_2"]) + " was added.")
        else:
            print ("Either " + str(row["Node_1"]) + " or " + str(row["Node_2"]) + " is not in the Graph. No edge will be added.")

    Graph = G
    print(nx.info(Graph))

    ##append depth-file to graph
    b = ri.BasementInputs.DepthFile(
        "C:/Users/Dominik/OneDrive/Dokumente/UniBe/Master/Masterarbeit/99_Test/FlAare_202001/Input_Data/.sol-files/Hasliaare_nds_depth_LV1.sol")

    dict_of_MaxDepth=b['MaxDepth'].to_dict()
    nx.set_node_attributes(Graph, dict_of_MaxDepth, 'MaxDepth')
    print ('All good')

    print (nx.info(Graph))

    ##append buildings to graph
    InputBuildings="C:/Users/Dominik/OneDrive/Dokumente/UniBe/Master/Masterarbeit/99_Test/FlAare_202001/Input_Data/Buildings_HasliAare.shp"
    NodesShp = "C:/Users/Dominik/OneDrive/Dokumente/UniBe/Master/Masterarbeit/99_Test/FlAare_201912/Input_Data/HasliAare_nodes.shp"
    NodesShp_right = "C:/Users/Dominik/OneDrive/Dokumente/UniBe/Master/Masterarbeit/99_Test/FlAare_201912/Input_Data/HasliAare_nodes_right.shp"
    buildings_firstjoin = "C:/Users/Dominik/OneDrive/Dokumente/UniBe/Master/Masterarbeit/99_Test/FlAare_202001/Temp_Data/Temp1.shp"
    buildings_secondjoin = "C:/Users/Dominik/OneDrive/Dokumente/UniBe/Master/Masterarbeit/99_Test/FlAare_202001/Temp_Data/Temp2.shp"

    # check if temporary data already exist:
    files_to_delete = [buildings_firstjoin, buildings_secondjoin]
    for file in files_to_delete:
        if arcpy.Exists(file):
            print(file + " will be deleted")
            try:
                arcpy.Delete_management(file)
            except Exception:
                e = sys.exc_info()[1]
                print(e.args[0])
                arcpy.AddError(e.args[0])

    DF_BuildingsNodes = ri.shpInputs.NearestNodes(InputBuildings, NodesShp, buildings_firstjoin,
                                                  buildings_secondjoin)

    drop_columns=['Wert', 'X_centroid', 'Y_centroid']
    DF_Buildings_modified=DF_BuildingsNodes.drop(drop_columns, axis=1)

    Prepare_Buildings = pd.DataFrame(DF_Buildings_modified.Node_IDs.str.split(',').tolist(), index=DF_Buildings_modified.V25OBJECTI).stack()
    Prepare_Buildings.columns = ['Node_ID']
    Buildings=Prepare_Buildings.reset_index(level=Prepare_Buildings.index.names)
    del Buildings['level_1']
    Buildings.columns=['Buildings_ID', 'Node_ID']

    Buildings_columns=Buildings.columns.tolist()
    order=[1,0]
    new_Buildings_columns=[Buildings_columns[i] for i in order]

    Buildings = Buildings[new_Buildings_columns]
    Buildings=Buildings.set_index('Node_ID')
    Buildings_df = Buildings.to_dict()
    Buildings_df = Buildings_df['Buildings_ID']

    Buildings_df = {int(k): int(v) for k, v in Buildings_df.items()}

    if not len(Buildings_df) == len(dict_of_MaxDepth):
        i=1
        while i <= len(dict_of_MaxDepth):
            if i not in Buildings_df.keys():
                Buildings_df[i]=0
                i=i+1
            else:
                i=i+1

    nx.set_node_attributes(Graph, Buildings_df, 'Buildings_ID')
    print(nx.info(Graph))


    ##create sub graph from wet nodes only
    list_of_wet_nodes = [x for x, y in Graph.nodes(data=True) if y['MaxDepth'] > 0]
    print ('I finished')

    Wet_Graph = Graph.subgraph(list_of_wet_nodes)
    print(nx.info(Wet_Graph))

    ##open .txt-file where levee failures are assigned to the Node IDs
    Levee_Failures = {}
    with open("U:/Masterarbeit/AareComplete_LeveeFailures_201911_inkl_Node_ID_V2.txt") as f:
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

    affected_buildings=[]
    for i in Levee_Failures[1]:
        print (i)
        for key, value in only_Buildings_df.items():
            print (key, value)
            if not value in affected_buildings:
                try:
                    nx.has_path(Wet_Graph, i, key)
                    affected_buildings.append(value)
                    print(affected_buildings)
                except nx.exception.NodeNotFound:
                    print ('building is not affected')



    with open("C:/Users/Dominik/OneDrive/Dokumente/UniBe/Master/Masterarbeit/99_Test/FlAare_202001/Temp_Data/AffectedBuildings_test.csv", "w") as output:
        #writer = csv.writer(output, lineterminator='/n')
        writer = csv.writer(output, lineterminator=',')
        for val in affected_buildings:
            writer.writerow([val])

    print ('Connections durchgeschaut')

CreateNetworkV2("C:/Users/Dominik/OneDrive/Dokumente/UniBe/Master/Masterarbeit/99_Test/FlAare_202001/Input_Data/HasliAare.2dm")