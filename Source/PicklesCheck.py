import ArcGISProcesses as ap
import pandas as pd
import pickle as pkl
import ReadInputs as ri
import NetworkCreation as nc
import networkx as nx

class PicklesCheck:

    # def __init__(self, pickle_load_DF_LeveeFailures, LeveeFailures_shp, filename_pickle_LeveeFailures,
    #              pickle_load_Depth, filename_depth, filename_geom, filename_pickle_Depth,
    #              pickle_load_NodePerBuilding, InputBuildings, NodesShp, buildings_firstjoin,
    #              buildings_secondjoin, filename_pickle_Building_Nodes):
    #     #self.pickle_load_DF_LeveeFailures = None
    #     self.LeveeFailures_shp = None
    #     self.filename_pickle_LeveeFailures = None
    #     self.pickle_load_Depth = None
    #     self.filename_depth = None
    #     self.filename_geom = None
    #     self.filename_pickle_Depth = None
    #     self.pickle_load_NodePerBuilding = None
    #     self.InputBuildings = None
    #     self.NodesShp = None
    #     self.buildings_firstjoin = None
    #     self.buildings_secondjoin = None
    #     self.filename_pickle_Building_Nodes = None

    def pickle_LeveeFailures(pickle_load_DF_LeveeFailures, LeveeFailures_shp, filename_pickle_LeveeFailures):
        ######################################
        ##1st pickle check: Can data be received from preprocessed pickle file of the levee failures
        ######################################
        if pickle_load_DF_LeveeFailures == False:
            DF_LeveeFailures = ap.ArcGISConversion.dbf2pandas(LeveeFailures_shp)
            print("DataFrame of Levee failures created, writing pickle file")
            with open(filename_pickle_LeveeFailures, 'wb') as fh:
                pkl.dump(DF_LeveeFailures, fh, 2)
        else:
            with open(filename_pickle_LeveeFailures, 'rb') as fh:
                DF_LeveeFailures = pkl.load(fh)
                print("no pickle-file for the Levee Failures needs to be created. It already exists")
        return DF_LeveeFailures

    def pickle_Depth(pickle_load_Depth, filename_depth, filename_geom, filename_pickle_Depth):
        ######################################
        ##2st pickle check: Can data be received from preprocessed pickle file of the water depth
        ######################################
        if pickle_load_Depth == False:
            print("Start to read files and create pickle-file")
            d = ri.BasementInputs.DepthFile(filename_depth)
            print("Depth file sucessfully read")
            g = ri.BasementInputs.GeometryFile(filename_geom)
            print("Geometry file sucessfully read")
            g.index = g.index + 1
            DF_Depth = pd.concat([g, d], axis=1)
            print("Concatenation done, writing pickle file")
            with open(filename_pickle_Depth, 'wb') as fh:
                pkl.dump(DF_Depth, fh, 2)
        else:
            with open(filename_pickle_Depth, 'rb') as fh:
                DF_Depth = pkl.load(fh)
                print("no pickle-file needs to be created. It already exists")
        print("Buildings-shp has been updated with the amount of nodes inside them")
        return DF_Depth

    def pickle_NodePerBuilding(pickle_load_NodePerBuilding, InputBuildings, NodesShp, buildings_firstjoin,
                               buildings_secondjoin, filename_pickle_Building_Nodes):
        ######################################
        ##3rd pickle check: Can data be received from preprocessed pickle file of the nodes per building
        ######################################
        if pickle_load_NodePerBuilding == False:
            print("Creating pickle-file...")
            DF_BuildingsNodes = ri.shpInputs.NearestNodes(InputBuildings, NodesShp, buildings_firstjoin,
                                                          buildings_secondjoin)
            print("Nearest nodes sucessfully retrieved")
            with open(filename_pickle_Building_Nodes, 'wb') as fh:
                pkl.dump(DF_BuildingsNodes, fh, 2)
        else:
            with open(filename_pickle_Building_Nodes, 'rb') as fh:
                DF_BuildingsNodes = pd.read_pickle(fh, compression=None)
                print("no pickle-file needs to be created. It already exists")
        return DF_BuildingsNodes

    def pickle_Graph(pickle_Graph, path_pickle_Graph, df_Edges_total, Nodes):
        if pickle_Graph == False:
            Graph = nc.CreateGraph(df_Edges_total, Nodes)
            nx.write_gpickle(Graph, path_pickle_Graph)
        else:
            Graph = nx.read_gpickle(path_pickle_Graph)
        return Graph