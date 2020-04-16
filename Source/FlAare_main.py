###################################################################################
##Imports##
###################################################################################
import arcpy
import ArcGISProcesses as ap
import ReadInputs as ri
import PrepareVisualization as pv
import PandasDFProcesses as pdfp
import NetworkCreation as nc
import psutil
import PicklesCheck as pc
import copy
import pickle as pkl
import networkx as nx
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

###################################################################################
##Folder structure##
###################################################################################
RootPath="C:/Users/Dominik/OneDrive/Dokumente/UniBe/Master/Masterarbeit/99_Test"
ProjectFolder=RootPath + "/FlAare_202001"
TempFolder = ProjectFolder + "/Temp_Data"
InputFolder = ProjectFolder + "/Input_Data"
OutputFolder = ProjectFolder + "/Output_Data"
PickleData = ProjectFolder + "/Pickle_Data"
LeveeFailures_shp = InputFolder + "/Aare_LeveeFailures_202002_LV03.shp"
LeveeFailures_txt = InputFolder + "/AareComplete_LeveeFailures_201911_inkl_Node_ID_V4.txt"
LeveeFailure_shp_WGS84 = TempFolder + "LF_shp_WGS84.shp"

###################################################################################
##Path structure to create connection from buildings to levees
###################################################################################
Accum_csv_HasliAare = OutputFolder+"/Accum_csv_HasliAare.csv"
Accum_csv_HasliAare_mod = OutputFolder + "/Connection_Building_2_levee_HasliAare.csv"
Building_connection_HasliAare = TempFolder  + "/temp.gdb/Building_connection_HasliAare"
Building_connection_WGS84_HasliAare = TempFolder  + "/temp.gdb/Building_connection_HasliAare_WGS84"
house_geojson_HasliAare = TempFolder + "/temp_connection_HasliAare.geojson"
house_geojson_HasliAare_modified = OutputFolder + "/Connection_Building_to_HasliAare.geojson"
Geojson_LeveeWithHouses_HasliAare = OutputFolder + "/LeveeWithHouses_HasliAare.geojson"
ConnectionLines_HasliAare = OutputFolder + "/Lines_HasliAare.geojson"

Accum_csv_lowerAare = OutputFolder+"/Accum_csv_lowerAare.csv"
Accum_csv_lowerAare_mod = OutputFolder + "/Connection_Building_2_levee_lowerAare.csv"
Building_connection_lowerAare = TempFolder  + "/temp.gdb/Building_connection_lowerAare"
Building_connection_WGS84_lowerAare = TempFolder  + "/temp.gdb/Building_connection_lowerAare_WGS84"
house_geojson_lowerAare = TempFolder + "/temp_connection_lowerAare.geojson"
house_geojson_lowerAare_modified = OutputFolder + "/Connection_Building_to_lowerAare.geojson"
Geojson_LeveeWithHouses_lowerAare = OutputFolder + "/LeveeWithHouses_lowerAare.geojson"
ConnectionLines_lowerAare = OutputFolder + "/Lines_lowerAare.geojson"

Temp_csv_buildings = TempFolder + "/AffectedBuildings.csv"
temp_table = TempFolder + "/temp.gdb/Table_Line_Features"
temp_point = TempFolder + "/temp.gdb/Point_Features"
Line_features = TempFolder + "/temp.gdb/Line_Features"
Line_features_WGS84 = TempFolder + "/temp.gdb/Line_Features_WGS84"

###################################################################################
##definition of main method##
###################################################################################

Current_LF=1 #initial count of levee failure

def main(pickle_load_DF_LeveeFailures, filename_pickle_LeveeFailures, LeveeFailures_shp, pickle_load_Depth,
         filename_pickle_Depth, filename_depth, filename_geom, InputBuildings, NodesShp, buildings_firstjoin,
         buildings_secondjoin, pickle_load_NodePerBuilding, filename_pickle_Building_Nodes, pickle_load_Graph,
         filename_pickle_Graph, Path_to_csv,
         ArcGISTablepath, ArcGISPointFC, ArcGISLineFCPath, ArcGISLineFC_joinPath, ArcGISLineFC_joinPath_WGS84,
         LeveeFailureLinesGeojson, ArcGISFC, ArcGISFC_WGS84, LeveeFailureBuildingsGeojson,
         LeveeFailureBuildingsGeojson_modified):

    ###################################################################################
    ##Define whether current levee failure is on orographically left or right side of river##
    ###################################################################################

    orographically_left = list(range(24, 43))
    orographically_left.append(3)
    orographically_right = list(range(1, 24))
    orographically_right.remove(3)
    if Current_LF in orographically_left and Current_LF <= 4:
        Nodes = InputFolder + "/HasliAare_nodes_left_V3.xls"
    elif Current_LF in orographically_left and Current_LF >= 4:
        Nodes = InputFolder + "/lowerAare_nodes_left_V2.xls"
    elif Current_LF in orographically_right and Current_LF <= 4:
        Nodes = InputFolder + "/HasliAare_nodes_right_V3.xls"
    elif Current_LF in orographically_right and Current_LF >= 4:
        Nodes = InputFolder + "/lowerAare_nodes_right_V2.xls"
    else:
        print("Something went wrong. A succesesfull run of the skript cannot be guaranteed.")

    ###################################################################################
    ##Check for pickles##
    ###################################################################################
    DF_LeveeFailures = pc.PicklesCheck.pickle_LeveeFailures(pickle_load_DF_LeveeFailures, LeveeFailures_shp, filename_pickle_LeveeFailures)
    DF_Depth = pc.PicklesCheck.pickle_Depth(pickle_load_Depth, filename_depth, filename_geom, filename_pickle_Depth)
    DF_BuildingsNodes = pc.PicklesCheck.pickle_NodePerBuilding(pickle_load_NodePerBuilding, InputBuildings, NodesShp,
                                                              buildings_firstjoin, buildings_secondjoin,
                                                              filename_pickle_Building_Nodes)
    df_Edges_total = ri.BasementInputs.twodmFile(two_dm_file)
    Graph = pc.PicklesCheck.pickle_Graph(pickle_load_Graph, filename_pickle_Graph, df_Edges_total, Nodes)
    ###################################################################################
    ##Create network and corresponding subnetwork of wet nodes and affected buildings##
    ###################################################################################

    #DF_BuildingsNodes = ri.shpInputs.NearestNodes(BuildingsHasliAare, ShpNodes, buildings_firstjoin, buildings_secondjoin)
    #df_Edges_total = ri.BasementInputs.twodmFile(two_dm_file)
    Graph = nc.CreateGraph(df_Edges_total, Nodes)
    # with open("C:/Users/Dominik/OneDrive/Dokumente/UniBe/Master/Masterarbeit/99_Test/FlAare_202001/Temp_Data/Graph.pkl", 'wb') as fh:
    #      pkl.dump(Graph, fh, 2)
    # with open ("C:/Users/Dominik/OneDrive/Dokumente/UniBe/Master/Masterarbeit/99_Test/FlAare_202001/Temp_Data/Graph.pkl", 'rb') as fh:
    #      Graph = pkl.load(fh)

    Depth_df = ri.BasementInputs.DepthFile(input_sol)
    dict_of_MaxDepth = nc.AppendDepthFileToGraph(Depth_df, Graph)
    #print (psutil.virtual_memory())
    print ("The water depth was successfully added to the graph")
    Wet_Graph_Buildings = nc.AppendBuildingsToGraph(Graph, dict_of_MaxDepth, DF_BuildingsNodes, Buildings, NodesShp, buildings_firstjoin, buildings_secondjoin)
    Wet_Graph = Wet_Graph_Buildings[0]
    Buildings_df = Wet_Graph_Buildings[1]
    Graph.clear()
    print ("Wet_Graph has " + str(len(Wet_Graph)) + " nodes.")
    affected_buildings = nc.NetworkCreationOfWetNodes(Current_LF, LeveeFailures_txt, Buildings_df, Wet_Graph, Temp_csv_buildings)

    Basepath = "C:/Users/Dominik/OneDrive/Dokumente/UniBe/Master/Masterarbeit/99_Test/FlAare_202001/Output_Data"
    ##test
    #nx.write_shp(Wet_Graph, Basepath + "/WetGraph_LF"+str(Current_LF)+".shp")
    #df = pd.read_excel(Nodes, delimiter=',')
    #df = df.drop('Z', axis=1)
    #dictionary = df.set_index('NODE_ID').T.apply(tuple).to_dict()
    #nx.write_gexf(Wet_Graph, "C:/Users/Dominik/OneDrive/Dokumente/UniBe/Master/Masterarbeit/99_Test/FlAare_202001/Output_Data/test.gexf")
    #nx.draw(Wet_Graph, cmap=plt.get_cmap('jet'), with_labels=True)
    #nx.draw(Wet_Graph, pos=dictionary, cmap=plt.get_cmap('jet'), with_labels = True)
    #plt.show()
    ##test end

    ###################################################################################
    ##Connect the wet buildings with the nodes and export the connection as line to a geojson##
    ###################################################################################

    ##check if any buildings are affected
    if len(affected_buildings) > 0:

        #make a selection of the wet buildings from the total buildings
        DF_WettBuildingsNodes = DF_BuildingsNodes[DF_BuildingsNodes['V25OBJECTI'].isin(affected_buildings)]
        #DF_WettBuildingsNodes = DF_BuildingsNodes[~DF_BuildingsNodes['V25OBJECTI'].isin(affected_buildings)]

        BuildingsDepth = pdfp.PandasDF_join.JoiningBuildingsDepthExport(DF_WettBuildingsNodes, DF_Depth, Path_to_csv)
        #consumes a lot of memory
        print ("Buildings have successfully been assigned to the corresponding water depth per node")
        ap.ArcGISConversion.ArcGISPoint2Line(Current_LF, DF_LeveeFailures, BuildingsDepth, ArcGISTablepath, ArcGISPointFC,
                                             ArcGISLineFCPath, ArcGISLineFC_joinPath, ArcGISLineFC_joinPath_WGS84,
                                             LeveeFailureLinesGeojson)
        print("Flooded Houses were sucessfully connected to the corresponding levee as a line feature and exported as"
              " a geojson-file")
        ap.ArcGISConversion.ArcGIS2geojson(Current_LF, ArcGISFC, ArcGISFC_WGS84, LeveeFailureBuildingsGeojson)
        pv.GeoJSONPreparation.ModifyGeoJSON(LeveeFailureBuildingsGeojson, LeveeFailureBuildingsGeojson_modified )
        print ("Geojson was successfully created from ArcGIS Feature Class")

        if Current_LF <= 4:
            pv.csvProcesses.AccumulateEntries(BuildingsDepth, Current_LF, Accum_csv_HasliAare)
        elif Current_LF <=43:
            pv.csvProcesses.AccumulateEntries(BuildingsDepth, Current_LF, Accum_csv_lowerAare)

    ##else it seams that no buildings are affected. Therefore no geojson was done and the script moves to the next
    #levee failure
    else:
        print ("Levee failure No " + str(Current_LF) + " does not cause any damage on buildings. Move on to the next"
                                                       " levee failure")

###################################################################################
##Setting up loop for 42 runs (42 levee failures) and execution of the main method##
###################################################################################

#define first levee failure: Afterwards it will be updates through the script JoiningBuildingsDepthExport
Current_LF=ri.shpInputs.CountLeveeFailures(LeveeFailures_shp)[0]
Total_lv = ri.shpInputs.CountLeveeFailures(LeveeFailures_shp)[1]
# Total_lv = 4

while Current_LF <=Total_lv:
    print("Currently levee failure " + str(Current_LF) + " is beeing executed.")

    ##if the current levee failure has ID 1 to 4, use the 2dm-file from the Hasliaare, otherwise the one from the lower
    ## Aare Thun-Bern
    if Current_LF <=4:
        two_dm_file = InputFolder + "/HasliAare.2dm"
        input_sol = InputFolder + "/.sol-files/HasliAare_nds_depth_LF" + str(Current_LF) + ".sol"
        if Current_LF != 3:
            mesh_nodes = InputFolder + "/HasliAare_nodes_right_V3.shp"
            Buildings = InputFolder + "/Buildings_HasliAare_right.shp"
        else:
            mesh_nodes = InputFolder + "/HasliAare_nodes_left_V3.shp"
            Buildings = InputFolder + "/Buildings_HasliAare_left.shp"
    else:
        two_dm_file = InputFolder + "/Aare.2dm"
        input_sol = InputFolder + "/.sol-files/AareThunBern_LF" + str(Current_LF) + "_nds_depth.sol"
        if Current_LF <= 23:
            mesh_nodes = InputFolder + "/lowerAare_nodes_right_V2.shp"
            Buildings = InputFolder + "/Buildings_lowerAare_right.shp"
        else:
            mesh_nodes = InputFolder + "/lowerAare_nodes_left_V2.shp"
            Buildings = InputFolder + "/Buildings_lowerAare_left.shp"

    ##defining PickleGraph variable:
    pickle_False = [1,3,4,5,24,26]
    pickle_True = [2,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,25,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42]

    if Current_LF in pickle_False:
        PickleGraph = False
    elif Current_LF in pickle_True:
        PickleGraph = True
    else:
        print ("Something went wrong")

    ##Depending whether the levee failures of the Hasliaare or the lower Aare should be processed the .2dm file has
    ##to be changed. Therefore for the first 4 levee failures have the same .2dm file and the processed file can be
    ##written into a pickle. When it changes to levee failure 5 (lower Aare) the new .2dm file has to be taken and so
    ##the pickle file has to be created newly.
    if Current_LF == 1:
        PickleBuilding = False
        PickleLevee = False
    elif Current_LF == 5:
        PickleBuilding = False
        PickleLevee = True
    else:
        PickleBuilding = False
        PickleLevee = True

    ##main script will be executed heareafter
    main(PickleLevee, PickleData + "/DF_LeveeFailures.pkl", LeveeFailures_shp, False, PickleData + "/Dataframe.pkl",
         input_sol, two_dm_file, Buildings, mesh_nodes, TempFolder + "/Buildings_join1.shp",
         TempFolder + "/Buildings_join2.shp", PickleBuilding, PickleData + "/Buildings_Nodes.pkl", PickleGraph,
         PickleData + "/Graph.pkl",
         OutputFolder + "/Flooded_Buildings_LF_" + str(Current_LF) + ".csv", TempFolder + "/temp.gdb/db_Table",
         TempFolder + "/PointFC.shp", TempFolder + "/LineFC.shp", TempFolder + "/LineFC_join.shp",
         TempFolder + "/LineFC_join_WGS84.shp", OutputFolder + "/ConnectionLines_LF_" + str(Current_LF) + ".geojson",
         TempFolder + "/PointFC.shp", TempFolder + "/PointFC_WGS84.shp",
         OutputFolder + "/Buildings_LF_" + str(Current_LF) + ".geojson", OutputFolder + "/Buildings_LF_" +
         str(Current_LF) + "_modified.geojson")
    print("Levee failure " + str(Current_LF) + " was sucessfully written to a csv and geojson-file.")

    ##Delete temp-files
    TempFiles=[TempFolder + "/temp.gdb/test_table1", TempFolder + "/temp.gdb/test_fc",TempFolder +
               "/Buildings_join1.shp",
               TempFolder + "/Buildings_join2.shp", TempFolder + "/temp.gdb/testTable",
               TempFolder + "/PointFC.shp", TempFolder + "/LineFC.shp", TempFolder + "/LineFC_join.shp",
               TempFolder + "/LineFC_join_WGS84.shp",  TempFolder + "/PointFC.shp", TempFolder + "/PointFC_WGS84.shp",
               Temp_csv_buildings]

    for file in TempFiles:
        if arcpy.Exists(file):
            arcpy.Delete_management(file)

    Current_LF = Current_LF + 1

print ("The programm has successfully finished and written all .csv and .geojson files")

###################################################################################
##Sort and prepare the output for export as geojson
###################################################################################

##the accumulated csv containing the maximum water depth for each building and each levee will be modified
print ('Start to sort the accumulated csv for the HasliAare...')
pv.csvProcesses.sort_csv(Accum_csv_HasliAare, Accum_csv_HasliAare_mod)
print ('Successfully sorted the accumulated csv of the HasliAare. Start to prepare data for export to geojson...')
pv.GeoJSONPreparation.house2geojson(Accum_csv_HasliAare_mod, Building_connection_HasliAare,
                                    Building_connection_WGS84_HasliAare, house_geojson_HasliAare, OutputFolder,
                                    house_geojson_HasliAare_modified)
pv.GeoJSONPreparation.UniteBuildingsPerLevee(Accum_csv_HasliAare, LeveeFailures_shp, LeveeFailure_shp_WGS84,
                                             Geojson_LeveeWithHouses_HasliAare, Current_LF)
pv.GeoJSONPreparation.CreateLinesBuildings2Levee(Accum_csv_HasliAare, LeveeFailures_shp, temp_table, temp_point,
                                                 Line_features, Line_features_WGS84, ConnectionLines_HasliAare)

print ('Start to sort the accumulated csv for the lower Aare...')
pv.csvProcesses.sort_csv(Accum_csv_lowerAare, Accum_csv_lowerAare_mod)
print('Geojson files for the lowerAare successfully created')
print ('Successfully sorted the accumulated csv of the lower Aare. Start to prepare data for export to geojson...')
pv.GeoJSONPreparation.house2geojson(Accum_csv_lowerAare_mod, Building_connection_lowerAare,
                                     Building_connection_WGS84_lowerAare, house_geojson_lowerAare, OutputFolder,
                                     house_geojson_lowerAare_modified)
pv.GeoJSONPreparation.UniteBuildingsPerLevee(Accum_csv_lowerAare, LeveeFailures_shp, LeveeFailure_shp_WGS84,
                                             Geojson_LeveeWithHouses_lowerAare, Current_LF)
pv.GeoJSONPreparation.CreateLinesBuildings2Levee(Accum_csv_lowerAare, LeveeFailures_shp, temp_table, temp_point,
                                                 Line_features, Line_features_WGS84, ConnectionLines_lowerAare)
print('Geojson files for the lower Aare successfully created')
print('All calculations have been successfully terminated. The python script is now finished. Congratulation!')