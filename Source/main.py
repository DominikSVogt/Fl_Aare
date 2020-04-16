###################################################################################
##Imports##
###################################################################################
import arcpy
import pandas as pd
import pickle as pkl

import ArcGISProcesses as ap
import ReadInputs as ri
import PrepareVisualization as pv
import PandasDFProcesses as pdfp

###################################################################################
##Folder structure##
###################################################################################
RootPath="C:/Users/Dominik/OneDrive/Dokumente/UniBe/Master/Masterarbeit/99_Test"
ProjectFolder=RootPath + "/FlAare_201912"
TempFolder = ProjectFolder + "/Temp_Data"
InputFolder = ProjectFolder + "/Input_Data"
OutputFolder = ProjectFolder + "/Output_Data"
PickleData = ProjectFolder + "/Pickle_Data"
LeveeFailures_shp=InputFolder + "/Aare_LeveeFailures_201911_LV03.shp"

###################################################################################
##Path structure to create connection from buildings to levees
###################################################################################
Accum_csv_HasliAare = OutputFolder+"/Accum_csv_HasliAare.csv"
Accum_csv_HasliAare_mod = OutputFolder + "/Connection_Building_2_levee_HasliAare.csv"
Building_connection_HasliAare = TempFolder  + "/temp.gdb/Building_connection_HasliAare"
Building_connection_WGS84_HasliAare = TempFolder  + "/temp.gdb/Building_connection_HasliAare_WGS84"
house_geojson_HasliAare = TempFolder + "/temp_connection_HasliAare.geojson"
house_geojson_HasliAare_modified = OutputFolder + "/Connection_Building_to_HasliAare.geojson"

Accum_csv_lowerAare = OutputFolder+"/Accum_csv_lowerAare.csv"
Accum_csv_lowerAare_mod = OutputFolder + "/Connection_Building_2_levee_lowerAare.csv"
Building_connection_lowerAare = TempFolder  + "/temp.gdb/Building_connection_lowerAare"
Building_connection_WGS84_lowerAare = TempFolder  + "/temp.gdb/Building_connection_lowerAare_WGS84"
house_geojson_lowerAare = TempFolder + "/temp_connection_lowerAare.geojson"
house_geojson_lowerAare_modified = OutputFolder + "/Connection_Building_to_lowerAare.geojson"

###################################################################################
##definition of main method##
###################################################################################

Current_lv=1 #initial count of levee failure

def main(pickle_load_DF_LeveeFailures, filename_pickle_LeveeFailures, LeveeFailures_shp, pickle_load_Depth,
         filename_pickle_Depth, filename_depth, filename_geom, InputBuildings, NodesShp, buildings_firstjoin,
         buildings_secondjoin, pickle_load_NodePerBuilding, filename_pickle_Building_Nodes, Path_to_csv,
         ArcGISTablepath, ArcGISPointFC, ArcGISLineFCPath, ArcGISLineFC_joinPath, ArcGISLineFC_joinPath_WGS84,
         LeveeFailureLinesGeojson, ArcGISFC, ArcGISFC_WGS84, LeveeFailureBuildingsGeojson,
         LeveeFailureBuildingsGeojson_modified):

    ######################################
    ##1st pickle check: Can data be received from preprocessed pickle file of the levee failures
    ######################################
    if pickle_load_DF_LeveeFailures == False:
        DF_LeveeFailures = ap.ArcGISConversion.dbf2pandas(LeveeFailures_shp)
        print ("DataFrame of Levee failures created, writing pickle file")
        with open(filename_pickle_LeveeFailures, 'wb') as fh:
            pkl.dump(DF_LeveeFailures,fh,2)
    else:
        with open(filename_pickle_LeveeFailures, 'rb') as fh:
            DF_LeveeFailures = pkl.load(fh)
            print("no pickle-file for the Levee Failures needs to be created. It already exists")

    ######################################
    ##2st pickle check: Can data be received from preprocessed pickle file of the water depth
    ######################################
    if pickle_load_Depth==False:
        print("Start to read files and create pickle-file")
        d = ri.BasementInputs.DepthFile(filename_depth)
        print("Depth file sucessfully read")
        g = ri.BasementInputs.GeometryFile(filename_geom)
        print ("Geometry file sucessfully read")
        g.index=g.index+1
        DF_Depth=pd.concat([g,d], axis=1)
        print ("Concatenation done, writing pickle file")
        with open(filename_pickle_Depth, 'wb') as fh:
            pkl.dump(DF_Depth,fh,2)
    else:
        with open(filename_pickle_Depth, 'rb') as fh:
            DF_Depth=pkl.load(fh)
            print ("no pickle-file needs to be created. It already exists")
    print("Buildings-shp has been updated with the amount of nodes inside them")

    ######################################
    ##3rd pickle check: Can data be received from preprocessed pickle file of the nodes per building
    ######################################
    if pickle_load_NodePerBuilding==False:
        print("Creating pickle-file...")
        DF_BuildingsNodes = ri.shpInputs.NearestNodes(InputBuildings, NodesShp, buildings_firstjoin,
                                                      buildings_secondjoin)
        print("Nearest nodes sucessfully retrieved")
        with open(filename_pickle_Building_Nodes, 'wb') as fh:
            pkl.dump(DF_BuildingsNodes, fh, 2)
    else:
        with open(filename_pickle_Building_Nodes, 'rb') as fh:
            DF_BuildingsNodes = pd.read_pickle(fh, compression=None)
            print ("no pickle-file needs to be created. It already exists")


    BuildingsDepth=pdfp.PandasDF_join.JoiningBuildingsDepthExport(DF_BuildingsNodes, DF_Depth, Path_to_csv)
    #consumes a lot of memory
    print ("Buildings have successfully been assigned to the corresponding water depth per node")
    ap.ArcGISConversion.ArcGISPoint2Line(Current_lv, DF_LeveeFailures, BuildingsDepth, ArcGISTablepath, ArcGISPointFC,
                                         ArcGISLineFCPath,
                        ArcGISLineFC_joinPath, ArcGISLineFC_joinPath_WGS84, LeveeFailureLinesGeojson)
    print("Flooded Houses were sucessfully connected to the corresponding levee as a line feature and exported as"
          " a geojson-file")
    ap.ArcGISConversion.ArcGIS2geojson(Current_lv, ArcGISFC, ArcGISFC_WGS84, LeveeFailureBuildingsGeojson)
    pv.GeoJSONPreparation.ModifyGeoJSON(LeveeFailureBuildingsGeojson, LeveeFailureBuildingsGeojson_modified )
    print ("Geojson was successfully created from ArcGIS Feature Class")

    if Current_lv <= 4:
        pv.csvProcesses.AccumulateEntries(BuildingsDepth, Current_lv, Accum_csv_HasliAare)
    elif Current_lv <=43:
        pv.csvProcesses.AccumulateEntries(BuildingsDepth, Current_lv, Accum_csv_lowerAare)


###################################################################################
##Setting up loop for 42 runs (42 levee failures) and execution of the main method##
###################################################################################

##define first levee failure: Afterwards it will be updates through the script JoiningBuildingsDepthExport
Current_lv=ri.shpInputs.CountLeveeFailures(LeveeFailures_shp)[0]
Total_lv = ri.shpInputs.CountLeveeFailures(LeveeFailures_shp)[1]

while Current_lv <=Total_lv:
    print("Currently levee failure " + str(Current_lv) + " is beeing executed.")

    ##if the current levee failure has ID 1 to 4, use the 2dm-file from the Hasliaare, otherwise the one from the lower
    ## Aare Thun-Bern
    if Current_lv <=4:
        two_dm_file = InputFolder + "/HasliAare.2dm"
        mesh_nodes = InputFolder + "/HasliAare_nodes.shp"
        input_sol = InputFolder + "/.sol-files/HasliAare_nds_depth_LV" + str(Current_lv) + ".sol"
        Buildings = InputFolder + "/Buildings_HasliAare.shp"
    else:
        two_dm_file = InputFolder + "/Aare.2dm"
        mesh_nodes = InputFolder + "/lowerAare_nodes.shp"
        input_sol = InputFolder + "/.sol-files/AareThunBern_LV" + str(Current_lv) + "_nds_depth.sol"
        Buildings = InputFolder + "/Buildings_lowerAare.shp"

    ##Depending whether the levee failures of the Hasliaare or the lower Aare should be processed the .2dm file has
    ##to be changed. Therefore for the first 4 levee failures have the same .2dm file and the processed file can be
    ##written into a pickle. When it changes to levee failure 5 (lower Aare) the new .2dm file has to be taken and so
    ##the pickle file has to be created newly.
    if Current_lv == 1:
        PickleBuilding = False
        PickleLevee = False
    elif Current_lv == 5:
        PickleBuilding = False
        PickleLevee = True
    else:
        PickleBuilding = True
        PickleLevee = True

    ##main script will be executed heareafter
    main(PickleLevee, PickleData + "/DF_LeveeFailures" , LeveeFailures_shp, False, PickleData + "/Dataframe.pkl",
         input_sol, two_dm_file, Buildings, mesh_nodes, TempFolder + "/Buildings_join1.shp",
         TempFolder + "/Buildings_join2.shp", PickleBuilding, PickleData + "/Buildings_Nodes.pkl",
         OutputFolder + "/Flooded_Buildings_LV_" + str(Current_lv) + ".csv", TempFolder + "/temp.gdb/testTable",
         TempFolder + "/PointFC.shp", TempFolder + "/LineFC.shp", TempFolder + "/LineFC_join.shp",
         TempFolder + "/LineFC_join_WGS84.shp", OutputFolder + "/ConnectionLines_LV_" + str(Current_lv) +".geojson",
         TempFolder + "/PointFC.shp", TempFolder + "/PointFC_WGS84.shp",
         OutputFolder + "/Buildings_LV_" + str(Current_lv) +".geojson", OutputFolder + "/Buildings_LV_" +
         str(Current_lv) +"_modified.geojson")
    print("Levee failure " + str(Current_lv) + " was sucessfully written to a csv and geojson-file.")

    ##Delete temp-files
    TempFiles=[TempFolder + "/temp.gdb/test_table1", TempFolder + "/temp.gdb/test_fc",TempFolder +
               "/Buildings_join1.shp",
               TempFolder + "/Buildings_join2.shp", TempFolder + "/temp.gdb/testTable",
               TempFolder + "/PointFC.shp", TempFolder + "/LineFC.shp", TempFolder + "/LineFC_join.shp",
               TempFolder + "/LineFC_join_WGS84.shp",  TempFolder + "/PointFC.shp", TempFolder + "/PointFC_WGS84.shp"]

    for file in TempFiles:
        if arcpy.Exists(file):
            arcpy.Delete_management(file)

    Current_lv = Current_lv + 1

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

print ('Start to sort the accumulated csv for the lower Aare...')
pv.csvProcesses.sort_csv(Accum_csv_lowerAare, Accum_csv_lowerAare_mod)
print('Geojson files for the HasliAare successfully created')
print ('Successfully sorted the accumulated csv of the lower Aare. Start to prepare data for export to geojson...')
pv.GeoJSONPreparation.house2geojson(Accum_csv_lowerAare_mod, Building_connection_lowerAare,
                                    Building_connection_WGS84_lowerAare, house_geojson_lowerAare, OutputFolder,
                                    house_geojson_lowerAare_modified)
print('Geojson files for the lower Aare successfully created')
print('All calculations have been successfully terminated. The python script is now finished. Congratulation!')