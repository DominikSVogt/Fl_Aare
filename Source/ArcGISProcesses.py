import arcpy
import numpy as np
import time
import pandas as pd
import psutil

class ArcGISConversion:
    ##This script creates an ArcGIS Line-feautre class from the flood affected houses and the corresponding levee failures.

    def ArcGISPoint2Line(Current_lv, DF_LeveeFailures, CompleteDataframe, ArcGISTablepath, ArcGISPointFC,
                         ArcGISLineFCPath, ArcGISLineFC_joinPath, ArcGISLineFC_joinPath_WGS84,
                         LeveeFailureLinesGeojson):
        DF_Completed = None
        Point = None
        start_time = time.time()

        #first of all, let's create an ArcGIS table from the panda dataframe
        FilesList=[ArcGISTablepath, ArcGISPointFC, ArcGISLineFCPath, ArcGISLineFC_joinPath,
                   ArcGISLineFC_joinPath_WGS84, LeveeFailureLinesGeojson]
        print(psutil.virtual_memory())
        for objects in FilesList:
            if arcpy.Exists(objects):
                arcpy.Delete_management(objects)
                print ("The file " + objects + " was deleted")
        DF_Completed=CompleteDataframe
        x = np.array(np.rec.fromrecords(DF_Completed.values))
        names = DF_Completed.dtypes.index.tolist()
        x.dtype.names = tuple(names)
        arcpy.da.NumPyArrayToTable(x, ArcGISTablepath)
        print ("ArcGIS table was successfully written")
        print("--- %s seconds have elapsed within the ArcGISPoint2Line method ---" % (time.time() - start_time))

        X_CentroidOfLeveeFailure = DF_LeveeFailures.at[Current_lv, 'X_center']
        Y_CentroidOfLeveeFailure=DF_LeveeFailures.at[Current_lv, 'Y_center']
        arcpy.AddField_management(ArcGISTablepath, "X_levee", "long")
        arcpy.AddField_management(ArcGISTablepath, "Y_levee", "long")
        arcpy.CalculateField_management(ArcGISTablepath, "X_levee", X_CentroidOfLeveeFailure)
        arcpy.CalculateField_management(ArcGISTablepath, "Y_levee", Y_CentroidOfLeveeFailure)
        EventLayer="Event_Layer"
        arcpy.MakeXYEventLayer_management(ArcGISTablepath, "X_centroid", "Y_centroid", EventLayer, spatial_reference=arcpy.SpatialReference(21781))
        arcpy.CopyFeatures_management(EventLayer, ArcGISPointFC)
        print("--- %s seconds have elapsed within the ArcGISPoint2Line method ---" % (time.time() - start_time))

        print(psutil.virtual_memory())
        #arcpy.env.overwriteOutput = True
        #arcpy.env.qualifiedFieldNames = False
        arcpy.XYToLine_management(ArcGISPointFC, ArcGISLineFCPath, "X_levee", "Y_levee", "X_centroid",
                                  "Y_centroid", id_field="V25OBJECTI")
        JoinedLineFC=arcpy.AddJoin_management(ArcGISLineFCPath, "V25OBJECTI", ArcGISTablepath, "V25OBJECTI")
        print ("Successful 1")
        arcpy.CopyFeatures_management(JoinedLineFC, ArcGISLineFC_joinPath)
        print("Successful 2")
        arcpy.Project_management(ArcGISLineFC_joinPath, ArcGISLineFC_joinPath_WGS84, 4326)
        print("Successful 3")
        arcpy.FeaturesToJSON_conversion(ArcGISLineFC_joinPath_WGS84, LeveeFailureLinesGeojson, format_json="FORMATTED",geoJSON="GEOJSON")
        print("Successful 4")
        print ("Line Feature Class was successfully created, projected to WGS84 and exported as GEOJSON")
        print("--- %s seconds have elapsed within the ArcGISPoint2Line method ---" % (time.time() - start_time))

    def ArcGIS2geojson (Current_lv, ArcGIS_FeatureClass, ArcGIS_FeatureClass_WGS84, GeojsonFilePath):
        ##This script creates a .geojson file from an ArcGIS feature class.
        arcpy.Project_management(ArcGIS_FeatureClass, ArcGIS_FeatureClass_WGS84, 4326)
        arcpy.AddField_management(ArcGIS_FeatureClass_WGS84, "Levee", "short")
        arcpy.AddField_management(ArcGIS_FeatureClass_WGS84, "Time", "string", field_length=50)
        arcpy.CalculateField_management(ArcGIS_FeatureClass_WGS84, "Levee", Current_lv)
        Time = '"' + str("2006-03-11T09:00:00") + '"'
        arcpy.CalculateField_management(ArcGIS_FeatureClass_WGS84, "Time", Time)
        arcpy.FeaturesToJSON_conversion(ArcGIS_FeatureClass_WGS84, GeojsonFilePath, format_json="FORMATTED",
                                        geoJSON="GEOJSON")
        print("ArcGIS to Geojson conversion successfully finished")

    def dbf2pandas(LeveeFailures_shp):
        ## a pandas dataframe will be created from the shp-file of the levee failures
        DF_LeveeFailures = None

        LeveeFailures_shp = LeveeFailures_shp
        final_fields = [field.name for field in arcpy.ListFields(LeveeFailures_shp)]
        DeleteStuff = ["FID", "Shape", "MATID"]
        for element in DeleteStuff:
            if element in final_fields: final_fields.remove(element)
        data = [row for row in arcpy.da.SearchCursor(LeveeFailures_shp, final_fields)]
        if 'DB' in final_fields:
            DF_LeveeFailures = pd.DataFrame(data, columns=final_fields).sort_values('LF', ascending=True).reset_index(
                drop=True)
        else:
            DF_LeveeFailures = pd.DataFrame(data, columns=final_fields).reset_index(drop=True)
        DF_LeveeFailures.index += 1
        return DF_LeveeFailures
        print("Pandas Dataframe of levee failures was sucessfully created.")
        
class Creation:

    def ArcGISTable(joinedPanda, Workspace_table_name):
        # hereafter an ArcGIS table will be created from the joined panda dataframes
        dataframe = None
        Workspace_path_with_table_name = None

        Workspace_path_with_table_name = Workspace_table_name
        dataframe = joinedPanda
        if not arcpy.Exists(Workspace_table_name):
            x = np.array(np.rec.fromrecords(dataframe.values))
            names = dataframe.dtypes.index.tolist()
            x.dtype.names = tuple(names)
            arcpy.da.NumPyArrayToTable(x, Workspace_path_with_table_name)
            print("ArcGIS Table sucessfully created")
        else:
            print("ArcGIS Table already exists")

    def ArcGISFeatureClass(arcgisTable, Workspace_fc_name):
        arcgistable = None
        Workspace_path = None
        Workspace_path = Workspace_fc_name

        arcgistable = arcgisTable
        print("The following function will only be executed correctly if the input data contain data in the coordinate"
            " system LV03. Otherwise a manual transformation will have to be done before")
        if not arcpy.Exists(Workspace_fc_name):
            arcpy.MakeXYEventLayer_management(arcgistable, 'X', 'Y', 'EventLayer',
                                              arcpy.SpatialReference(21781), 'Z')
            print("event layer successfully created")
            fc = arcpy.CopyFeatures_management('EventLayer', Workspace_fc_name)
            print("fc successfully created")
        else:
            print("Feature Class already exists and will not be overwritten")