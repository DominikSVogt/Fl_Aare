import json
import pandas as pd
import arcpy
import copy
import os
import Vulnerability as vu
import numpy as np

class GeoJSONPreparation:

    def ModifyGeoJSON(Path_geoJSON, Path_newgeoJSON):
        with open (Path_geoJSON) as gj:
            geojson_dictionary=json.load(gj)
            for features in geojson_dictionary["features"]:
                features["properties"]["Depth"] = []

            for features in geojson_dictionary ['features']:
                for key, value in features["properties"].items():
                    if key.startswith("TS_"):
                        features["properties"]["Depth"].append(round(value, 3))
            print("Waterdepths successfully aggregated to a single list")

            ##delete coordinate points in between the beginning and end point of the line
            for parts in geojson_dictionary ['features']:
                length = len(parts["geometry"] ["coordinates"])
                del parts["geometry"] ["coordinates"][1:length-1]

            fields_to_delete = ["X_levee", "Y_levee", "X_levee_1", "Y_levee_1", "X_centroid", "Y_centroid",
                                "X_centro_1", "Y_centro_1", "V25OBJEC_1", "FID", "OBJECTID"]

            for features in geojson_dictionary ['features']:
                for key, value in features["properties"].items():
                    if key.startswith("TS_"):
                        fields_to_delete.append(key)
                    elif key.startswith("Node_"):
                        fields_to_delete.append(key)
                    else:
                        pass
            print ("the list fields_to_delete was extended and can now be deleted from the dictionary")

            for item in geojson_dictionary ['features']:
                for fields in fields_to_delete:
                    try:
                        del item['properties'][fields]
                    except KeyError:
                        pass
            print ("unnecessary keys and values sucessfully deleted")

            mod_file=open(Path_newgeoJSON, "w+")
            json.dump(geojson_dictionary, mod_file, indent=3)
            #by adding indent= e.g. 4 the json file can be structured for a more readable style. However, the file size
            #then increases
            print ("Modified Geojson file sucessfully written")

    def house2geojson(sorted_csv, building_connections, building_connections_WGS84, house_geojson,
                     Path_for_levee_based_geojson, house_geojson_modified):
        ##First modify the arrays created in the class 'sort_csv' so that they are numerical and not a string anymore.
        ##Further filter the dictionary containing the accumulated csv-data so that for each levee failure a geojson can
        ##be exported which only contains the (from this levee) affected buildings

        if arcpy.Exists(building_connections):
            print ("Process aborted. A feature class with the same name already exists")
        elif arcpy.Exists (building_connections_WGS84):
            print("Process aborted. A feature class with the same name already exists")
        else:
            arcpy.management.XYTableToPoint(sorted_csv, building_connections, "X_centroid", "Y_centroid",
                                        coordinate_system=21781)
            arcpy.Project_management(building_connections, building_connections_WGS84, 4326)
            arcpy.FeaturesToJSON_conversion(building_connections_WGS84, house_geojson, format_json="FORMATTED",
                                        geoJSON="GEOJSON")
        print("Geojson successfully written. Start modyfing Geojson...")

        with open(house_geojson, mode='r+') as gj:
            geojson_dictionary = json.load(gj)
            list_int = ['Levee_Failure', 'LF_Max']
            list_float = ['MaxDepth', 'Damage']

            for part in geojson_dictionary['features']:
                for key, value in part['properties'].items():
                    if key in list_float:
                        temp = value
                        temp1 = temp.strip('[ ] ,')
                        temp2 = temp1.split(",")
                        temp3 = [float(i) for i in temp2]
                        part['properties'][key] = temp3
                    elif key in list_int:
                        temp5 = value
                        temp6 = temp5.strip('[ ] ,')
                        temp7 = temp6.split(",")
                        temp8 = [int(i) for i in temp7]
                        part['properties'][key] = temp8
            print("The GeoJson file was transformed to a dictionary and modified.")
            print("Beginn to write to geojson file...")

            ##calculating the maximum number of levee failure that is affected
            temp_max = []
            temp_min = []
            for part in geojson_dictionary['features']:
                temp_max.append(max(part['properties']['Levee_Failure']))
                temp_min.append(min(part['properties']['Levee_Failure']))
            maximum = max(temp_max)
            minimum = min(temp_min)
            i = minimum

            ##select only those buildings which are affected by the levee failure i and writing a new geojson file
            # for each levee failure
            # while i <= maximum:
            #     features = []
            #     for part in geojson_dictionary['features']:
            #         for item in part['properties']['Levee_Failure']:
            #             if item == i:
            #                 features.append(part)
            #     if i <=4:
            #         new_geojson = Path_for_levee_based_geojson + '/Connection_Building_to_HasliAare_LF' + str(i)\
            #                       + '.geojson'
            #     elif i <=42:
            #         new_geojson = Path_for_levee_based_geojson + '/Connection_Building_to_lowerAare_LF' + str(i)\
            #                       + '.geojson'
            #     new_dictionary = copy.deepcopy(geojson_dictionary)
            #     new_dictionary['features'] = features
            #     file = open(new_geojson, "w+")
            #     json.dump(new_dictionary, file, indent=3)
            #     print('geojson file for levee number ' + str(
            #         i) + ' has sucessfully been created. Moving to the next number...')
            #     i = i + 1


            ##finaly create file with all houses affected by levees at the HasliAare or Untere Aare and close it
            mod_file = open(house_geojson_modified, "w+")
            json.dump(geojson_dictionary, mod_file, indent=3)
            print("Original geojson file successfully overwritten.")

    def UniteBuildingsPerLevee (accum_csv, Levee_failures_shp, LF_shp_WGS84, geojson, Current_LF):
        out_cor_system = arcpy.SpatialReference(4326)
        if not arcpy.Exists(LF_shp_WGS84):
            arcpy.Project_management(Levee_failures_shp, LF_shp_WGS84, out_coor_system=out_cor_system)
        else:
            print ("Dataset " + str(LF_shp_WGS84) + " already exists")
        if not arcpy.Exists(geojson):
            arcpy.FeaturesToJSON_conversion(LF_shp_WGS84, geojson, format_json="FORMATTED",
                                        geoJSON="GEOJSON")
        else:
            print("Dataset " + str(geojson) + " already exists")
        with open(geojson, mode='r+') as gj:
            geojson_dictionary = json.load(gj)

        df_csv = pd.read_csv(accum_csv)
        dict_csv = df_csv.to_dict()

        fields_to_delete=['Wert', 'X_centroid', 'Y_centroid', 'MaxDepth', 'deg_of_loss']
        for items in fields_to_delete:
            if items in dict_csv:
                del dict_csv[items]

        ##create new dictionary with subdictionaries and the information per levee
        new_dict={}
        i=1
        while i <= 42:
            k=0
            while k < len(dict_csv['Levee_Failure']):
                if dict_csv['Levee_Failure'][k]==i:
                    print("Move to Levee Failure from LF " + str(i))
                    if str(i) not in new_dict.keys():
                        new_dict[str(i)] = {}
                        new_dict[str(i)]['Levee_Failures'] = []
                        new_dict[str(i)]['Levee_Failures'].append(dict_csv ['Levee_Failure'][k])
                    elif 'Levee_Failures' not in new_dict[str(i)].keys():
                        new_dict[str(i)]['Levee_Failures'] = []
                        new_dict[str(i)]['Levee_Failures'].append(dict_csv['Levee_Failure'][k])
                    else:
                        new_dict[str(i)]['Levee_Failures'].append(dict_csv['Levee_Failure'][k])

                    print ("Move to V25OBJECTI from LF " + str(i))
                    if 'V25OBJECTI' not in new_dict[str(i)].keys():
                        new_dict[str(i)]['V25OBJECTI'] = []
                        new_dict[str(i)]['V25OBJECTI'].append(dict_csv ['V25OBJECTI'][k])
                    elif 'V25OBJECTI' in new_dict[str(i)].keys():
                        new_dict[str(i)]['V25OBJECTI'].append(dict_csv['V25OBJECTI'][k])
                    else:
                        print ("Something went wrong")

                    print("Move to Damage from LF " + str(i))
                    if 'Damage' not in new_dict[str(i)].keys():
                        new_dict[str(i)]['Damage'] = []
                        new_dict[str(i)]['Damage'].append(dict_csv ['Damage'][k])
                    elif 'Damage' in new_dict[str(i)].keys():
                        new_dict[str(i)]['Damage'].append(dict_csv['Damage'][k])
                    else:
                        print("Something went wrong")
                k = k + 1
            i = i + 1

        ##calculate total cost per levee failure
        for dict in new_dict.keys():
            print (dict)
            new_dict[dict]['Total_Damage'] =sum(new_dict[dict]['Damage'])
            new_dict[dict]['Affected_Buildings']=len(new_dict[dict]['Damage'])

        ##merge the two dictionaries
        for items in new_dict.keys():
            for parts in geojson_dictionary['features']:
                if parts['properties']['LF'] == int(items):
                    parts['properties']['Total_Damage'] = new_dict[items]['Total_Damage']
                    parts['properties']['Buildings'] = new_dict[items]['V25OBJECTI']
                    parts['properties']['Affected_Buildings'] = new_dict[items]['Affected_Buildings']

        # for parts in geojson_dictionary['features']:
        #     if 'Buildings' not in parts['properties'].keys():
        #         del geojson_dictionary['features'][parts]



        if Current_LF <=4:
            for parts in geojson_dictionary['features']:
                if parts['properties']['LF'] < 5:
                    if not 'temp' in geojson_dictionary.keys():
                        geojson_dictionary['temp']=[]
                        geojson_dictionary['temp'].append(parts)
                    else:
                        geojson_dictionary['temp'].append(parts)
        else:
            for parts in geojson_dictionary['features']:
                if parts['properties']['LF'] > 4:
                    if not 'temp' in geojson_dictionary.keys():
                        geojson_dictionary['temp']=[]
                        geojson_dictionary['temp'].append(parts)
                    else:
                        geojson_dictionary['temp'].append(parts)

        del geojson_dictionary['features']
        geojson_dictionary['features'] = geojson_dictionary.pop('temp')
        print ("successfully modified dictionary")

        ##drop modified dictionary back to geojson
        mod_file = open(geojson, "w+")
        json.dump(geojson_dictionary, mod_file, indent=3)

        print ("GEOJSON successfully modified")

    def CreateLinesBuildings2Levee(accum_csv, Levee_failures_shp, temp_table, temp_point, Line_Features,
                                   Line_Features_WGS84, ConnectionLines):

        df_csv = pd.read_csv(accum_csv)

        field_names=['X_center', 'Y_center', 'LF']
        arr = arcpy.da.TableToNumPyArray(Levee_failures_shp, (field_names))
        df = pd.DataFrame(arr)

        merged_df = df_csv.merge(df, left_on='Levee_Failure', right_on="LF")
        columns_to_drop = ['Wert', 'MaxDepth', 'deg_of_loss', 'Damage', 'LF']
        for columns in columns_to_drop:
            merged_df = merged_df.drop(columns, axis=1)
        merged_df = merged_df.rename(columns={'X_centroid': 'X_Build', 'Y_centroid': 'Y_Build', 'X_center': 'X_LF', 'Y_center': 'Y_LF'})

        x = np.array(np.rec.fromrecords(merged_df.values))
        names = merged_df.dtypes.index.tolist()
        x.dtype.names = tuple(names)
        if not arcpy.Exists(temp_table):
            arcpy.da.NumPyArrayToTable(x, temp_table)
        else:
            print ("Temporal table already exists. Cannot move on. Script aborted.")

        print ("ArcGIS Event layer will now be created.")
        EventLayer='Event_Layer_Lines'
        arcpy.MakeXYEventLayer_management(temp_table, "X_Build", "Y_Build", EventLayer,
                                          spatial_reference=arcpy.SpatialReference(21781))
        arcpy.CopyFeatures_management(EventLayer, temp_point)
        print ("Start to create ArcGIS line feature class...")
        arcpy.XYToLine_management(temp_point, Line_Features, "X_Build", "Y_Build", "X_LF",
                                  "Y_LF", id_field="V25OBJECTI")
        arcpy.JoinField_management(Line_Features, "X_LF", temp_table ,"X_LF", fields = "Levee_Failure")
        out_coor_system = arcpy.SpatialReference(4326)
        arcpy.Project_management(Line_Features, Line_Features_WGS84, out_coor_system=out_coor_system)
        arcpy.FeaturesToJSON_conversion(Line_Features_WGS84, ConnectionLines, format_json="FORMATTED", geoJSON="GEOJSON")
        print ('Finished converting and exporting to geojson')

        print ('begin to delete temporary files')
        files_to_delete=[temp_table, temp_point, Line_Features, Line_Features_WGS84]
        for files in files_to_delete:
            if arcpy.Exists(files):
                arcpy.Delete_management(files)
                print (str(files) + (" successfully deleted."))
            else:
                print ("File " + str(files) + " does not exist and cannot be deleted")
        print ("Successfully finished function")


class csvProcesses:

    def sort_csv(csv, sorted_csv):
        ##The function 'sort_csv' creates a pandas dataframe from the accumulated csv and then sorts and stacks the
        # entries, so that each house ID is represented no more than once. Therefore an array with the levee failures
        # affecting the house, an array with the corresponding maximum water depth, the vulnerability and the damage
        # potential is created.
        df = pd.read_csv(csv)
        df['MaxDepth'] = df['MaxDepth'].round(3)

        index_cols = df.columns.tolist()
        index_cols = [e for e in index_cols if e not in ['MaxDepth', 'Levee_Failure', 'deg_of_loss', 'Damage']]

        df2 = df.groupby(index_cols)["MaxDepth"].apply(list)
        df2 = df2.to_frame().reset_index()
        df2 = df2.drop(columns=['Wert', 'X_centroid', 'Y_centroid'])

        df3 = df.groupby(index_cols)["Damage"].apply(list)
        df3 = df3.to_frame().reset_index()
        df3 = df3.drop(columns=['Wert', 'X_centroid', 'Y_centroid'])

        df4 = df.groupby(index_cols)["Levee_Failure"].apply(list)
        df4 = df4.to_frame().reset_index()

        df5 = df4.merge(df2, left_on='V25OBJECTI', right_on='V25OBJECTI')
        df6 = df5.merge(df3, left_on='V25OBJECTI', right_on='V25OBJECTI')
        df6['No_of_levees'] = df6['Levee_Failure'].str.len()
        df6['Max_Water_Depth_overall'] = df6['MaxDepth'].apply(max)
        df6['Max_Damage'] = df6['Damage'].apply(max)

        df6['temp'] = df6.apply(
            lambda x: [i for i, e in enumerate(x['MaxDepth']) if e == x['Max_Water_Depth_overall']], axis=1)
        df6['LF_Max'] = df6.apply(lambda x: [x['Levee_Failure'][e] for e in (x['temp'])], axis=1)
        df6 = df6.drop(columns=['temp', 'Wert'])
        df6.to_csv(sorted_csv, index=False)
        print(
            "The csv was sucessfully sorted, shortened and compressed so that each building ID contaings links to"
            " the related levee failures")

    def AccumulateEntries(DF_Completed, Current_lv, Path_to_csv):
        ##This script saves the flooded houses with the max Depth and the current levee ID into a .csv for following
        # runs, the data will be added to the existing .csv file.
        DF_Completed.drop(DF_Completed.columns.difference(['V25OBJECTI', 'Wert', 'X_centroid', 'Y_centroid',
                                                           'MaxDepth']), 1, inplace=True)
        DF_Completed['Levee_Failure']=Current_lv

        deg_of_loss_list=[]
        for index, row in DF_Completed.iterrows():
            row['deg_of_loss']=vu.Vulnerability.vulnerabilityFuchs(row['MaxDepth'])
            deg_of_loss_list.append(row['deg_of_loss'])

        if len(DF_Completed) == len(deg_of_loss_list):
            DF_Completed['deg_of_loss'] = deg_of_loss_list
            print('Column of pandas dataframe could be updated successfully.')
        else:
            print('The length of the pandas dataframe and of the list do not match. Something went wrong. Please check'
                  ' your input list and dataframe')
        print ('Pandas dataframe modified with the degree of loss')

        DF_Completed['Damage'] = DF_Completed.apply(lambda row: (row['deg_of_loss'] * row['Wert']), axis=1)

        column_names=['V25OBJECTI', 'Wert', 'X_centroid', 'Y_centroid', 'MaxDepth', 'Levee_Failure', 'deg_of_loss', 'Damage']
        DF_Completed['MaxDepth'] = DF_Completed['MaxDepth'].round(3)
        DF_Completed['Damage'] = DF_Completed['Damage'].round(2)
        # if file does not exist write header
        if not os.path.isfile(Path_to_csv):
            DF_Completed.to_csv(Path_to_csv, header=column_names, index = False)
        else:  # else it exists so append without writing the header
            DF_Completed.to_csv(Path_to_csv, mode='a', header=False, index = False)