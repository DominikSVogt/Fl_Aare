import pandas as pd
import arcpy
import ArcGISProcesses as ap
import json


csv_buildings = "C:/Users/Dominik/OneDrive/Dokumente/UniBe/Master/Masterarbeit/99_Test/FlAare_202001/Output_Data/Accum_csv_HasliAare.csv"
levee_failures = "C:/Users/Dominik/OneDrive/Dokumente/UniBe/Master/Masterarbeit/99_Test/FlAare_202001/Input_Data/Aare_LeveeFailures_201911_LV03.shp"
temp_table = "C:/Users/Dominik/OneDrive/Dokumente/UniBe/Master/Masterarbeit/99_Test/FlAare_202001/Temp_Data/BuildingsLevees.dbf"
FeatureClass = "C:/Users/Dominik/OneDrive/Dokumente/UniBe/Master/Masterarbeit/99_Test/FlAare_202001/Temp_Data/FC_Build_Houses.shp"
FeatureClass_WGS84 = "C:/Users/Dominik/OneDrive/Dokumente/UniBe/Master/Masterarbeit/99_Test/FlAare_202001/Temp_Data/FC_Build_Houses_WGS84.shp"
output_geojson = "C:/Users/Dominik/OneDrive/Dokumente/UniBe/Master/Masterarbeit/99_Test/FlAare_202001/Output_Data/network_buildings_2_LF_HasliAare.geojson"
mod_output_geojson = "C:/Users/Dominik/OneDrive/Dokumente/UniBe/Master/Masterarbeit/99_Test/FlAare_202001/Output_Data/network_buildings_2_LF_HasliAare_modified.geojson"

# buildings = pd.read_csv(csv_buildings)
# columns_to_drop=["Wert", "MaxDepth", "deg_of_loss", "Damage"]
# buildings = buildings.drop(columns_to_drop, axis=1)
# buildings = buildings.rename(columns = {"X_centroid": "X_centroid", "Y_centroid": "Y_centroid", "V25OBJECTI": "ID", "Levee_Failure": "Levee"})
# levee_failures  = arcpy.da.FeatureClassToNumPyArray(levee_failures, ("X_center", "Y_center", "LF"))
# DF_LeveeFailures = pd.DataFrame(levee_failures)
# DF_LeveeFailures = DF_LeveeFailures.rename(columns = {"X_center": "X_centroid", "Y_center": "Y_centroid", "LF": "ID"})
# Reordered_List=["ID", "X_centroid", "Y_centroid"]
# DF_LeveeFailures=DF_LeveeFailures[Reordered_List]
# DF_LeveeFailures["Levee"]=DF_LeveeFailures["ID"]
#
# print("Dataframes of the buildings and of the levee failures were created")
#
# DF_complete = buildings.append(DF_LeveeFailures, ignore_index=True)
# print ("Dataframes successfully merged")
# Reorder=["ID", "Levee", "X_centroid", "Y_centroid"]
# DF_LeveeFailures=DF_LeveeFailures[Reorder]
# print ("convert to shapfile for transformation to WGS84")
#
# ap.Creation.ArcGISTable(DF_complete, temp_table)
# Event_Layer = "EventLayer"
# LV03=arcpy.SpatialReference(21781)
# arcpy.MakeXYEventLayer_management(table=temp_table, in_x_field = "X_centroid", in_y_field = "Y_centroid", out_layer= Event_Layer, spatial_reference=LV03, in_z_field=None)
# arcpy.CopyFeatures_management(Event_Layer, FeatureClass)
# arcpy.Project_management(FeatureClass, FeatureClass_WGS84,arcpy.SpatialReference(4326))
# print ("Begin to export as geojson...")
# arcpy.FeaturesToJSON_conversion(FeatureClass_WGS84, output_geojson, format_json="FORMATTED", geoJSON="GEOJSON")
# print ("Geojson exported")


with open (output_geojson) as f:
    dictionary = json.load(f)
print ("ok")

new_dictionary={}

for features in dictionary['features']:
    features["properties"]['lat'] = features["geometry"]["coordinates"][1]
    features["properties"]['lon'] = features["geometry"]["coordinates"][0]
    features["properties"]['id'] = features["properties"]['ID']
    features["geometry"]['uid1'] = features["properties"]['id']
    features["geometry"]['uid2'] = features["properties"]['Levee']
    # features["geometry"]['uid2'] = 1
    del (features["properties"]["FID"])
    del (features["properties"]["ID"])
    del (features["properties"]["X_centroid"])
    del (features["properties"]["Y_centroid"])
    del (features["properties"]["Levee"])
    del (features["geometry"]["coordinates"])
    del (features["geometry"]["type"])
    features["connections"] = features.pop("geometry")
    #new_dictionary[""].update(features)
del (dictionary["type"])


with open(mod_output_geojson, 'w', encoding='utf-8') as f:
    json.dump(dictionary, f, ensure_ascii=False, indent=4)
print ("Well done")