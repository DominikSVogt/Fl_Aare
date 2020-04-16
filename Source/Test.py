import arcpy
import ReadInputs as ri
import pandas as pd

file = "C:/Users/Dominik/OneDrive/Dokumente/UniBe/Master/Masterarbeit/99_Test/FlAare_202001/Temp_Data/test.shp"
file2 = "C:/Users/Dominik/OneDrive/Dokumente/UniBe/Master/Masterarbeit/99_Test/FlAare_202001/Temp_Data/test2.shp"
file3 = "C:/Users/Dominik/OneDrive/Dokumente/UniBe/Master/Masterarbeit/99_Test/FlAare_202001/Temp_Data/test3.shp"
path_file3 = "C:/Users/Dominik/OneDrive/Dokumente/UniBe/Master/Masterarbeit/99_Test/FlAare_202001/Temp_Data"
Nodes = "C:/Users/Dominik/OneDrive/Dokumente/UniBe/Master/Masterarbeit/99_Test/FlAare_202001/Input_Data/HasliAare_nodes_right_V3.xls"

# list = [file, file2]
#
# print ("Test")
# print ("starts")
# print ("now")
#
# b = ri.BasementInputs.DepthFile(
#     "C:/Users/Dominik/OneDrive/Dokumente/UniBe/Master/Masterarbeit/99_Test/FlAare_202001/Input_Data/.sol-files/Hasliaare_nds_depth_LV1.sol")
#
# dict_of_MaxDepth = b['MaxDepth'].to_dict()
#
# if arcpy.Exists(file3):
#     for files in list:
#         if arcpy.Exists(files):
#             arcpy.Delete_management(files)
#             print ("file" + str(files) + " sucessfully deleted")
#         else:
#             print ("file " + str(files) + (" does not exist and cannot be deleted"))
#
#     print ("Move to the next step")
# else:
#     print ("We first have to create file3")
#     arcpy.CreateFeatureclass_management(path_file3, "test3.shp", geometry_type="POINT", spatial_reference=arcpy.SpatialReference(4326))
#     print ("Feature class successfully created")
# print ("ok well done")

df = pd.read_excel(Nodes, delimiter=',')
List = ['Z', 'FID']
for item in List:
   df = df.drop(item, axis=1)
dictionary = df.set_index('NODE_ID').T.apply(tuple).to_dict()
print ('ok')