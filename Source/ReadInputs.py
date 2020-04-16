import pandas as pd
import arcpy
import copy
import sys

class helpfunctions:
    #devides input file of one column into several columns of specific length
    # Yield successive n-sized chunks from l.
    # n -> How many elements each list should have
    def divide_chunks(l, n):
        # looping till length l
        for i in range(0, len(l), n):
            yield l[i:i + n]

class BasementInputs:

    def DepthFile (filename):

        ##read .sol file with the waterdepth
        df = None  # panda dataframe
        NodeCount = 0
        TSCount = 0

        # Count number of nodes
        with open(filename) as isf:
            for line in isf:
                if line.startswith("ND"):
                    NodeCount = int(line[2:])

        with open(filename) as isf:
            lines = isf.read().splitlines()
        print("Lines in Depth file sucessfully split")
        newList = lines[8:]

        x = list(helpfunctions.divide_chunks(newList, NodeCount + 1))
        x = x[:-1]

        # Counts how many Timesteps there are in file. More complicated way had to be chosen because the word
        # "timeunits" contains "TS" as well. Therefore, the "TS" in the input file cannot be counted
        TSCount = 0
        for line in lines:
            if line.startswith("TS"):
                TSCount = TSCount + 1
        print("amount of TS sucessfully counted")
        Header = []
        for line in lines:
            if line.startswith("TS"):
                Header.append(line)

        x_transposed = list(zip(*x))
        df = pd.DataFrame(x_transposed, columns=Header)

        df = df[1:]

        ##Converting scientific notation to float
        df = df.astype(float)
        print("Dataframe sucessfully transposed")
        ##convert scientific notation in column names to integers
        list(df)

        TSNumbers = []
        for items in Header:
            TSNumbers.append(items[5:])

        TSNumbers_int = []
        for items in TSNumbers:
            try:
                TSNumbers_int.append(int(float(items)))
            except ValueError:
                TSNumbers_int.append(int(0))
        print("Convertion of numbers in header from scientific notation to integer has started")
        Header_int = []
        n = 0
        for items in Header:
            Header_int.append(items[:3] + str(TSNumbers_int[n]))
            n = n + 1

        df.columns = Header_int
        print("Headers sucessfully converted into strings with decimal notation")

        # add column with the max depth over all time steps
        df['MaxDepth'] = df[[col for col in df if col.startswith('TS')]].max(axis=1)
        print("Column successfully added")
        df = df.reset_index(drop=True)
        df.index += 1
        return df

    def GeometryFile(filename):
        df_Nodes = None # panda dataframe

        with open(filename) as geom:
            nodes_string = [line.strip() for line in geom if line.startswith("ND")]

        nodes_list = []
        for elements in nodes_string:
            nodes_list.append(elements.split())

        print("List containing nodes sucessfully created")

        header = ["Node", "Node_IDs", "X", "Y", "Z"]
        df_Nodes = pd.DataFrame(nodes_list, columns=header)
        df_Nodes = df_Nodes.drop(columns="Node")
        # Pandas rounds the last number behind comma. Guess it can only handle 5 digits after the comma.
        df_Nodes = df_Nodes.astype({"Node_IDs": int, "X": float, "Y": float, "Z": float, })
        df_Nodes = df_Nodes.drop(columns='Z')
        return df_Nodes

    def twodmFile (filename):

        df_Edges = None  # panda dataframe

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

        # Conversion of pandas df for beeing able to have only two columns left for the edges.
        column_names = ["Node_1", "Node_2"]
        df_Edges_1 = copy.deepcopy(df_Edges).drop(columns="Node_ID_3")
        df_Edges_1.columns = column_names
        df_Edges_2 = copy.deepcopy(df_Edges).drop(columns="Node_ID_1")
        df_Edges_2.columns = column_names
        df_Edges_3 = copy.deepcopy(df_Edges).drop(columns="Node_ID_2")
        df_Edges_3.columns = column_names
        df_Edges_total = df_Edges_1.append(df_Edges_2, ignore_index=True)
        df_Edges_total = df_Edges_total.append(df_Edges_3, ignore_index=True)
        return df_Edges_total

class shpInputs():

    def ReadBuildings(shpBuildings):
        ##converts shp to pandas dataframe
        DFBuilding = None

        # Test to check wether geometry type of input is of type polygon
        desc = arcpy.Describe(shpBuildings)
        geometryType = desc.shapeType

        if geometryType == 'Polygon':
            print("Building-input-data is ok")
            arrayBuildings = arcpy.da.FeatureClassToNumPyArray(shpBuildings, (
                "V25OBJECTI", "Wert", "Typ", "AnzPers", "X_centroid", "Y_centroid"))
            DFBuilding = pd.DataFrame(arrayBuildings)
            print("Buildings have been added to a new dataframe")
            return DFBuilding
        else:
            print("Building-input-data is not the expected format of polygons")

    def CountLeveeFailures(LeveesFailures_shp):
        ##This script counts the amount of levee failures. After each itteration number will be increased so that the
        # next levee failure can be processed.

        if 'Current_LF' not in globals():
            Current_lv = 0

        LeveeFailures_shp = LeveesFailures_shp
        Total_Levee_Failures = int(arcpy.GetCount_management(LeveesFailures_shp).getOutput(0))
        Current_lv = Current_lv + 1
        return Current_lv, Total_Levee_Failures

    def NearestNodes(shpBuildingsOriginal, shpNodes, buildings_firstjoin, buildings_secondjoin):
        ##This script reads a point dataset and updates a containing polygon dataset with various statistics. The result
        #is joined and exportedto a dataframe.

        DF_BuildingsNodes = None  # panda dataframe of buildings combined with water depth

        ##check if shapefile buildings_firstjoin already exists
        if not arcpy.Exists(buildings_firstjoin):

            ##check if field "Node_IDS" already exist in shapefile
            if len(arcpy.ListFields(shpBuildingsOriginal, "Node_IDs")) > 0:
                print("The attribute field NodeIDs already exists and can not be created")
            else:

                # Create a new fieldmappings and add the two input feature classes.
                fieldmappings = arcpy.FieldMappings()
                fieldmappings.addTable(shpBuildingsOriginal)
                fieldmappings.addTable(shpNodes)

                NodeID = fieldmappings.findFieldMapIndex("NODE_ID")
                fieldmap = fieldmappings.getFieldMap(NodeID)

                # Get the output field's properties as a field object
                field = fieldmap.outputField

                # Rename the field and pass the updated field object back into the field map
                field.name = "Node_IDs"
                field.aliasName = "Node_IDs"
                field.type = "TEXT"
                field.length = 255
                fieldmap.outputField = field

                # Set the merge rule to join and then replace the old fieldmap in the mappings object with the updated
                # one
                fieldmap.mergeRule = "join"
                fieldmap.joinDelimiter = ","
                fieldmappings.replaceFieldMap(NodeID, fieldmap)

                arcpy.SpatialJoin_analysis(shpBuildingsOriginal, shpNodes, buildings_firstjoin,
                                           join_operation="JOIN_ONE_TO_ONE", join_type="KEEP_ALL",
                                           field_mapping=fieldmappings, match_option="INTERSECT")
                print("The first spatial join has been done successfully joining the Node-IDs which are located"
                      " in the buildings to the buildings themselves")

                #next step is to calculate a near-analysis for all features / buildings that do not contain any points

            ##check if shapefile buildings_secondjoin already exists
            if not arcpy.Exists(buildings_secondjoin):

                arcpy.SpatialJoin_analysis(buildings_firstjoin, shpNodes, buildings_secondjoin,
                                           join_operation="JOIN_ONE_TO_ONE", join_type="KEEP_ALL",
                                           match_option="CLOSEST")
                print("The second spatial join has been made by calculating the nearest Node for each building")

                ##combine the two spatial joins. Means that if the field for NodeIDs in the house is empty, write the
                # NodeID of the closest point into that field

                attribute_values = ['Node_IDs', 'NODE_ID']
                with arcpy.da.UpdateCursor(buildings_secondjoin, attribute_values) as cursor:
                    for row in cursor:
                        if row[0] == ' ':
                            row[0] = str(row[1])
                            cursor.updateRow(row)
                print("Fields of the two spatial joins have been successfully merged. The field -Node_IDs- now contains"
                    " the NodeIDs of the nodes inside a building or if there is not node inside a building the nearest"
                      " node")

                ##drop the not wanted attributes of the building shp-file
                fields = arcpy.ListFields(buildings_secondjoin)

                # manually enter field names to keep here; include mandatory fields name such as OBJECTID (or FID),
                # and Shape in keepfields
                keepFields = ["FID", "Shape", "V25OBJECTI", "V25YEAROFC", "HEIGHT", "AnzPers", "Wert", "Typ",
                              "GKATS", "BAUZONE", "gebCHid", "Node_IDs", "X_centroid", "Y_centroid"]

                dropFields = [x.name for x in fields if x.name not in keepFields]
                # delete fields
                arcpy.DeleteField_management(buildings_secondjoin, dropFields)
                print("Unnecessary fields have been deleted")

        ##create panda dataframe from the shp-file containing the buildings and nodes
        # as we do not want all attributes contained in the dataframe, we make a subselection
        DF_BuildingsNodes = pd.DataFrame(arcpy.da.FeatureClassToNumPyArray(buildings_secondjoin,
                                                                                ['V25OBJECTI', 'Wert', 'Node_IDs',
                                                                                 'X_centroid', 'Y_centroid'],
                                                                                skip_nulls=False, null_value=-99999))
        print("Panda Dataframe containing the assignment of node IDs to corresponding buildings has been"
              " successfully created")
        return DF_BuildingsNodes