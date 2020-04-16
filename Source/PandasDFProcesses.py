import pandas as pd
import time
from ArcGISProcesses import ArcGISConversion


class PandasDF_join:

    def JoinHouses2Levees(csv, leveeFailures):

        df = pd.read_csv(csv)
        DF_LeveeFailures = ArcGISConversion.dbf2pandas(leveeFailures)
        df_merged = df.merge(DF_LeveeFailures.DF_LeveeFailures, left_on='Levee_Failure', right_on='DB', how='left')
        df_merged['MaxDepth'] = df_merged['MaxDepth'].round(3)
        print ("Successfully merged")

    def JoiningBuildingsDepthExport(DF_BuildingsNodes, DF_Depth, Path_to_csv):
        ##This script joines the two panda dataframes of the Buildings with the assigned Node_IDs to the corresponding
        # waterdepth and exports the resulting panda dataframe to an .csv file
        start_time = time.time()
        temp = (DF_BuildingsNodes[['Node_IDs']].applymap(lambda x: str.count(x, ',')))
        max_nodes_per_house = max(temp['Node_IDs'])+1
        ##
        #DF_BuildingsNodes = DF_BuildingsNodes.rename(columns={"Node_IDs": "NodeIDs"})
        Base = "Node_ID_"
        List = [Base+str(e) for e in range(1,max_nodes_per_house+1)]
        DF_BuildingsNodes[List] = DF_BuildingsNodes.Node_IDs.str.split(',', expand=True)
        DF_BuildingsNodes = DF_BuildingsNodes.apply(pd.to_numeric, errors='coerce')
        print ("Panda Dataframe of buildings with connected nodes was split into several columns each column only"
               " containing 1 node ID")
        print("--- %s seconds have elapsed within the jbde.py module ---" % (time.time() - start_time))

        List = [col for col in DF_BuildingsNodes if col.startswith('Node')]

        #code below divides the dataframe of the nodes per building so that each dataframe only has one node per house
        # left. Following that a join / merge of the different dataframes with different depths and nodes.
        # The nodes without a connection to a building are beiing deleted.

        DF_Extraction = {name: pd.DataFrame(DF_BuildingsNodes) for name in List}
        DF_Join={}
        DF_Extraction.pop('Node_IDs', None)
        indexNames = DF_Depth[DF_Depth['MaxDepth'] == 0].index
        DF_Depth.drop(indexNames, inplace=True)
        del List, temp, indexNames
        p = 1
        for name, df in DF_Extraction.items():
            k='Node_ID_'+str(p)
            df.drop(df.columns.difference(['V25OBJECTI', 'Wert', 'X_centroid', 'Y_centroid', k]), 1, inplace=True)
            DF_Join[k + "_conc"]=pd.merge(DF_Extraction[name], DF_Depth, left_on=str(name), right_on='Node_IDs',
                                          how='outer')
            DF_Join[k + "_conc"].dropna(how='any', inplace=True)
            p=p+1

        print("--- %s seconds have elapsed within the jbde.py module ---" % (time.time() - start_time))
        #putting together the different dataframes so that each house only contains one water depth per time. Therefore,
        #all water depth inside a building are averaged.

        DF_Completed = pd.concat(DF_Join, sort=False).groupby('V25OBJECTI', as_index=False).mean().drop\
            (columns=['Node_IDs'])

        #sort columns
        All_Column_names=list(DF_Completed.columns)
        TS_Columns = [col for col in DF_Completed if col.startswith('TS')]
        Node_Columns = [col for col in DF_Completed if col.startswith('Node')]
        MaxDepth_columns=[col for col in DF_Completed if col.startswith('Max')]
        Building_columns = list(set(All_Column_names) - set(TS_Columns) - set (Node_Columns) - set (MaxDepth_columns))
        Building_columns.sort()
        NewOrder_columns=Building_columns+ Node_Columns+ MaxDepth_columns+ TS_Columns
        DF_Completed=DF_Completed.reindex(columns=NewOrder_columns)
        DF_Completed=DF_Completed.drop(['X','Y'], axis=1)
        print("--- %s seconds have elapsed within the jbde.py module ---" % (time.time() - start_time))

        #the last step exports the above averaged dataframe as .csv.
        try:
            DF_Completed.to_csv(Path_to_csv, index=False)
            print ("CSV was successfully exported")
        except:
            print ("CSV cannot be created. Check for any mistakes.")

        print ("Succuessfuly joined Buildings with corresponding nodes, assigned water depth and exported the"
               " resulting pandas dataframe as .csv")
        print("--- %s seconds have elapsed within the jbde.py module ---" % (time.time() - start_time))
        return DF_Completed