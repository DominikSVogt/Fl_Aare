# This script reads a point dataset and updates a containing polygon
#  dataset with various statistics

import arcpy
arcpy.env.overwriteOutput = True

Points = "D:\\Data\\Geodatabase.gdb\\PointFC"
Zones = "D:\\Data\\Geodatabase.gdb\\PolygonFC"
zoneNameField = "NAME"
zoneIncidentsField = "INCIDENTS"

# This tuple holds all the field names to be used.
fieldsToUse = (zoneNameField, zoneIncidentsField)

# Make a feature layer of all the graffiti incidents
arcpy.MakeFeatureLayer_management(Points, "PointsLayer")

try:

    with arcpy.da.UpdateCursor(Zones, fieldsToUse) as ZoneRows:
        for Zone in ZoneRows:

            # Create a query string for the current zone using the zoneNameField (which is index 0 in the fieldsToUse tuple)
            zoneName = Zone[0]
            queryString = '"' + zoneNameField + '" = ' + "'" + zoneName + "'"

            # Make a feature layer of just the current zone polygon
            arcpy.MakeFeatureLayer_management(Zones, "CurrentZoneLayer", queryString)

            # Narrow down the incidents layer by selecting only the incidents
            #  that fall within the current zone
            arcpy.SelectLayerByLocation_management("PointsLayer", "CONTAINED_BY", "CurrentZoneLayer")

            try:
                # Count the number of incidents selected    
                selectedIncidentsCount = arcpy.GetCount_management("PointsLayer")
                numIncidents = int(selectedIncidentsCount.getOutput(0))

                # Write the count to the zoneIncidentsField (which is index 1 in the fieldsToUse tuple)
                Zone[1] = numIncidents

                # Call update row to apply the changes
                ZoneRows.updateRow(Zone)


            # Clean up feature layers, even if an error occurred
            finally:     
                arcpy.Delete_management("CurrentZoneLayer")

finally:
    arcpy.Delete_management("PointsLayer")




    