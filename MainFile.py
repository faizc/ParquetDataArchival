import pyarrow.parquet as pq
from datetime import datetime
from dateutil import parser

# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    date1 = datetime(2020, 4, 1)
    date2 = datetime(2020, 5, 31)

    filesReadyForMovement = []
    filesWithConflict = []
    #list all the files in directory
    all_files = dbutils.fs.ls("/mnt/delta-table/yellow-taxi/")
    files = []
    for fileinfo in all_files:
        if(fileinfo.name.endswith('.parquet')):
            files.append(fileinfo.name)
    for fileName in files:
        filePath='/dbfs/mnt/delta-table/yellow-taxi/'+fileName
        #    print(filePath)
        parquet_file = pq.ParquetFile(filePath)
        pfile = pq.read_table(filePath)

        colName = 'tpep_pickup_datetime'
        colIndex = pfile.schema.get_field_index('tpep_pickup_datetime')
        numRowGroups = parquet_file.metadata.num_row_groups
        fileWithinRange = False
        cond = -1

        for rowGroup in range(0, numRowGroups):
            min = parser.parse(parquet_file.metadata.row_group(rowGroup).column(colIndex).statistics.min)
            max = parser.parse(parquet_file.metadata.row_group(rowGroup).column(colIndex).statistics.max)
            minFlag = min>date1
            maxFlag = max<date2
            if(minFlag and maxFlag):
                fileWithinRange = True
                cond = 1
            else:
                fileWithinRange = False
                cond = 2
                break

        if(fileWithinRange):
            filesReadyForMovement.append(fileName)
        else:
            filesWithConflict.append(fileName)