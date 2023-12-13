# SPLIT DATA
"""
COUNT = 0
with open('../../../Dropbox-heavy-stuff/ocldb1567259534.30032.OSD.csv', 'r') as f:
	DATA = str()

	for line in f:
		if COUNT < 2000000:
			if line[0] == '#':
				COUNT += 1
				with open('../../../Dropbox-heavy-stuff/tmp-csv/data_{}.csv'.format(COUNT), 'w') as g:
					g.write(DATA)
				DATA = str()
			else:
				DATA += line
print(COUNT)
"""


# BUILD CSV

"""
import pandas as pd
import numpy as np

FULL = 185101+1
df   = pd.DataFrame(columns=
['Depth',
 'Temperatur',
 'Salinity',
 'Pressure',
 'ID',
 'LAT',
 'LON',
 'YEAR',
 'MONTH',
 'DAY'])

for i in range(2, 100):
	print(i)
	CSV = str()
	filename = open('../../../Dropbox-heavy-stuff/tmp-csv/data_{}.csv'.format(i), 'r')
	f = filename.readlines()
	filename.close()

	VAR_INDEX   = 0
	VAR_START   = 0
	ID_INDEX    = 0
	LAT_INDEX   = 0
	LON_INDEX   = 0
	YEAR_INDEX  = 0
	MONTH_INDEX = 0
	DAY_INDEX   = 0

	for j,v in enumerate(f):
		if 'Temperatur' in v:
			VAR_INDEX = j
		if 'Prof-Flag' in v:
			VAR_START = j+1 
		if 'CAST' in v:
			ID_INDEX = j
		if 'Latitude' in v:
			LAT_INDEX = j
		if 'Longitude' in v:
			LON_INDEX = j
		if 'Year' in v:
			YEAR_INDEX = j
		if 'Month' in v:
			MONTH_INDEX = j
		if 'Day' in v:
			DAY_INDEX = j

	ID    = f[ID_INDEX].split(',')[2].replace(' ', '')
	LAT   = f[LAT_INDEX].split(',')[2].replace(' ', '')
	LON   = f[LON_INDEX].split(',')[2].replace(' ', '')
	YEAR  = f[YEAR_INDEX].split(',')[2].replace(' ', '')
	MONTH = f[MONTH_INDEX].split(',')[2].replace(' ', '')
	DAY   = f[DAY_INDEX].split(',')[2].replace(' ', '')
	FILENAME = '{}_{}_{}_{}_{}_{}'.format(ID, YEAR, MONTH, DAY, LAT, LON)

	header = '# ,' + f[VAR_INDEX]
	header = header.replace(' ', '')
	CSV += header.replace('VARIABLES ,', '')
	for line in f[VAR_START:]:
	    if 'END' not in line:
	        CSV += line

	with open('../../../Dropbox-heavy-stuff/csv/{}.csv'.format(FILENAME), 'w') as g:
	    g.write(CSV)

	_df = pd.read_csv('../../../Dropbox-heavy-stuff/csv/{}.csv'.format(FILENAME), delimiter=',')
	_df.loc[:, 'ID']    = ID
	_df.loc[:, 'LAT']   = str(LAT)
	_df.loc[:, 'LON']   = str(LON)
	_df.loc[:, 'YEAR']  = YEAR
	_df.loc[:, 'MONTH'] = MONTH
	_df.loc[:, 'DAY']   = DAY

	_df.to_csv('../../../Dropbox-heavy-stuff/csv/{}_2.csv'.format(FILENAME))

	try:
		df = df.append(_df[['Depth',
							 'Temperatur',
							 'Salinity',
							 'Pressure',
							 'ID',
							 'LAT',
							 'LON',
							 'YEAR',
							 'MONTH',
							 'DAY']], ignore_index=True)
	except:
		print(i ,LAT, LON)

df.to_csv('../../../Dropbox-heavy-stuff/csv/FINAL.csv')
"""

import pickle as pkl

import numpy as np
import pandas as pd
from IPython.display import IFrame, display

FULL = 185101 + 1
df = pd.DataFrame(
    columns=[
        "VARIABLES",
        "Depth",
        "uncertain",
        "F",
        "O",
        "Temperatur",
        "uncertain",
        "F",
        "O",
        "Salinity",
        "uncertain",
        "F",
        "O",
        "Pressure",
        "uncertain",
        "ID",
        "LAT",
        "LON",
        "YEAR",
        "MONTH",
        "DAY",
        "depth_u",
        "temp_u",
        "salt_u",
        "press_u",
    ]
)

LIST_VAR = list()
LIST_VAR_ID = 19

# LEMBRETE IMPORTANTE:
# 10001, 20001, 30001, etc, ficaram de fora sem querer, lembrar de corrigir

for i in range(180002, 190001):
    if i % 500 == 0:
        print(i, df.shape)

    CSV = list()
    filename = open("../../../Dropbox-heavy-stuff/tmp-csv/data_{}.csv".format(i), "r")
    f = filename.readlines()
    filename.close()

    VAR_INDEX = 0
    VAR_START = 0
    ID_INDEX = 0
    LAT_INDEX = 0
    LON_INDEX = 0
    YEAR_INDEX = 0
    MONTH_INDEX = 0
    DAY_INDEX = 0
    UNIT_INDEX = 0

    for j, v in enumerate(f):
        if "Temperatur" in v:
            VAR_INDEX = j
        if "Prof-Flag" in v:
            VAR_START = j + 1
        if "CAST" in v:
            ID_INDEX = j
        if "Latitude" in v:
            LAT_INDEX = j
        if "Longitude" in v:
            LON_INDEX = j
        if "Year" in v:
            YEAR_INDEX = j
        if "Month" in v:
            MONTH_INDEX = j
        if "Day" in v:
            DAY_INDEX = j
        if "UNITS" in v:
            UNIT_INDEX = j

    ID = f[ID_INDEX].split(",")[2].replace(" ", "")
    LAT = f[LAT_INDEX].split(",")[2].replace(" ", "")
    LON = f[LON_INDEX].split(",")[2].replace(" ", "")
    YEAR = f[YEAR_INDEX].split(",")[2].replace(" ", "")
    MONTH = f[MONTH_INDEX].split(",")[2].replace(" ", "")
    DAY = f[DAY_INDEX].split(",")[2].replace(" ", "")

    UNITS = f[UNIT_INDEX].split(",")
    depth_u = UNITS[1].replace(" ", "")
    temp_u = UNITS[5].replace(" ", "")
    salt_u = UNITS[9].replace(" ", "")
    press_u = UNITS[13].replace(" ", "")

    FILENAME = "{}_{}_{}_{}_{}_{}".format(ID, YEAR, MONTH, DAY, LAT, LON)

    header = "# ," + f[VAR_INDEX]
    header = header.replace(" ", "").replace("VARIABLES ,", "")
    header = header.rstrip().split(",")[1:-4]

    for line in f[VAR_START:]:
        if "END" not in line:
            CSV.append(line.rstrip().split(",")[:-3])

    try:
        _df = pd.DataFrame(CSV, columns=header)
        _df.loc[:, "ID"] = ID
        _df.loc[:, "LAT"] = str(LAT)
        _df.loc[:, "LON"] = str(LON)
        _df.loc[:, "YEAR"] = YEAR
        _df.loc[:, "MONTH"] = MONTH
        _df.loc[:, "DAY"] = DAY
        _df.loc[:, "depth_u"] = depth_u
        _df.loc[:, "temp_u"] = temp_u
        _df.loc[:, "salt_u"] = salt_u
        _df.loc[:, "press_u"] = press_u
    except:
        LIST_VAR.append(FILENAME)

    try:
        df = df.append(_df, ignore_index=True)
    except:
        LIST_VAR.append(FILENAME)

df = df.replace("   ---0---", np.nan)
df["Depth"] = df["Depth"].astype(float)
df["Temperatur"] = df["Temperatur"].astype(float)
df["Salinity"] = df["Salinity"].astype(float)
df["Pressure"] = df["Pressure"].astype(float)

df.to_csv("../../../Dropbox-heavy-stuff/csv/FINAL_{}.csv".format(LIST_VAR_ID))
pkl.dump(
    LIST_VAR,
    open("../../../Dropbox-heavy-stuff/csv/list_var_{}.p".format(LIST_VAR_ID), "wb"),
)
# pkl.dump(df, open('../../../Dropbox-heavy-stuff/csv/FINAL_1.p', 'wb'))
