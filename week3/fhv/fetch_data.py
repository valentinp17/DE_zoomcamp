# download the fhv data
import os
months = range(1, 13)
for month in months:
    url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-{month:02d}.csv.gz'
    os.system('wget ' + url)