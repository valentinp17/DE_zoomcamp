For Mac don't forget:
export C_INCLUDE_PATH=/Library/Devpreeloper/CommandLineTools/Library/Frameworks/Python3.framework/Versions/3.8/Headers

run postgres
docker run -it 
-e POSTGRES_USER="root" 
-e POSTGRES_PASSWORD="root" 
-e POSTGRES_DB="ny_taxi" 
-v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data 
-p 5432:5432 
postgres:13
docker run -it -e POSTGRES_USER="root" -e POSTGRES_PASSWORD="root" -e POSTGRES_DB="ny_taxi" -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data -p 5432:5432 postgres:13
p/greeb5oss –publish 5432:5432 –publish 88:22 –volume `pwd`:/code kochanpivotal/gpdb5oss bin/bash