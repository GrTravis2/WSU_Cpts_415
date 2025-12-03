# CPTS 322 SWE Project - F25

## Contributors

## Installation and setup
1. Make sure to have python available with version compatible with pyproject spec
2. Ensure you are in the correct directory and make python virtual env in folder with name ".venv":
    `python3 -m venv .venv/`
3. Activate venv before proceeding to package install, may vary depending on system. For Unix-like systems, try: `source .venv/bin/activate`
4. Before installation do a final check with `which python`, validate that the output file path is your local venv!
5. Download packages with respect to dev dependencies called out in the pyproject spec. Give `python -m pip install -e ".[dev]"` a try.
    - Note: The double quotes around `".[dev]"` may be critical depending on your shell, for example they are required so that zsh doesn't misunderstand.
6. **Enable pre-commit hooks with `pre-commit install`**, _this step is critical otherwise tools will not be enabled to validate git commits!!!_
7. Once installed, make sure pre-commit is doing stuff with command `pre-commit run`
8. Start building!

## Scripts
python script entry points are created with pyproject.toml config, see `[project.scripts]` table. Bash scripts will be located in ./tools directory.

| Start Script | Description | Args |
| --- | --- | --- |
| `load_data <args>`  | recursively loads data from text files not named log.txt | `--i <path> ` parent directory path, attempt to parse all txt files in parent, `--o <path>` puts results in folder on `--o` path, `--log` optional write rejected lines to log file in out path |
| `load_data_mongo <args>` | recursively uses load_data to load data from text files into a mongodb | `--dir-path <path>` parent directory, like 0318, in quotes|
| `analyze_links` | process entire mongoDB and output relations between number of video connections against other video stats. | `--use-cluster` to schedule as job on spark cluster containers and `--view-results` to query db and update image file with results of spark job, also **CAUTION** this will run the entire DB's contents, so be careful when calling this script :) |
| `correlation_analysis` | Loads YouTube dataset from MongoDB to a spark data frame, creates a sub data frame of numeric fields. loops through the columns and returns correlation results from pairs of columns to a list of correlations. Prints results of Positive Negative and Near Zero Correlations. To run on the Spark cluster, pass the --use-cluster flag. When run locally the heatmap is saved as correlation_heatmap.png in the project root, and when run on the cluster the image is saved inside the master container at /opt/spark/scripts/analysis/correlation_heatmap.png; you can copy it to your machine with docker cp master:/opt/spark/scripts/analysis/correlation_heatmap.png . | `--use-cluster` |
| `graph_filter` | Process up to 100,000 rows using the SCC algorithm plotting results | `--use-cluster` to schedule as job on spark cluster containers and `--view-results` to query db and update image file with results of spark job |
| `trending_predictor` | process entire mongoDB and output a trending score based off of user engagement, views, recency, and ratings. | no args

## Docker...
- In order to test the clustered performance of the algorithms, the project has been dockerized such that it can be built and ran using docker compose.
    - It should be noted that there are several limitations due to versioning issues between pyspark containers and python versions where graphframes is not supported so that will need to be ran on a single machine :(
- To build the container simply have the docker daemon running in the background and issue the command `docker compose build` to generate the images, installing the spark container and all python packaging as well as setting up the container network. This may take a few minutes, especially for first time installs!
    - as a developer you may find yourself building the containers over and over again, you may run into space issues depending on your machine. I would consider calling `docker system prune` to empty build directories every few builds just to be safe or things might hang.
- Once the containers have built, they may be ran using command `docker compose up -d` to run in detached mode, the extra flag is important as you will need to call python scripts from the host side to interact with the cluster! To shutdown the containers safely, `docker compose down` should be used.
    - if you suspect things might not have launched correctly, try `docker ps -a` to see all containers launched (both alive and dead), there should be at least 4 containers including db, master, history-server, and n workers where default n is 1.
- For interacting with the cluster, jobs may be submitted using helper function inside scripts._cluster. See script analyze_links for usage, but essentially the script and all its packages must be passed and ran on the master node so it may be distributed across the cluster. In the example, the cluster does the computations and saves to the mongodb container for later reference. This is initiated with `--use-cluster` flag, for full example `analyze_links --use-cluster` would schedule the analyze_links script on the cluster! As long as the containers are up, the data may be fetched, plotted, and saved to image in the root directory using `<script> --view-results`
- Some notes on mongodb. In an effort to make the transition to containerization simple, the compose file has bound localhost to the db container so mongodb is still accessible from the host using hostname localhost. To manually connect try `mongosh mongodb://localhost:27017`, this can be handy if you want to double check if your script is correctly writing to the db.
    - note that from the host side the name is "localhost", but when connecting from the spark container side, use connection string `mongodb://db:27017` for auto mapping with respect to container network. See analyze_links spark session function for usage.
- Spark has several helpful UI's for visualizing jobs, and the containers have the logging configured to keep them accurate! try url `localhost:8080` for spark health page for node status, and `localhost:18080` for spark history server! This will be important for long running jobs so we may track run times.
- Scaling the cluster is simple with the `--scale` flag available on docker compose, each worker is configured to use a single core so to get the most processing out of your machine try `docker compose up --scale worker=n` for n workers.
    - Another large factor in performance is the number of memory available to each worker, to be conservative I have set the value to 1Gb per node, but this can be adjusted by changing environment variable `SPARK_WORKER_MEMORY` in docker compose.
