const express = require("express");
const morgan = require("morgan");
const fs = require("fs");
const redis = require("redis");
const { STATES } = require("./config");
const { sortAndGroup, partitionKeyValueList } = require("./utils.js");

async function makeWorker(PORT) {
  let status = STATES.WORKER_IDLE;
  let map_status = STATES.WORKER_IDLE;
  let reduce_status = STATES.WORKER_IDLE;
  const partitionLocations = [];

  const app = express();
  app.use(express.json());

  const redisClient = redis.createClient({
    password: process.env.REDIS_PASSWORD,
    socket: {
      host: process.env.REDIS_HOST,
      port: process.env.REDIS_PORT,
    },
  });

  redisClient.on("error", (err) =>
    console.log("Redis Client Error", err.message)
  );

  redisClient.on("", (err) => console.log("Redis Client Error", err.message));

  await redisClient.connect();

  async function saveToRedis(partitions) {
    try {
      for (let i = 0; i < partitions.length; i++) {
        const key = `worker-${PORT}-${i}`;
        const value = JSON.stringify(partitions[i]);
        await redisClient.set(key, value);
        console.log(`${key} key set to redis successfully`);
        redisClient.expire(key, 600);
        partitionLocations.push(key);
        console.log("save to redis: ", partitionLocations);
      }
      map_status = STATES.MAP_COMPLETED;
      status = STATES.WORKER_IDLE;
    } catch (err) {
      console.error("Error in saveToRedis: ", err.message);
      map_status = STATES.MAP_ERROR;
    }
  }

  function execMap(inputFilePath, mapperFilePath, numReducers) {
    // 1. Get the mapper function from mapper file
    const { map } = require(mapperFilePath);

    // 2. Read the input text file
    fs.readFile(inputFilePath, "utf-8", (err, data) => {
      if (err) {
        console.error("Error reading Input File: ", err);
        map_status = STATES.MAP_ERROR;
        return;
      }
      // 3. run the mapper function mentioned by user
      const result = map(inputFilePath.split("/").pop(), data);

      // 4. divide the intermediate key value pairs into R partitions
      const partitions = partitionKeyValueList(result, numReducers);

      // 5. save the result to shared memory redis
      saveToRedis(partitions);
    });
  }

  async function execReduce(fileLocations, reducerFilePath, outputFilePath) {
    const { reducer } = require(reducerFilePath);

    // 1. read all intermediate data.
    const data = [];
    for (const redisKey of fileLocations) {
      const kvlist = JSON.parse(await redisClient.get(redisKey));
      for (const [key, value] of kvlist) {
        data.push([key, value]);
      }
    }

    // 2. sorts it by the intermediate keys so that all occurrences of the same key are grouped together.
    const groupedkv = sortAndGroup(data);

    // 3. for each unique intermediate key encountered, it passes the key and the corresponding
    // set of intermediate values to the user’s Reduce function.
    const output = {};
    for (const [key, values] of groupedkv) {
      output[key] = reducer(key, values);
    }

    fs.writeFile(
      outputFilePath,
      JSON.stringify(output, null, 2),
      "utf8",
      (err) => {
        if (err) {
          console.error("Error writing JSON file:", err);
          reduce_status = STATES.REDUCE_ERROR;
          return;
        }

        reduce_status = STATES.REDUCE_COMPLETED;
        status = STATES.WORKER_IDLE;
      }
    );
  }

  app.use(morgan("dev"));

  app.post("/task", (req, res) => {
    if (status != STATES.WORKER_IDLE) {
      res.status(200).json({
        status,
      });
      return;
    }

    status = STATES.WORKER_RUNNING;

    const { taskType } = req.body;

    if (taskType == "map") {
      const { inputFilePath, mapperFilePath, numReducers } = req.body;

      map_status = STATES.MAP_RUNNING;
      partitionLocations.length = 0;

      execMap(inputFilePath, mapperFilePath, numReducers);

      res.status(200).json({
        status: map_status,
      });
    } else if (taskType == "reduce") {
      const { fileLocations, reducerFilePath, outputFilePath } = req.body;

      reduce_status = STATES.REDUCE_RUNNING;
      execReduce(fileLocations, reducerFilePath, outputFilePath);

      res.status(200).json({
        status: reduce_status,
      });
    }
  });

  app.get("/worker-status", (req, res) => {
    res.status(200).json({
      status,
    });
  });

  app.get("/redis-locations", (req, res) => {
    let keys = [];
    if (map_status == STATES.MAP_COMPLETED) {
      keys = partitionLocations;
    }
    res.status(200).json({
      keys,
    });
  });

  app.get("/map-status", (req, res) => {
    res.status(200).json({
      status: map_status,
    });
  });

  app.get("/reduce-status", (req, res) => {
    res.status(200).json({
      status: reduce_status,
    });
  });

  app.use((err, req, res, next) => {
    console.error(err.stack);
    res.status(500).send("Something broke!");
  });

  const server = app.listen(PORT, () =>
    console.log(`Worker server running on port ${PORT}`)
  );

  process.on("SIGINT", () => {
    console.log("Server is shutting down...");
    server.close(() => {
      console.log("Server has been shut down");
      process.exit(0);
    });
  });
}

module.exports = { makeWorker };
