const express = require("express");
const path = require("path");
const morgan = require("morgan");
const {
  NUM_REDUCERS,
  REDUCE_PORTS,
  MAP_PORTS,
  BASE_URL,
  FILE_PARTS_DIR,
  STATES,
} = require("./config.js");
const { default: axios } = require("axios");

function makeMaster(PORT, filenames) {
  const app = express();
  app.use(express.json());
  let status = STATES.WORKER_IDLE;
  const map_tasks = [];
  const reduce_tasks = [];

  async function getAllFileLocations() {
    console.log("Getting all file locations from the mapper tasks");
    const calls = [];
    for (const task of map_tasks) {
      calls.push(axios.get(task.fileLocationsURL));
    }

    const results = await Promise.all(calls);
    const fileLocations = [];
    for (const res of results) {
      console.log("file location: ", res.data.keys);
      fileLocations.push(res.data.keys);
    }
    console.log("Recieved all file locations from the mapper tasks");
    return fileLocations;
  }

  async function createReduceTasks(reducerFileName) {
    console.log("Creating Reduce tasks");
    const fileLocations = await getAllFileLocations();
    for (let j = 0; j < NUM_REDUCERS; j++) {
      const filesForReducer = [];
      for (let i = 0; i < fileLocations.length; i++) {
        filesForReducer.push(fileLocations[i][j]);
      }
      const task = {
        workerPayload: {
          taskType: "reduce",
          fileLocations: filesForReducer,
          reducerFilePath: `./public/${reducerFileName}`,
          outputFilePath: `./public/output-${j}.json`,
        },
        workerURL: `${BASE_URL}:${REDUCE_PORTS[j]}/task`,
        statusURL: `${BASE_URL}:${REDUCE_PORTS[j]}/reduce-status`,
      };
      console.log("Reducer task created for worker: ", task.workerURL);
      reduce_tasks.push(task);
    }

    console.log("All reduce tasks created and ready to send to workers");
  }

  function createMapTasks(mapperFileName) {
    console.log("Creating Map tasks");

    for (let i = 0; i < filenames.length; i++) {
      const task = {
        workerPayload: {
          taskType: "map",
          inputFilePath: `./${FILE_PARTS_DIR}/${filenames[i]}`,
          mapperFilePath: `./public/${mapperFileName}`,
          numReducers: NUM_REDUCERS,
        },
        workerURL: `${BASE_URL}:${MAP_PORTS[i]}/task`,
        statusURL: `${BASE_URL}:${MAP_PORTS[i]}/map-status`,
        fileLocationsURL: `${BASE_URL}:${MAP_PORTS[i]}/redis-locations`,
      };

      console.log(`Map Task created for input file: ${filenames[i]}`);
      map_tasks.push(task);
    }

    console.log("All Map tasks created and ready to send to workers");
  }

  async function executeTasks(tasks) {
    const calls = [];
    for (let i = 0; i < tasks.length; i++) {
      const task = tasks[i];
      console.log(
        `Sending ${task.workerPayload.taskType} task to worker: `,
        task.workerURL
      );
      calls.push(axios.post(task.workerURL, task.workerPayload));
    }

    Promise.all(calls);
  }

  async function pingTasks(tasks) {
    const calls = [];
    for (let i = 0; i < tasks.length; i++) {
      const task = tasks[i];
      console.log(
        `Pinging ${task.workerPayload.taskType} worker: `,
        task.workerURL
      );
      calls.push(axios.get(task.statusURL));
    }

    const res = await Promise.all(calls);
    for (let i = 0; i < res.length; i++) {
      if (
        res[i].data.status !== STATES.MAP_COMPLETED &&
        res[i].data.status !== STATES.REDUCE_COMPLETED
      ) {
        return false;
      }
    }

    console.log("All Tasks completed by the workers");
    return true;
  }

  async function runMapReduce(mapperFileName, reducerFileName) {
    // 1. Map phase
    console.log(
      ".......................Starting MAP PHASE......................."
    );
    status = STATES.MAP_PHASE;
    createMapTasks(mapperFileName);
    executeTasks(map_tasks);

    // 2. Check for Map completion and move to Reduce phase
    let allTasksCompleted = false;
    while (!allTasksCompleted) {
      allTasksCompleted = await pingTasks(map_tasks);
      if (allTasksCompleted) {
        console.log(
          ".......................Starting REDUCE PHASE......................."
        );
        status = STATES.REDUCE_PHASE;
        await createReduceTasks(reducerFileName);
        executeTasks(reduce_tasks);
        break;
      } else {
        console.log("Not all map tasks are completed. Waiting...");
        await new Promise((resolve) => setTimeout(resolve, 5000)); // Wait for 5 seconds before retrying
      }
    }

    // 3. check for reducer completion
    allTasksCompleted = false;
    while (!allTasksCompleted) {
      allTasksCompleted = await pingTasks(reduce_tasks);
      if (allTasksCompleted) {
        status = STATES.REDUCE_COMPLETED;
        break;
      } else {
        console.log("Not all reduce tasks are completed. Waiting...");
        await new Promise((resolve) => setTimeout(resolve, 5000)); // Wait for 5 seconds before retrying
      }
    }
  }

  app.use(morgan("dev"));

  app.use(express.static(path.join(__dirname, "public")));

  app.post("/map-reduce", (req, res) => {
    const { mapperFileName, reducerFileName } = req.body;

    runMapReduce(mapperFileName, reducerFileName);

    res.status(200).json({
      message: "Map Reduce Started, Use the below link to check status",
      statusURL: `${BASE_URL}:${PORT}/status`,
    });
  });

  app.get("/status", (req, res) => {
    const outputFiles = [];
    if (status == STATES.REDUCE_COMPLETED) {
      for (const task of reduce_tasks) {
        const fileName = task.workerPayload.outputFilePath.split("/").pop();
        outputFiles.push(`${BASE_URL}:${PORT}/${fileName}`);
      }
    }
    res.status(200).json({
      status,
      outputFiles,
    });
  });

  const server = app.listen(PORT, () =>
    console.log(`Master server running on port ${PORT}`)
  );

  app.use((err, req, res, next) => {
    console.error(err.stack);
    console.log("inside");
    res.status(500).send("Something broke!");
  });

  process.on("SIGINT", () => {
    console.log("Server is shutting down...");
    server.close(() => {
      console.log("Server has been shut down");
      process.exit(0);
    });
  });
}

module.exports = { makeMaster };
