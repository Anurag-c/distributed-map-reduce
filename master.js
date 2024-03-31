const cluster = require("cluster");
const os = require("os");
const { makeMaster } = require("./master-server");
const { makeWorker } = require("./worker");
const { MASTER_PORT, NUM_WORKERS } = require("./config");
const { splitFile } = require("./fileSplitter");
const dotenv = require("dotenv").config();

const numCPUs = os.cpus().length;

if (cluster.isPrimary) {
  // 1. The MapReduce library first splits the input file into M pieces
  const filenames = splitFile();

  // 2. One of the copies of the program is special– the master
  makeMaster(MASTER_PORT, filenames);

  // 3. The rest are workers that are assigned work by the master.
  // There are M map tasks and R reduce tasks to assign.
  for (let i = 0; i < Math.min(numCPUs, NUM_WORKERS); i++) {
    cluster.fork();
  }

  cluster.on("exit", (worker, code, signal) => {
    console.log(`Worker ${worker.process.pid} died`);
  });
} else {
  makeWorker(3000 + cluster.worker.id);
}
