const { BASE_URL, MASTER_PORT, STATES } = require("./config");

async function pingMaster() {
  try {
    const URL = `${BASE_URL}:${MASTER_PORT}/status`;

    const response = await fetch(URL, {
      method: "GET",
    });

    const data = await response.json();

    console.log("GET Request to: ", URL);
    console.log("Response: \n", data);

    return data.status == STATES.REDUCE_COMPLETED ? data : false;
  } catch (err) {
    console.log("Ping Error: ", err.message);
    return false;
  }
}

async function test(mapperFileName, reducerFileName) {
  const body = {
    mapperFileName,
    reducerFileName,
  };
  const URL = `${BASE_URL}:${MASTER_PORT}/map-reduce`;

  const response = await fetch(URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  });

  const data = await response.json();

  console.log("POST Request to: ", URL);
  console.log("Response: \n", data);

  let taskCompleted = false;
  while (!taskCompleted) {
    taskCompleted = await pingMaster();
    if (taskCompleted) {
      return;
    } else {
      console.log("Map Reduce task not completed. Waiting...");
      await new Promise((resolve) => setTimeout(resolve, 5000)); // Wait for 5 seconds before retrying
    }
  }
}

const [type] = process.argv.slice(2);
if (type.toLowerCase() == "wc") {
  test("mapper-wc.js", "reducer-wc.js");
} else if (type.toLowerCase() == "iv") {
  test("mapper-iv.js", "reducer-iv.js");
}
