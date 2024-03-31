const fs = require("fs");
const path = require("path");
const { NUM_MAPPERS, INPUT_FILE_PATH, FILE_PARTS_DIR } = require("./config");

function splitFile(
  inputFilePath = INPUT_FILE_PATH,
  outputDir = FILE_PARTS_DIR,
  numParts = NUM_MAPPERS
) {
  const data = fs.readFileSync(inputFilePath, "utf-8");

  console.log("Splitting Input File");

  // Split the text into words
  const words = data.split(/\s+/);

  // Calculate the approximate number of words per part
  const wordsPerPart = Math.floor(words.length / numParts);
  const remainingWords = words.length % numParts;

  // Create output directory if it doesn't exist
  if (!fs.existsSync(outputDir)) {
    fs.mkdirSync(outputDir);
  }

  // Divide the text into parts
  let startIndex = 0;
  const filenames = [];
  for (let i = 0; i < numParts; i++) {
    // Calculate the end index for this part
    const endIndex = startIndex + wordsPerPart + (i < remainingWords ? 1 : 0);

    // Write the part to a separate file
    const partText = words.slice(startIndex, endIndex).join(" ");
    const outputFile = path.join(outputDir, `part_${i + 1}.txt`);

    fs.writeFileSync(outputFile, partText);
    console.log(`Part ${i + 1} written successfully.`);
    filenames.push(`part_${i + 1}.txt`);

    // Update the start index for the next part
    startIndex = endIndex;
  }

  return filenames;
}

module.exports = { splitFile };
