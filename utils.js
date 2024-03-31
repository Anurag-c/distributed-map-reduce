const crypto = require("crypto");

function sortAndGroup(kvlist) {
  kvlist.sort((a, b) => a[0].localeCompare(b[0]));
  const groupedkv = [];
  let currentKey = null;
  let currentValues = null;

  for (const [key, value] of kvlist) {
    if (key !== currentKey) {
      // If encountering a new key, push the previous key-value pair to the output
      if (currentKey !== null) {
        groupedkv.push([currentKey, currentValues]);
      }
      // Update the current key and start a new list of values
      currentKey = key;
      currentValues = [value];
    } else {
      // If the key is the same, append the value to the current list of values
      currentValues.push(value);
    }
  }

  // Push the last key-value pair to the output
  if (currentKey !== null) {
    groupedkv.push([currentKey, currentValues]);
  }

  return groupedkv;
}

/*
function hash(key) {
  const hashValue = crypto.createHash("sha256").update(key).digest("hex");
  const hashInt = parseInt(hashValue, 16);
  return hashInt;
}
*/

function hash(s) {
  const p = 31;
  const m = 10 ** 9 + 9;
  let hashValue = 0;
  let pPow = 1;
  for (let i = 0; i < s.length; i++) {
    hashValue =
      (hashValue + (s.charCodeAt(i) - "a".charCodeAt(0) + 1) * pPow) % m;
    pPow = (pPow * p) % m;
  }
  return hashValue;
}

function partitionKeyValueList(kvlist, numReducers) {
  const regions = Array.from({ length: numReducers }, () => []);

  kvlist.forEach(([key, value]) => {
    let regionIndex = hash(key) % numReducers;
    if (regionIndex < 0) regionIndex += numReducers;
    regions[regionIndex].push([key, value]);
  });

  return regions;
}

module.exports = { sortAndGroup, partitionKeyValueList };
