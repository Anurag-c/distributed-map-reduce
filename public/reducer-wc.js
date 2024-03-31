function reducer(key, values) {
  let result = 0;
  for (const v of values) {
    result += parseInt(v);
  }
  return result.toString();
}

module.exports = { reducer };
