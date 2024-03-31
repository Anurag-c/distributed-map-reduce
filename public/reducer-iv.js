function reducer(key, values) {
  const uniqueSet = new Set();
  for (const value of values) {
    uniqueSet.add(value);
  }

  return Array.from(uniqueSet);
}

module.exports = { reducer };
