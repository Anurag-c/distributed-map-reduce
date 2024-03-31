function map(filename, contents) {
  // Function to detect word separators
  const isLetter = (char) => /[a-zA-Z]/.test(char);

  // Split contents into an array of words
  const words = contents.split(/\s+/).filter((word) => word.trim().length > 0);

  const kvarr = [];
  for (const word of words) {
    if (isLetter(word[0]) && isLetter(word[word.length - 1])) {
      const kv = [word, 1];
      kvarr.push(kv);
    }
  }
  return kvarr;
}

module.exports = { map };
