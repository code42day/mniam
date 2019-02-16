module.exports = trigger;

function trigger(fn, e, result) {
  if (!fn) {
    if (e) { throw e; }
    return result;
  }
  process.nextTick(fn, e, result);
}
