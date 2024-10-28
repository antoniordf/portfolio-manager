export function formatDate(date) {
  const year = date.getFullYear();

  // Months are zero-based in javascript
  const month = `0${date.getMonth() + 1}`.slice(-2);
  const day = `0${date.getDate()}`.slice(-2);

  return `${year}-${month}-${day}`;
}
