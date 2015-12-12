mail = load '${in}' using PigStorage();
store mail into '${out}' using PigStorage();