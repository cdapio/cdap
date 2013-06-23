var data = "asd123";

function sliceOf() {
	return data[0];
}

exports.compare = {
  "slice, ==" : function () {
	  return data.slice(0, 1) == "a";
  },
  "slice, ===" : function () {
	  return data.slice(0, 1) === "a";
  },
  "index, ==" : function () {
	  return data[0] == "a";
  },
  "index, ===" : function () {
	  return data[0] === "a";
  },
  "charAt, ==" : function () {
	  return data.charAt(0) == 97;
  },
  "charAt, ===" : function () {
	  return data.charAt(0) === 97;
  },
  "wrapped, ==" : function () {
	  return sliceOf() == "a";
  },
  "wrapped, ===" : function () {
	  return sliceOf() === "a";
  },
  "regex" : function () {
	  return /^a/.test(data);
  }
};

require("bench").runMain()
