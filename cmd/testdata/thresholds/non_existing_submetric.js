export const options = {
	thresholds: {
		// http_reqs being a builtin metric has no foo:bar
		// tag definition. As such k6 should detect it and
		"http_reqs{foo:bar}": ["count>0"],
	},
};

export default function () {
	console.log(
		"asserting that a threshold applying a method over a metric not supporting it fails with exit code 104 (Invalid config)"
	);
}
