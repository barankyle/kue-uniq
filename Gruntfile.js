module.exports = function(grunt) {
	grunt.initConfig({
		package: grunt.file.readJSON('package.json'),

		jscs: {
			all: {
				options: {
					config: 'config/jscs.json'
				},
				src: [
					'Gruntfile.js',
					'main.js',
					'lib/**/*.js',
					'test/**/*.js'
				],
				gruntfile: 'Gruntfile.js'
			}
		},

		jshint: {
			all: {
				options: {
					jshintrc: 'config/jshint.json',
					reporter: require('jshint-stylish')
				},
				src: [
					'Gruntfile.js',
					'main.js',
					'lib/**/*.js',
					'test/**/*.js'
				]
			}
		},

		jsonlint: {
			jscs: {
				src: 'config/jscs.json'
			},
			jshint: {
				src: 'config/jshint.json'
			},
			package: {
				src: 'package.json'
			}
		},

		mochaTest: {
			full: {
				src: [
					'test/*.js',
					'test/*/*.js'
				]
			},
			grid: {
				options: {
					reporter: 'dot'
				},
				src: '<%= mochaTest.full.src %>'
			},
			nyan: {
				options: {
					reporter: 'nyan'
				},
				src: '<%= mochaTest.full.src %>'
			}
		}
	});

	// Load grunt tasks from NPM packages
	require('load-grunt-tasks')(grunt);

	grunt.registerTask('lint', [
		'jsonlint:jshint',
		'jshint',
		'jsonlint:jscs',
		'jscs'
	]);

	grunt.registerTask('test', [
		'mochaTest:full'
	]);

	// Default grunt
	grunt.registerTask('default', [
		'lint'
	]);
};
