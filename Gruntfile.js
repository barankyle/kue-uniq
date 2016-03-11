module.exports = function(grunt) {
	grunt.initConfig({
		package: grunt.file.readJSON('package.json'),

		clean: {
			deploy: 'deploy'
		},

		compress: {
			main: {
				options: {
					archive: 'deploy/<%= package.name %>.tar.gz'
				},
				expand: true,
				src: [
					'lib/**/*',
					'package.json',
					'LICENSE',
					'main.js'
				],
				dest: '<%= package.name %>'
			}
		},

		jscs: {
			all: {
				options: {
					config: 'jscs.json'
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
					jshintrc: 'jshint.json',
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
				src: 'jscs.json'
			},
			jshint: {
				src: 'jshint.json'
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
