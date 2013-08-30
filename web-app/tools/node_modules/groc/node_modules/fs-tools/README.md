fs-tools
========
[![Build Status](https://secure.travis-ci.org/nodeca/fs-tools.png)](http://travis-ci.org/nodeca/fs-tools)

Useful file utitiles. See [API Documentation](http://nodeca.github.com/fs-tools/#FsTools) for detailed info.

---

### walk(path, [pattern,] iterator[, callback])

Recurcively scan files by regex pattern & apply iterator to each. Iterator
applied only to files, not to directories.


### remove(path, callback)

Recursively delete directory with all content.


### mkdir(path, mode = '0755', callback)

Recursively make path.


### copy(src, dst, callback)

Copy file. Not optimized for big sourses (read all to memory at once).


## License

View the [LICENSE](https://github.com/nodeca/fs-tools/blob/master/LICENSE) file (MIT).
