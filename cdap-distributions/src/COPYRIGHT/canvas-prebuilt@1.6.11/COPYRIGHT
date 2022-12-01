[![NPM version](https://badge.fury.io/js/canvas-prebuilt.svg)](http://badge.fury.io/js/canvas-prebuilt)

This is a drop-in replacement for canvas that does not require any compiling. To use it
just `npm install canvas-prebuilt` or replace `canvas` with `canvas-prebuilt` in your
dependencies.

The repo is just a set of scripts that downloads a specific node-canvas version, builds it
and bundles it on all platforms. It's meant to run on Travis and AppVeyor but it can
be run locally too

# Releases

More detail on the releases below, this won't be relevant to most users.

## Binaries

Make sure your node version is the most recent to guarantee ABI compatibility

| canvas@1.4.x<br>canvas@1.5.x<br>canvas@1.6.x | node 7 | node 6 | node 5 | node 4 | node 0.12 | node 0.10 |
| ------------ | ------ | ------ | ------ | ------ | --------- | --------- |
| Linux x64    |   ✓    |   ✓    |   ✓    |    ✓   |    ✓      |     ✓     |
| Windows x64  |   ✓    |   ✓    |   ✓    |    ✓   |    ✓      |     ✓     |
| OSX x64      |   ✓    |   ✓    |   ✓    |    ✓   |    ✓      |     ✓     |
| Windows x86  |   𐄂¹   |   𐄂¹   |   𐄂¹   |    𐄂¹  |    𐄂¹     |     𐄂¹    |
| Linux x86    |   𐄂¹   |   𐄂¹   |   𐄂¹   |    𐄂¹  |    𐄂¹     |     𐄂¹    |
| Linux ARM    |   𐄂¹   |   𐄂¹   |   𐄂¹   |    𐄂¹  |    𐄂¹     |     𐄂¹    |

¹I have some ideas on how to get these working with cross-compilation if people request it.
I plan to add Linux/ARM

## Canvas version mapping

| canvas | canvas-prebuilt |
| ------ | --------------- |
| 1.4.0  | 1.4.0           |
| 1.5.0  | 1.5.0           |
| 1.6.0  | 1.6.0           |

# Bundling

The bundling scripts just take a regularly compiled executable (canvas.node in this case)
and look at which non-system libraries it links against. Those libraries are then copied to the release
directory and binaries are updated if necessary to refer to them.

The strategies for bundling could be applied to other projects too since they're general:

* On macOS, [macpack](https://github.com/chearon/macpack) is used to search dependencies, filter out non-system ones, and update binary references
* On Windows, [Dependency Walker](http://www.dependencywalker.com)'s CLI is used to search dependencies. Anything in the MSYS2 folder is considered non-system. Patching is not necessary because Windows looks for dlls in the same folder as the binary
* On Linux, [pax-utils](https://wiki.gentoo.org/wiki/Hardened/PaX_Utilities) searches dependencies, and everything not in `/lib` is non-system. The custom `binding.gyp` compiles `canvas.node` to look inside its own directory for dependencies
