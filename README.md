# Yet Another Minecraft Packmaker (YAMP)
Concept is simple: read .toml file to download all necessary mods and resources.
Both [modrinth](https://modrinth.com/) and [curseforge](https://www.curseforge.com/minecraft/mc-mods) 
as sources are supported.

** This is still work in progress **


### How to use?

Run with [uv](https://docs.astral.sh/uv/):
```shell
uvx https://github.com/Im-making-tools/yamp.git
```

Example modpacks toml files can be found in [examples/](examples/) directory. 
To run, a file or url can be specified, e.g.:

```shell
TOML_URL = "https://raw.githubusercontent.com/Im-making-tools/yamp/refs/heads/master/examples/fabric_1.21.1.toml"
# Run server
uvx https://github.com/Im-making-tools/yamp.git "$TOML_URL" server

# Run game
uvx https://github.com/Im-making-tools/yamp.git "$TOML_URL" client
```


# Also see

* [picomc](https://github.com/sammko/picomc) - minecraft launcher this project is based on
* [portablemc](https://github.com/mindstorm38/portablemc) - another cool minecraft launcher
* [packmaker](https://gitlab.com/routhio/minecraft/tools/packmaker/) - older, but more complete alternative to yamp