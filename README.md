# bmstrem
Bare Minimum Struct Relational Mapping

This intends to provide a bare minimum ORM-Like-but-not-quite interface to work with postgres.

The original intent behind this project is to allow us to replace [gorm](https://github.com/jinzhu/gorm) in a project at work because it is falling a bit short.

Ideally this will Open a connection, provide you with a db object that can in turn give you chainable
commands so you can compose your query and then both render it in a way that allows you to determine
if the query is what you expect and also execute it and get results into objects through introspection.
