# Neo4j Connector for Kafka

The project provides Neo4j sink and source connector implementations for Kafka Connect platform.

## Documentation & Articles

Read more at https://neo4j.com/docs/kafka/.

## Feedback & Suggestions

Please raise [issues on Github](https://github.com/neo4j/neo4j-kafka-connector/issues). We also love contributions, so
don't be shy to send a Pull Request.

## Development & Contributions

### Build locally

Make sure you install [ruby](https://www.ruby-lang.org/en/documentation/installation/) and [direnv](https://direnv.net/)
and configure `direnv` for your bash.

First, install the configured version of [dip](https://github.com/bibendi/dip) using ruby bundle:

```shell
bundle install
```

For the end-to-end tests, you need to provision a local Kafka cluster, Kafka Connect instance and a Neo4j server.
This is done by running (re-running recreates the containers):

```shell
dip provision
```

Make sure `direnv` exports environment variables by running:

```shell
direnv allow .
```

You can build and package the project using (as many as time as necessary):

```shell
mvn verify
```

You'll find the connector archive
at `packaging/target/neo4j-kafka-connect-{version}.zip`.

### Code Format

TL;DR? You can run `dip format`.

For Kotlin code, we follow the [ktfmt](https://github.com/facebook/ktfmt) code style. There is an `.editorconfig` file
to mimic the underlying style guides for built-in Intellij code style rules, but we recommend
[ktfmt IntelliJ Plugin](https://plugins.jetbrains.com/plugin/14912-ktfmt) for formatting. Remember that your builds will
fail if your changes doesn't match the enforced code style, but you can use `./mvnw spotless:apply` to format your code.

For POM files, we are using [sortpom](https://github.com/Ekryd/sortpom) to have a tidier project object model. Remember
that your builds will fail if your changes doesn't conform to the enforced rules, but you can use `./mvnw sortpom:sort`
to format it accordingly.

### Docs

The documentation source for this version lives at [this repository](https://github.com/neo4j/docs-kafka-connector).
Please raise any documentation updates by creating a PR against it.

## License

Neo4j Connector for Kafka is licensed under the terms of the Apache License, version 2.0. See `LICENSE` for more
details. 
