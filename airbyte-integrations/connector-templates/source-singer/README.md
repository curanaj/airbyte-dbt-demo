# Source {{titleCase name}} Singer

This is the repository for the {{titleCase name}} source connector, based on a Singer tap.
For information about how to use this connector within Airbyte, see [the User Documentation](https://docs.airbyte.io/integrations/sources/{{dashCase name}}).

## Local development

### Prerequisites
**To iterate on this connector, make sure to complete this prerequisites section.**

#### Build & Activate Virtual Environment and install dependencies
From this connector directory, create a virtual environment:
```
python -m venv .venv
```

This will generate a virtualenv for this module in `.venv/`. Make sure this venv is active in your
development environment of choice. To activate it from the terminal, run:
```
source .venv/bin/activate
pip install -r requirements.txt
```
If you are in an IDE, follow your IDE's instructions to activate the virtualenv.

Note that while we are installing dependencies from `requirements.txt`, you should only edit `setup.py` for your dependencies. `requirements.txt` is
used for editable installs (`pip install -e`) to pull in Python dependencies from the monorepo and will call `setup.py`.
If this is mumbo jumbo to you, don't worry about it, just put your deps in `setup.py` but install using `pip install -r requirements.txt` and everything
should work as you expect.

#### Building via Gradle
From the Airbyte repository root, run:
```
./gradlew :airbyte-integrations:connectors:source-{{dashCase name}}:build
```

#### Create credentials
**If you are a community contributor**, follow the instructions in the [documentation](https://docs.airbyte.io/integrations/sources/{{dashCase name}})
to generate the necessary credentials. Then create a file `secrets/config.json` conforming to the `source_{{snakeCase name}}_singer/spec.json` file.
Note that the `secrets` directory is gitignored by default, so there is no danger of accidentally checking in sensitive information.
See `sample_files/sample_config.json` for a sample config file.

**If you are an Airbyte core member**, copy the credentials in Lastpass under the secret name `source {{dashCase name}} test creds`
and place them into `secrets/config.json`.

### Locally running the connector
```
python main_dev.py spec
python main_dev.py check --config secrets/config.json
python main_dev.py discover --config secrets/config.json
python main_dev.py read --config secrets/config.json --catalog sample_files/configured_catalog.json
```

### Locally running the connector docker image

#### Build
First, make sure you build the latest Docker image:
```
docker build . -t airbyte/source-{{dashCase name}}-singer:dev
```

You can also build the connector image via Gradle:
```
./gradlew :airbyte-integrations:connectors:source-{{dashCase name}}:airbyteDocker
```
When building via Gradle, the docker image name and tag, respectively, are the values of the `io.airbyte.name` and `io.airbyte.version` `LABEL`s in
the Dockerfile.

#### Run
Then run any of the connector commands as follows:
```
docker run --rm airbyte/source-{{dashCase name}}-singer:dev spec
docker run --rm -v $(pwd)/secrets:/secrets airbyte/source-{{dashCase name}}-singer:dev check --config /secrets/config.json
docker run --rm -v $(pwd)/secrets:/secrets airbyte/source-{{dashCase name}}-singer:dev discover --config /secrets/config.json
docker run --rm -v $(pwd)/secrets:/secrets -v $(pwd)/sample_files:/sample_files airbyte/source-{{dashCase name}}-singer:dev read --config /secrets/config.json --catalog /sample_files/configured_catalog.json
```
## Testing
   Make sure to familiarize yourself with [pytest test discovery](https://docs.pytest.org/en/latest/goodpractices.html#test-discovery) to know how your test files and methods should be named.
First install test dependecies into your virtual environment:
```
pip install .[test]
```
### Unit Tests
To run unit tests locally, from the connector directory run:
```
python -m pytest unit_tests
```
Unit tests also executed as part of the `build` command
```
./gradlew :airbyte-integrations:connectors:source-{{dashCase name}}:build
```
### Integration Tests
To run the standard integration test suite, from the airbyte project root run:
```
./gradlew :airbyte-integrations:connectors:source-{{dashCase name}}-singer:integrationTest
```
To run additional integration tests, place them inside `integration_tests/` folder.

To run your integration tests manually, from the connector directory run:
```
pytest integration_tests
```

### Acceptance Tests
1. Update `acceptance-test-config.yml` file to configure tests. See (acceptance tests)[] for more information.
2. From the connector root, run `./acceptance-test.sh` 

## Dependency Management
All of your dependencies should go in `setup.py`, NOT `requirements.txt`. The requirements file is only used to connect internal Airbyte dependencies in the monorepo for local development. 
We split dependencies between two groups, dependencies that are:
* required for your connector to work need to go to `MAIN_REQUIREMENTS` list. 
* required for the testing need to go to `TEST_REQUIREMENTS` list

### Publishing a new version of the connector
You've checked out the repo, implemented a million dollar feature, and you're ready to share your changes with the world. Now what?
1. Make sure your changes are passing unit and integration tests
1. Bump the connector version in `Dockerfile` -- just increment the value of the `LABEL io.airbyte.version` appropriately (we use [SemVer](https://semver.org/)).
1. Create a Pull Request
1. Pat yourself on the back for being an awesome contributor
1. Someone from Airbyte will take a look at your PR and iterate with you to merge it into master