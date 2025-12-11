# Infinispan - GEMINI Context

## Project Overview
**Infinispan** is an open-source, in-memory distributed database and key/value data store. It offers elastic scalability, high availability, and fault tolerance, functioning as both a volatile cache and a persistent data store.

### Key Capabilities
*   **Data Storage:** Stores all types of data, from Java objects to plain text.
*   **Distribution:** Distributes data across scalable clusters.
*   **APIs:** Supports multiple access protocols including Hot Rod, REST, Memcached, and Resp.
*   **Integrations:** Deep integration with Hibernate, Spring, Quarkus, and CDI.

### Architecture Highlights
The codebase is organized into several key modules managed by Maven:
*   **`core/`**: The core data grid implementation.
*   **`server/`**: Server-side components (Runtime, Hot Rod, REST, Memcached, Router).
*   **`client/`**: Java clients (Hot Rod, REST).
*   **`commons/`**: Shared utilities and SPIs.
*   **`persistence/`**: Data stores (JDBC, RocksDB, Remote, SQL).
*   **`api/`**: Public API definitions.
*   **`query/`**: Indexing and querying capabilities.

## Building and Running

The project uses **Maven** with a provided wrapper (`mvnw`). Ensure you have a compatible JDK (typically JDK 17+ for recent versions) installed.

### Common Commands

*   **Build Project (Fast):**
    ```bash
    ./mvnw clean install -DskipTests
    ```
    *Use this for a quick build to verify compilation.*

*   **Run All Tests:**
    ```bash
    ./mvnw verify
    ```
    *Note: This can take a significant amount of time.*

*   **Build Full Distribution:**
    ```bash
    ./mvnw clean install -Pdistribution
    ```

*   **Build Server Image:**
    ```bash
    ./mvnw -Pimage -pl server/image
    ```

### Debugging Tests
To debug a test running inside a container (e.g., in `server/tests`):
1.  Run the test with the debug flag (listens on port 5005):
    ```bash
    ./mvnw verify -pl server/tests -Dit.test=<TestClassName> -Dorg.infinispan.test.server.container.debug=0
    ```
2.  Attach your debugger to port 5005.

## Development Conventions

*   **Code Style:** Follow the existing coding style found in the project. The project shares style conventions with other Infinispan family projects.
*   **Testing:** New features and bug fixes **must** include test cases.
    *   Unit tests are standard.
    *   Integration tests are located in `integrationtests/`.
    *   Server tests are in `server/tests`.
*   **Commit Messages:**
    *   Start with the issue key (e.g., `ISPN-12345 Description of change`).
    *   Be concise but descriptive.
    *   Use the imperative mood ("Fix bug" not "Fixed bug").
*   **Workflow:**
    *   Create a topic branch for your feature/fix.
    *   Prefer **rebasing** over merging when updating from upstream.
    *   Squash commits into logical units before submitting a Pull Request.

## Key Configuration Files
*   **`pom.xml`**: Root Maven configuration, dependency management, and build profiles.
*   **`server/runtime/src/main/resources/infinispan.xml`** (Typical location): Default server configuration.
*   **`CONTRIBUTING.md`**: Detailed guide on legal, setup, and contribution process.
*   **`README-Build.md`**: Specifics on build options and profiles.
