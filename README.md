# ether-jdbc

JDBC-first implementation of the Ether database contracts using only the Java standard library.

## Scope

- `DatabaseClient` implementation over `DataSource`
- Simple `DataSource` backed by `DriverManager`
- Parameter binding for scalar, null and SQL array values
- Transaction handling without external frameworks

## Maven

```xml
<dependency>
  <groupId>dev.rafex.ether.jdbc</groupId>
  <artifactId>ether-jdbc</artifactId>
  <version>1.0.0-SNAPSHOT</version>
</dependency>
```
