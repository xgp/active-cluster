# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build/Test Commands
- Build: `mvn clean install`
- Run tests: `mvn test`
- Run single test: `mvn test -Dtest=TestClassName#testMethodName`
- Format code: `mvn fmt:format`
- Check style: `mvn fmt:check`

## Code Style Guidelines
- Java 21, follows Google Java Style
- Use 2-space indentation
- Class names: PascalCase (e.g., `BullyElectionStrategy`)
- Methods/variables: camelCase (e.g., `getDestination`)
- Interfaces don't use "I" prefix
- Comprehensive Javadoc with `@param`, `@return`, `@throws`
- Imports: organized by package, no wildcards
- Declare thrown exceptions in method signatures
- Implementations in `impl` subdirectories
- Use custom exceptions (e.g., `ClusterException`)
- Interface-based design with clear separation of concerns
- No raw types - use generics properly